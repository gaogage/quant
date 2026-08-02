//! PIT alpha derivation, combo score loading, liquidity filtering, and return-risk matrices.
use super::*;

#[derive(Debug, Clone, Copy)]
struct DerivedPitAlphaSpec {
    source_combo_name: &'static str,
    source_direction: ScoreDirection,
    current_weight: f64,
    change_weight: f64,
}

fn derived_pit_alpha_spec(combo_name: &str) -> Option<DerivedPitAlphaSpec> {
    match combo_name {
        "phase7_quality_recovery_acceleration_v1" => Some(DerivedPitAlphaSpec {
            source_combo_name: "phase7_financial_quality_v1",
            source_direction: ScoreDirection::Ascending,
            current_weight: 0.40,
            change_weight: 0.60,
        }),
        _ => None,
    }
}

pub(crate) fn derive_pit_quality_recovery_scores(
    source_scores: &FactorScoresByDate,
    score_days: &[NaiveDate],
    source_direction: ScoreDirection,
    result_direction: ScoreDirection,
    current_weight: f64,
    change_weight: f64,
    score_candidate_pool_size: Option<usize>,
) -> FactorScoresByDate {
    let score_days = normalized_dates(score_days);
    let mut derived = FactorScoresByDate::new();
    let mut previous_rows: Option<&Vec<(String, f64)>> = None;

    for score_day in score_days {
        let Some(current_rows) = source_scores.get(&score_day) else {
            continue;
        };
        if current_rows.is_empty() {
            previous_rows = Some(current_rows);
            continue;
        }

        if let Some(previous_rows) = previous_rows {
            let previous_by_symbol = previous_rows
                .iter()
                .filter(|(_, score)| score.is_finite())
                .map(|(symbol, score)| (symbol.as_str(), *score))
                .collect::<HashMap<_, _>>();
            let paired = current_rows
                .iter()
                .filter_map(|(symbol, current_score)| {
                    let previous_score = previous_by_symbol.get(symbol.as_str()).copied()?;
                    if current_score.is_finite() && previous_score.is_finite() {
                        Some((
                            symbol.clone(),
                            *current_score,
                            current_score - previous_score,
                        ))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            if !paired.is_empty() {
                let current_stats =
                    score_stats(paired.iter().map(|(_, current_score, _)| *current_score));
                let change_stats =
                    score_stats(paired.iter().map(|(_, _, score_change)| *score_change));
                let mut rows = paired
                    .into_iter()
                    .map(|(symbol, current_score, score_change)| {
                        let current_good =
                            oriented_standard_score(current_score, current_stats, source_direction);
                        let change_good =
                            oriented_standard_score(score_change, change_stats, source_direction);
                        (
                            symbol,
                            current_weight.max(0.0) * current_good
                                + change_weight.max(0.0) * change_good,
                        )
                    })
                    .collect::<Vec<_>>();
                sort_factor_scores(&mut rows, result_direction);
                if let Some(limit) = normalize_score_candidate_pool_size(score_candidate_pool_size)
                {
                    rows.truncate(limit);
                }
                if !rows.is_empty() {
                    derived.insert(score_day, rows);
                }
            }
        }

        previous_rows = Some(current_rows);
    }

    derived
}

async fn load_derived_pit_combo_scores_for_dates_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    config: &SignalConfig,
    score_days: &[NaiveDate],
    spec: DerivedPitAlphaSpec,
) -> Result<Arc<FactorScoresByDate>, String> {
    let score_days = normalized_dates(score_days);
    if score_days.is_empty() {
        return Ok(Arc::new(HashMap::new()));
    }

    let score_candidate_pool_size =
        normalize_score_candidate_pool_size(config.score_candidate_pool_size);
    let mut scores_by_date = FactorScoresByDate::new();
    let mut has_missing_day = false;
    for day in &score_days {
        let key = SignalDataCacheKey::combo_scores(
            &config.combo_name,
            &config.version,
            *day,
            *day,
            config.score_direction,
            score_candidate_pool_size,
            config.universe_profile,
        );
        if let Some(cached) = cache.cached_combo_scores(&key) {
            scores_by_date.extend(cached.as_ref().clone());
        } else {
            has_missing_day = true;
        }
    }

    if has_missing_day {
        let mut source_config = config.clone();
        source_config.combo_name = spec.source_combo_name.to_string();
        source_config.score_direction = spec.source_direction;
        source_config.score_candidate_pool_size = None;
        source_config.prediction_blend = None;
        source_config.event_gate = None;
        source_config.score_overlay = None;
        source_config.portfolio_sleeve = None;

        let source_scores =
            load_persisted_combo_scores_for_dates_cached(pool, cache, &source_config, &score_days)
                .await?;
        let derived_scores = derive_pit_quality_recovery_scores(
            source_scores.as_ref(),
            &score_days,
            spec.source_direction,
            config.score_direction,
            spec.current_weight,
            spec.change_weight,
            score_candidate_pool_size,
        );
        for day in &score_days {
            let day_scores = derived_scores.get(day).cloned().unwrap_or_default();
            let day_map = if day_scores.is_empty() {
                HashMap::new()
            } else {
                HashMap::from([(*day, day_scores.clone())])
            };
            let key = SignalDataCacheKey::combo_scores(
                &config.combo_name,
                &config.version,
                *day,
                *day,
                config.score_direction,
                score_candidate_pool_size,
                config.universe_profile,
            );
            cache.insert_combo_scores(key, day_map);
            if !day_scores.is_empty() {
                scores_by_date.insert(*day, day_scores);
            }
        }
    }

    if scores_by_date.is_empty() {
        return Err("No derived PIT combo scores found".into());
    }

    Ok(Arc::new(scores_by_date))
}

pub(crate) async fn load_combo_scores_for_dates_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    config: &SignalConfig,
    score_days: &[NaiveDate],
) -> Result<Arc<FactorScoresByDate>, String> {
    if let Some(spec) = derived_pit_alpha_spec(&config.combo_name) {
        return load_derived_pit_combo_scores_for_dates_cached(
            pool, cache, config, score_days, spec,
        )
        .await;
    }

    load_persisted_combo_scores_for_dates_cached(pool, cache, config, score_days).await
}

async fn load_persisted_combo_scores_for_dates_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    config: &SignalConfig,
    score_days: &[NaiveDate],
) -> Result<Arc<FactorScoresByDate>, String> {
    let score_days = normalized_dates(score_days);
    if score_days.is_empty() {
        return Ok(Arc::new(HashMap::new()));
    }

    let score_candidate_pool_size =
        normalize_score_candidate_pool_size(config.score_candidate_pool_size);
    let mut scores_by_date: FactorScoresByDate = HashMap::new();
    let mut missing_days = Vec::new();
    for day in &score_days {
        let key = SignalDataCacheKey::combo_scores(
            &config.combo_name,
            &config.version,
            *day,
            *day,
            config.score_direction,
            score_candidate_pool_size,
            config.universe_profile,
        );
        if let Some(cached) = cache.cached_combo_scores(&key) {
            scores_by_date.extend(cached.as_ref().clone());
        } else {
            missing_days.push(*day);
        }
    }

    if !missing_days.is_empty() {
        let sql = combo_score_load_dates_sql(
            config.score_direction,
            score_candidate_pool_size,
            config.universe_profile,
        );
        let mut query = sqlx::query_as::<_, (String, NaiveDate, Option<f64>)>(&sql)
            .bind(&config.combo_name)
            .bind(&config.version)
            .bind(&missing_days);
        if let Some(limit) = score_candidate_pool_size {
            query = query.bind(limit as i64);
        }
        let rows = query
            .fetch_all(pool)
            .await
            .map_err(|e| format!("Failed to load combo scores for sparse dates: {}", e))?;
        let loaded_scores = factor_scores_by_date_from_rows(rows);

        for day in missing_days {
            let day_scores = loaded_scores.get(&day).cloned().unwrap_or_default();
            let day_map = if day_scores.is_empty() {
                HashMap::new()
            } else {
                HashMap::from([(day, day_scores.clone())])
            };
            let key = SignalDataCacheKey::combo_scores(
                &config.combo_name,
                &config.version,
                day,
                day,
                config.score_direction,
                score_candidate_pool_size,
                config.universe_profile,
            );
            cache.insert_combo_scores(key, day_map);
            if !day_scores.is_empty() {
                scores_by_date.insert(day, day_scores);
            }
        }
    }

    if scores_by_date.is_empty() {
        return Err("No combo scores found".into());
    }

    Ok(Arc::new(scores_by_date))
}

fn factor_scores_by_date_from_rows(
    rows: Vec<(String, NaiveDate, Option<f64>)>,
) -> FactorScoresByDate {
    let mut scores_by_date: FactorScoresByDate = HashMap::new();
    for (sym, date, score) in rows {
        let val = score.unwrap_or(0.0);
        if val.is_finite() {
            scores_by_date.entry(date).or_default().push((sym, val));
        }
    }
    scores_by_date
}

#[cfg(test)]
pub(crate) fn combo_score_load_sql(
    score_direction: ScoreDirection,
    score_candidate_pool_size: Option<usize>,
    universe_profile: TradableUniverseProfile,
) -> String {
    let universe_join = tradable_universe_join_sql(universe_profile);
    let universe_filter = tradable_universe_filter_sql(universe_profile)
        .map(|filter| format!("\n           AND {filter}"))
        .unwrap_or_default();
    let Some(_) = normalize_score_candidate_pool_size(score_candidate_pool_size) else {
        return format!(
            "SELECT mfv.symbol, mfv.trade_date, mfv.raw_score
         FROM multi_factor_value mfv{universe_join}
         WHERE mfv.combo_name = $1 AND mfv.version = $2
           AND mfv.trade_date >= $3 AND mfv.trade_date <= $4
           AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){universe_filter}
         ORDER BY mfv.trade_date, mfv.symbol"
        );
    };

    let score_order = match score_direction {
        ScoreDirection::Descending => "COALESCE(mfv.raw_score, 0.0) DESC",
        ScoreDirection::Ascending => "COALESCE(mfv.raw_score, 0.0) ASC",
    };
    format!(
        "SELECT symbol, trade_date, raw_score
         FROM (
             SELECT mfv.symbol, mfv.trade_date, mfv.raw_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY mfv.trade_date
                        ORDER BY {score_order}, mfv.symbol ASC
                    ) AS score_rank
             FROM multi_factor_value mfv{universe_join}
             WHERE mfv.combo_name = $1 AND mfv.version = $2
               AND mfv.trade_date >= $3 AND mfv.trade_date <= $4
               AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){universe_filter}
         ) ranked
         WHERE score_rank <= $5
         ORDER BY trade_date, score_rank, symbol"
    )
}

pub(crate) fn combo_score_load_dates_sql(
    score_direction: ScoreDirection,
    score_candidate_pool_size: Option<usize>,
    universe_profile: TradableUniverseProfile,
) -> String {
    let universe_join = tradable_universe_join_sql(universe_profile);
    let universe_filter = tradable_universe_filter_sql(universe_profile)
        .map(|filter| format!("\n           AND {filter}"))
        .unwrap_or_default();
    let Some(_) = normalize_score_candidate_pool_size(score_candidate_pool_size) else {
        return format!(
            "SELECT mfv.symbol, mfv.trade_date, mfv.raw_score
         FROM multi_factor_value mfv{universe_join}
         WHERE mfv.combo_name = $1 AND mfv.version = $2
           AND mfv.trade_date = ANY($3)
           AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){universe_filter}
         ORDER BY mfv.trade_date, mfv.symbol"
        );
    };

    let score_order = match score_direction {
        ScoreDirection::Descending => "COALESCE(mfv.raw_score, 0.0) DESC",
        ScoreDirection::Ascending => "COALESCE(mfv.raw_score, 0.0) ASC",
    };
    format!(
        "SELECT symbol, trade_date, raw_score
         FROM (
             SELECT mfv.symbol, mfv.trade_date, mfv.raw_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY mfv.trade_date
                        ORDER BY {score_order}, mfv.symbol ASC
                    ) AS score_rank
             FROM multi_factor_value mfv{universe_join}
             WHERE mfv.combo_name = $1 AND mfv.version = $2
               AND mfv.trade_date = ANY($3)
               AND (mfv.available_at IS NULL OR mfv.available_at <= mfv.trade_date){universe_filter}
         ) ranked
         WHERE score_rank <= $4
         ORDER BY trade_date, score_rank, symbol"
    )
}

fn tradable_universe_join_sql(profile: TradableUniverseProfile) -> &'static str {
    match profile {
        TradableUniverseProfile::All => "",
        TradableUniverseProfile::ListedNonSt
        | TradableUniverseProfile::MainBoardNonSt
        | TradableUniverseProfile::MainChinextNonSt => {
            "\n         JOIN market_stock ms ON ms.symbol = mfv.symbol"
        }
    }
}

fn tradable_universe_filter_sql(profile: TradableUniverseProfile) -> Option<&'static str> {
    match profile {
        TradableUniverseProfile::All => None,
        TradableUniverseProfile::ListedNonSt => {
            Some("ms.list_status = 'L' AND COALESCE(ms.is_st, false) = false")
        }
        TradableUniverseProfile::MainBoardNonSt => Some(
            "ms.list_status = 'L'
           AND COALESCE(ms.is_st, false) = false
           AND ms.exchange IN ('SSE', 'SZSE')
           AND ms.market = '主板'
           AND COALESCE(ms.market, '') NOT ILIKE '%创业%'
           AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
           AND COALESCE(ms.market, '') NOT ILIKE '%北交%'",
        ),
        TradableUniverseProfile::MainChinextNonSt => Some(
            "ms.list_status = 'L'
           AND COALESCE(ms.is_st, false) = false
           AND ms.exchange IN ('SSE', 'SZSE')
           AND ms.market IN ('主板', '创业板')
           AND ms.symbol NOT LIKE '688%SH'
           AND COALESCE(ms.market, '') NOT ILIKE '%科创%'
           AND COALESCE(ms.market, '') NOT ILIKE '%北交%'",
        ),
    }
}

pub(crate) async fn apply_factor_liquidity_filter(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    scores_by_date: &mut FactorScoresByDate,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(), String> {
    let Some(min_amount) = config.min_daily_amount_cny else {
        return Ok(());
    };
    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if all_symbols.is_empty() {
        return Ok(());
    }

    let average_amounts =
        load_average_amounts_cached(pool, cache, &all_symbols, start_date, end_date).await?;
    let stats =
        retain_scores_with_min_average_amount(scores_by_date, min_amount, average_amounts.as_ref());

    info!(
        "Liquidity filter (min ~{} CNY/day): kept {}/{} stock-date pairs ({} unique symbols)",
        min_amount as u64, stats.after, stats.before, stats.liquid_symbols
    );

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiquidityFilterStats {
    pub(crate) before: usize,
    pub(crate) after: usize,
    pub(crate) liquid_symbols: usize,
}

pub(crate) fn retain_scores_with_min_average_amount(
    scores_by_date: &mut FactorScoresByDate,
    min_amount_cny: f64,
    average_amounts: &AverageAmounts,
) -> LiquidityFilterStats {
    let min_amount_1k = min_amount_cny / 1000.0;
    let liquid_symbols: HashSet<&str> = average_amounts
        .iter()
        .filter_map(|(symbol, amount)| {
            if *amount >= min_amount_1k {
                Some(symbol.as_str())
            } else {
                None
            }
        })
        .collect();
    let before: usize = scores_by_date.values().flatten().count();
    for stocks in scores_by_date.values_mut() {
        stocks.retain(|(symbol, _)| liquid_symbols.contains(symbol.as_str()));
    }
    let after: usize = scores_by_date.values().flatten().count();

    LiquidityFilterStats {
        before,
        after,
        liquid_symbols: liquid_symbols.len(),
    }
}

pub(crate) fn build_pit_average_amounts_by_date(
    amount_history: &AverageAmountHistory,
    as_of_dates: &[NaiveDate],
    lookback_days: usize,
) -> AverageAmountsByDate {
    if amount_history.is_empty() || as_of_dates.is_empty() {
        return HashMap::new();
    }
    let mut dates = as_of_dates.to_vec();
    dates.sort_unstable();
    dates.dedup();
    let lookback_days = lookback_days.max(1);
    let mut amounts_by_date: AverageAmountsByDate =
        dates.iter().map(|date| (*date, HashMap::new())).collect();

    for (symbol, rows) in amount_history {
        let mut rows = rows
            .iter()
            .copied()
            .filter(|(_, amount)| amount.is_finite() && *amount > 0.0)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        rows.sort_unstable_by_key(|(date, _)| *date);

        let mut left = 0usize;
        let mut right = 0usize;
        let mut amount_sum = 0.0;
        for as_of in &dates {
            while right < rows.len() && rows[right].0 <= *as_of {
                amount_sum += rows[right].1;
                right += 1;
            }
            while right.saturating_sub(left) > lookback_days {
                amount_sum -= rows[left].1;
                left += 1;
            }
            let count = right.saturating_sub(left);
            if count == 0 {
                continue;
            }
            amounts_by_date
                .entry(*as_of)
                .or_default()
                .insert(symbol.clone(), amount_sum / count as f64);
        }
    }

    amounts_by_date.retain(|_, amounts| !amounts.is_empty());
    amounts_by_date
}

pub(crate) fn normalized_dates(dates: &[NaiveDate]) -> Vec<NaiveDate> {
    let mut dates = dates.to_vec();
    dates.sort_unstable();
    dates.dedup();
    dates
}

pub(crate) fn date_span(dates: &[NaiveDate]) -> Option<(NaiveDate, NaiveDate)> {
    let dates = normalized_dates(dates);
    Some((*dates.first()?, *dates.last()?))
}

fn average_amounts_for_score_day(
    average_amounts_by_date: &AverageAmountsByDate,
    score_day: NaiveDate,
) -> AverageAmounts {
    average_amounts_by_date
        .get(&score_day)
        .cloned()
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn build_rebalance_factor_signals<F>(
    trading_days: &[NaiveDate],
    scores_by_date: &HashMap<NaiveDate, Vec<(String, f64)>>,
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
    active_config_for_day: F,
) -> Result<HashMap<NaiveDate, StrategySignal>, String>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
{
    build_rebalance_factor_signals_with_return_risk_matrices(
        trading_days,
        scores_by_date,
        base_config,
        return_history,
        average_amounts_by_date,
        industry_by_symbol,
        &HashMap::new(),
        active_config_for_day,
    )
}

#[cfg(test)]
pub(crate) fn build_rebalance_factor_signals_with_return_risk_matrices<F>(
    trading_days: &[NaiveDate],
    scores_by_date: &HashMap<NaiveDate, Vec<(String, f64)>>,
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
    return_risk_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskMatrix>>,
    active_config_for_day: F,
) -> Result<HashMap<NaiveDate, StrategySignal>, String>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
{
    build_rebalance_factor_signals_with_score_selector_and_return_risk_matrices(
        trading_days,
        base_config,
        return_history,
        average_amounts_by_date,
        industry_by_symbol,
        return_risk_matrices,
        &HashMap::new(),
        false,
        |score_day, _active_config| scores_by_date.get(&score_day).cloned(),
        active_config_for_day,
    )
}

#[cfg(test)]
pub(crate) fn build_rebalance_factor_signals_with_return_risk_stats_matrices<F>(
    trading_days: &[NaiveDate],
    scores_by_date: &HashMap<NaiveDate, Vec<(String, f64)>>,
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
    return_risk_stats_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskStatsMatrix>>,
    active_config_for_day: F,
) -> Result<HashMap<NaiveDate, StrategySignal>, String>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
{
    build_rebalance_factor_signals_with_score_selector_and_return_risk_matrices(
        trading_days,
        base_config,
        return_history,
        average_amounts_by_date,
        industry_by_symbol,
        &HashMap::new(),
        return_risk_stats_matrices,
        true,
        |score_day, _active_config| scores_by_date.get(&score_day).cloned(),
        active_config_for_day,
    )
}

#[cfg(test)]
pub(crate) fn build_rebalance_factor_signals_with_score_selector<F, S>(
    trading_days: &[NaiveDate],
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
    scores_for_day: S,
    active_config_for_day: F,
) -> Result<HashMap<NaiveDate, StrategySignal>, String>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
    S: Fn(NaiveDate, &SignalConfig) -> Option<Vec<(String, f64)>>,
{
    build_rebalance_factor_signals_with_score_selector_and_return_risk_matrices(
        trading_days,
        base_config,
        return_history,
        average_amounts_by_date,
        industry_by_symbol,
        &HashMap::new(),
        &HashMap::new(),
        false,
        scores_for_day,
        active_config_for_day,
    )
}

pub(crate) fn build_rebalance_factor_signals_with_score_selector_and_return_risk_matrices<F, S>(
    trading_days: &[NaiveDate],
    base_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
    return_risk_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskMatrix>>,
    return_risk_stats_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskStatsMatrix>>,
    prefer_return_risk_stats_matrices: bool,
    scores_for_day: S,
    active_config_for_day: F,
) -> Result<HashMap<NaiveDate, StrategySignal>, String>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
    S: Fn(NaiveDate, &SignalConfig) -> Option<Vec<(String, f64)>>,
{
    let mut signals: HashMap<NaiveDate, StrategySignal> = HashMap::new();
    let mut previous_target_weights: Option<HashMap<String, Decimal>> = None;

    for (i, &day) in trading_days.iter().enumerate() {
        let active_config = active_config_for_day(day, base_config);
        let min_idx = 1 + active_config.entry_delay_days;
        if i < min_idx {
            continue;
        }
        if (i - min_idx) % active_config.rebalance_freq_days.max(1) != 0 {
            continue;
        }

        let score_day = match score_day_for_signal(trading_days, i, &active_config) {
            Some(day) => day,
            None => continue,
        };
        let average_amounts = average_amounts_for_score_day(average_amounts_by_date, score_day);
        let mut target_weights = match build_portfolio_sleeve_target_weights(
            score_day,
            &active_config,
            return_history,
            &average_amounts,
            industry_by_symbol,
            return_risk_matrices,
            return_risk_stats_matrices,
            prefer_return_risk_stats_matrices,
            &scores_for_day,
        ) {
            Some(weights) => weights,
            None => continue,
        };
        apply_rebalance_path_smoothing(
            &mut target_weights,
            previous_target_weights.as_ref(),
            active_config.rebalance_hysteresis_pct,
            active_config.partial_rebalance_ratio,
        );
        apply_execution_impact_budget(
            &mut target_weights,
            previous_target_weights.as_ref(),
            active_config.execution_impact_budget_profile,
        );

        signals.insert(
            day,
            StrategySignal {
                date: day,
                target_weights: target_weights.clone(),
            },
        );
        previous_target_weights = Some(target_weights);
    }

    info!(
        "Generated {} factor signals (base top-{}, rebalance every {}d, entry_delay {}d)",
        signals.len(),
        base_config.top_n,
        base_config.rebalance_freq_days,
        base_config.entry_delay_days
    );

    Ok(signals)
}

fn build_portfolio_sleeve_target_weights<S>(
    score_day: NaiveDate,
    active_config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    return_risk_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskMatrix>>,
    return_risk_stats_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskStatsMatrix>>,
    prefer_return_risk_stats_matrices: bool,
    scores_for_day: &S,
) -> Option<HashMap<String, Decimal>>
where
    S: Fn(NaiveDate, &SignalConfig) -> Option<Vec<(String, f64)>>,
{
    let mut base_config = active_config.clone();
    base_config.portfolio_sleeve = None;
    let base_weights = build_single_sleeve_target_weights(
        score_day,
        &base_config,
        return_history,
        average_amounts,
        industry_by_symbol,
        return_risk_matrices,
        return_risk_stats_matrices,
        prefer_return_risk_stats_matrices,
        scores_for_day,
    )?;

    let Some(sleeve) = active_config.portfolio_sleeve.as_ref() else {
        return Some(base_weights);
    };
    let sleeve_weight = sleeve.weight.clamp(0.0, 1.0);
    if sleeve_weight <= f64::EPSILON {
        return Some(base_weights);
    }
    let sleeve_config = score_source_config_for_portfolio_sleeve(active_config, sleeve);
    let Some(sleeve_weights) = build_single_sleeve_target_weights(
        score_day,
        &sleeve_config,
        return_history,
        average_amounts,
        industry_by_symbol,
        return_risk_matrices,
        return_risk_stats_matrices,
        prefer_return_risk_stats_matrices,
        scores_for_day,
    ) else {
        return Some(base_weights);
    };

    Some(blend_portfolio_sleeve_weights(
        base_weights,
        1.0 - sleeve_weight,
        sleeve_weights,
        sleeve_weight,
    ))
}

fn build_single_sleeve_target_weights<S>(
    score_day: NaiveDate,
    config: &SignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    return_risk_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskMatrix>>,
    return_risk_stats_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskStatsMatrix>>,
    prefer_return_risk_stats_matrices: bool,
    scores_for_day: &S,
) -> Option<HashMap<String, Decimal>>
where
    S: Fn(NaiveDate, &SignalConfig) -> Option<Vec<(String, f64)>>,
{
    let mut prev_scores = scores_for_day(score_day, config)?;
    sort_factor_scores(&mut prev_scores, config.score_direction);

    let skip_count = if config.skip_top_pct > 0.0 {
        (prev_scores.len() as f64 * config.skip_top_pct).ceil() as usize
    } else {
        0
    };
    let candidates: Vec<(String, f64)> = prev_scores
        .iter()
        .skip(skip_count)
        .map(|(symbol, score)| (symbol.clone(), *score))
        .collect();
    if candidates.len() < config.top_n.min(5) {
        return None;
    }

    let portfolio_config = PortfolioConstructionConfig::from(config);
    let target_weights = if prefer_return_risk_stats_matrices {
        return_risk_stats_matrices_cover_required_lookbacks(
            return_risk_stats_matrices,
            &portfolio_config,
        )
        .then(|| {
            build_portfolio_weights_with_return_risk_stats_matrices(
                score_day,
                &candidates,
                return_risk_stats_matrices,
                average_amounts,
                industry_by_symbol,
                &portfolio_config,
            )
        })
        .unwrap_or_else(|| {
            build_portfolio_weights_with_return_risk_matrices(
                score_day,
                &candidates,
                return_history,
                average_amounts,
                industry_by_symbol,
                &portfolio_config,
                Some(return_risk_matrices),
            )
        })
    } else {
        build_portfolio_weights_with_return_risk_matrices(
            score_day,
            &candidates,
            return_history,
            average_amounts,
            industry_by_symbol,
            &portfolio_config,
            Some(return_risk_matrices),
        )
    };
    if target_weights.len() < config.top_n.min(5) {
        return None;
    }
    Some(target_weights)
}

fn blend_portfolio_sleeve_weights(
    base_weights: HashMap<String, Decimal>,
    base_weight: f64,
    sleeve_weights: HashMap<String, Decimal>,
    sleeve_weight: f64,
) -> HashMap<String, Decimal> {
    let base_weight = decimal_from_unit_f64(base_weight);
    let sleeve_weight = decimal_from_unit_f64(sleeve_weight);
    let mut blended = HashMap::new();

    for (symbol, weight) in base_weights {
        let scaled = weight * base_weight;
        if scaled > Decimal::ZERO {
            blended.insert(symbol, scaled);
        }
    }
    for (symbol, weight) in sleeve_weights {
        let scaled = weight * sleeve_weight;
        if scaled > Decimal::ZERO {
            *blended.entry(symbol).or_insert(Decimal::ZERO) += scaled;
        }
    }
    blended.retain(|_, weight| *weight > Decimal::ZERO);
    blended
}

fn decimal_from_unit_f64(value: f64) -> Decimal {
    Decimal::from_f64(value.clamp(0.0, 1.0)).unwrap_or(Decimal::ZERO)
}

pub(crate) async fn apply_prediction_liquidity_filter(
    pool: &PgPool,
    scores_by_date: &mut HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>,
    config: &PredictionSignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(), String> {
    let Some(min_amount) = config.min_daily_amount_cny else {
        return Ok(());
    };

    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if all_symbols.is_empty() {
        return Ok(());
    }

    let min_amount_1k = min_amount / 1000.0;
    let liquid_rows: Vec<(String,)> = sqlx::query_as(
        "SELECT symbol FROM (
            SELECT symbol, AVG(amount) as avg_amt
            FROM market_stock_daily_bar_adj
            WHERE symbol = ANY($1)
              AND trade_date >= $2 AND trade_date <= $3
              AND amount > 0
            GROUP BY symbol
            HAVING AVG(amount) >= $4
        ) sub",
    )
    .bind(&all_symbols)
    .bind(start_date)
    .bind(end_date)
    .bind(min_amount_1k)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load liquidity data: {}", e))?;

    let liquid_set: HashSet<String> = liquid_rows.into_iter().map(|(s,)| s).collect();
    let before: usize = scores_by_date.values().flatten().count();
    for stocks in scores_by_date.values_mut() {
        stocks.retain(|(sym, _, _)| liquid_set.contains(sym));
    }
    let after: usize = scores_by_date.values().flatten().count();
    info!(
        "Prediction liquidity filter (min ~{} CNY/day): kept {}/{} stock-date pairs ({} unique symbols)",
        min_amount as u64,
        after,
        before,
        liquid_set.len()
    );

    Ok(())
}

pub(crate) async fn load_open_trading_days(
    pool: &PgPool,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<NaiveDate>, String> {
    sqlx::query_as::<_, (NaiveDate,)>(
        "SELECT trade_date FROM market_trade_calendar
         WHERE exchange = 'SSE' AND is_open = true
           AND trade_date >= $1 AND trade_date <= $2
         ORDER BY trade_date",
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map(|rows| rows.into_iter().map(|(d,)| d).collect())
    .map_err(|e| format!("Failed to load calendar: {}", e))
}

pub async fn load_open_trading_days_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Arc<Vec<NaiveDate>>, String> {
    let key = SignalDataCacheKey::trading_days(start_date, end_date);
    if let Some(days) = cache.cached_trading_days(&key) {
        return Ok(days);
    }
    let days = load_open_trading_days(pool, start_date, end_date).await?;
    Ok(cache.insert_trading_days(key, days))
}

pub(crate) fn classify_market_regime(returns: &[f64], policy: &MarketRegimePolicy) -> MarketRegime {
    let returns = returns
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if returns.len() < policy.min_observations.max(1) {
        return MarketRegime::Mixed;
    }

    let total_return = returns.iter().fold(1.0, |acc, ret| acc * (1.0 + ret)) - 1.0;
    let volatility = annualized_return_volatility(&returns);
    let drawdown = drawdown_from_return_path(&returns);

    if volatility >= policy.high_volatility_threshold {
        MarketRegime::HighVolatility
    } else if total_return <= policy.bear_return_threshold
        || drawdown >= policy.bear_drawdown_threshold
    {
        MarketRegime::Bear
    } else if total_return >= policy.bull_return_threshold && drawdown <= policy.bull_max_drawdown {
        MarketRegime::Bull
    } else if volatility <= policy.sideways_volatility_threshold
        && total_return.abs() <= policy.sideways_abs_return_threshold
    {
        MarketRegime::Sideways
    } else {
        MarketRegime::Mixed
    }
}

pub(crate) fn trailing_market_returns(
    returns: &[(NaiveDate, f64)],
    signal_day: NaiveDate,
    lookback_days: usize,
) -> Vec<f64> {
    let mut values = returns
        .iter()
        .filter(|(date, value)| *date < signal_day && value.is_finite())
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    if values.len() > lookback_days {
        values = values[values.len() - lookback_days..].to_vec();
    }
    values
}

fn annualized_return_volatility(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / returns.len() as f64;
    variance.sqrt() * (252.0_f64).sqrt()
}

fn drawdown_from_return_path(returns: &[f64]) -> f64 {
    let mut nav = 1.0;
    let mut peak = 1.0;
    let mut max_drawdown = 0.0;
    for ret in returns {
        nav *= 1.0 + ret;
        if nav > peak {
            peak = nav;
        }
        if peak > 0.0 {
            let drawdown = 1.0 - nav / peak;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }
    }
    max_drawdown
}

pub(crate) fn build_rebalance_prediction_signals(
    trading_days: &[NaiveDate],
    scores_by_date: &HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>,
    config: &PredictionSignalConfig,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts_by_date: &AverageAmountsByDate,
    industry_by_symbol: &HashMap<String, String>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let min_idx = 1 + config.entry_delay_days;
    let mut signals = HashMap::new();
    let mut previous_target_weights: Option<HashMap<String, Decimal>> = None;

    for (i, &day) in trading_days.iter().enumerate() {
        if i < min_idx || (i - min_idx) % config.rebalance_freq_days.max(1) != 0 {
            continue;
        }
        let score_day = match prediction_score_day_for_signal(trading_days, i, config) {
            Some(day) => day,
            None => continue,
        };
        let prev_scores = match scores_by_date.get(&score_day) {
            Some(scores) => scores,
            None => continue,
        };

        let skip_count = if config.skip_top_pct > 0.0 {
            (prev_scores.len() as f64 * config.skip_top_pct).ceil() as usize
        } else {
            0
        };
        let candidates: Vec<(String, f64)> = prev_scores
            .iter()
            .skip(skip_count)
            .map(|(symbol, score, _)| (symbol.clone(), *score))
            .collect();
        // Regime-aware parameter adjustment for prediction signals
        let (effective_top_n, regime_max_gross) = if config.market_regime.is_some() {
            let regime = detect_market_regime_from_returns(
                return_history,
                score_day,
                config.risk_budget_lookback_days,
            );
            match regime {
                MarketRegime::Bear | MarketRegime::HighVolatility => (
                    (config.top_n as f64 * 0.7).ceil() as usize,
                    (config.max_gross_exposure * 0.75).max(0.5),
                ),
                MarketRegime::Bull => (config.top_n, config.max_gross_exposure),
                _ => (config.top_n, config.max_gross_exposure),
            }
        } else {
            (config.top_n, config.max_gross_exposure)
        };
        let min_candidates = effective_top_n.min(5);
        if candidates.len() < min_candidates {
            continue;
        }

        let average_amounts = average_amounts_for_score_day(average_amounts_by_date, score_day);
        let mut port_config = PortfolioConstructionConfig::from(config);
        if regime_max_gross < config.max_gross_exposure {
            port_config.max_gross_exposure = regime_max_gross;
        }
        let mut target_weights = build_portfolio_weights(
            score_day,
            &candidates,
            return_history,
            &average_amounts,
            industry_by_symbol,
            &port_config,
        );
        if target_weights.len() < min_candidates {
            continue;
        }
        apply_rebalance_path_smoothing(
            &mut target_weights,
            previous_target_weights.as_ref(),
            config.rebalance_hysteresis_pct,
            config.partial_rebalance_ratio,
        );
        apply_execution_impact_budget(
            &mut target_weights,
            previous_target_weights.as_ref(),
            config.execution_impact_budget_profile,
        );

        signals.insert(
            day,
            StrategySignal {
                date: day,
                target_weights: target_weights.clone(),
            },
        );
        previous_target_weights = Some(target_weights);
    }

    if signals.is_empty() {
        return Err("No prediction signals generated".into());
    }

    info!(
        "Generated {} prediction signals (prediction_set={}, top-{}, rebalance every {}d, entry_delay {}d)",
        signals.len(),
        config.prediction_set_id,
        config.top_n,
        config.rebalance_freq_days,
        config.entry_delay_days
    );

    Ok(signals)
}

pub(crate) fn apply_rebalance_path_smoothing(
    target_weights: &mut HashMap<String, Decimal>,
    previous_target_weights: Option<&HashMap<String, Decimal>>,
    rebalance_hysteresis_pct: f64,
    partial_rebalance_ratio: f64,
) {
    let Some(previous_target_weights) = previous_target_weights else {
        return;
    };
    let hysteresis = finite_decimal(rebalance_hysteresis_pct, 0.0, 0.0, 1.0);
    let partial = finite_decimal(partial_rebalance_ratio, 1.0, 0.0, 1.0);
    if hysteresis.is_zero() && partial == Decimal::ONE {
        return;
    }
    if target_weights.is_empty() {
        return;
    }

    let target_gross = target_weights.values().copied().sum::<Decimal>();
    if target_gross.is_zero() {
        target_weights.clear();
        return;
    }

    let raw_target_weights = target_weights.clone();
    let mut symbols = previous_target_weights
        .keys()
        .chain(raw_target_weights.keys())
        .cloned()
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();

    target_weights.clear();
    for symbol in symbols {
        let previous = previous_target_weights
            .get(&symbol)
            .copied()
            .unwrap_or_default();
        let target = raw_target_weights.get(&symbol).copied().unwrap_or_default();
        let delta = target - previous;
        let adjusted = if delta.abs() <= hysteresis {
            previous
        } else {
            previous + delta * partial
        };
        if adjusted > Decimal::ZERO {
            target_weights.insert(symbol, adjusted);
        }
    }

    let adjusted_gross = target_weights.values().copied().sum::<Decimal>();
    if adjusted_gross > target_gross && !adjusted_gross.is_zero() {
        let scale = target_gross / adjusted_gross;
        for weight in target_weights.values_mut() {
            *weight *= scale;
        }
    }
    target_weights.retain(|_, weight| *weight > Decimal::ZERO);
}

pub(crate) fn apply_execution_impact_budget(
    target_weights: &mut HashMap<String, Decimal>,
    previous_target_weights: Option<&HashMap<String, Decimal>>,
    profile: ExecutionImpactBudgetProfile,
) {
    let Some(params) = profile.params() else {
        return;
    };
    let Some(previous_target_weights) = previous_target_weights else {
        return;
    };
    if target_weights.is_empty() || previous_target_weights.is_empty() {
        return;
    }

    let max_turnover =
        finite_decimal(params.max_rebalance_turnover_pct, 1.0, 0.0, 2.0).max(Decimal::ZERO);
    if max_turnover.is_zero() {
        target_weights.clear();
        return;
    }
    let max_new_name_weight =
        finite_decimal(params.max_new_name_weight_pct, 1.0, 0.0, 1.0).max(Decimal::ZERO);

    let mut desired = target_weights.clone();
    let mut released = Decimal::ZERO;
    let previous_symbols = previous_target_weights
        .iter()
        .filter(|(_, weight)| **weight > Decimal::ZERO)
        .map(|(symbol, _)| symbol.clone())
        .collect::<HashSet<_>>();

    for (symbol, weight) in desired.iter_mut() {
        let previous = previous_target_weights
            .get(symbol)
            .copied()
            .unwrap_or_default();
        if previous.is_zero() && *weight > max_new_name_weight {
            released += *weight - max_new_name_weight;
            *weight = max_new_name_weight;
        }
    }

    if released > Decimal::ZERO && !previous_symbols.is_empty() {
        redistribute_released_weight_to_existing_positions(
            &mut desired,
            previous_target_weights,
            &previous_symbols,
            released,
        );
    }

    let mut symbols = previous_target_weights
        .keys()
        .chain(desired.keys())
        .cloned()
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();

    let gross_turnover = symbols.iter().fold(Decimal::ZERO, |acc, symbol| {
        let previous = previous_target_weights
            .get(symbol)
            .copied()
            .unwrap_or_default();
        let target = desired.get(symbol).copied().unwrap_or_default();
        acc + (target - previous).abs()
    });

    if gross_turnover.is_zero() {
        target_weights.clear();
        for (symbol, weight) in previous_target_weights {
            if *weight > Decimal::ZERO {
                target_weights.insert(symbol.clone(), *weight);
            }
        }
        return;
    }

    let scale = if gross_turnover > max_turnover {
        max_turnover / gross_turnover
    } else {
        Decimal::ONE
    };

    target_weights.clear();
    for symbol in symbols {
        let previous = previous_target_weights
            .get(&symbol)
            .copied()
            .unwrap_or_default();
        let target = desired.get(&symbol).copied().unwrap_or_default();
        let adjusted = previous + (target - previous) * scale;
        if adjusted > Decimal::ZERO {
            target_weights.insert(symbol, adjusted);
        }
    }
    target_weights.retain(|_, weight| *weight > Decimal::ZERO);
}

fn redistribute_released_weight_to_existing_positions(
    weights: &mut HashMap<String, Decimal>,
    previous_target_weights: &HashMap<String, Decimal>,
    previous_symbols: &HashSet<String>,
    released: Decimal,
) {
    let mut rooms = previous_symbols
        .iter()
        .filter_map(|symbol| {
            let previous = previous_target_weights
                .get(symbol)
                .copied()
                .unwrap_or_default();
            let current = weights.get(symbol).copied().unwrap_or_default();
            let room = (previous - current).max(Decimal::ZERO);
            if room > Decimal::ZERO {
                Some((symbol.clone(), room))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    rooms.sort_by(|(left, _), (right, _)| left.cmp(right));

    let total_room = rooms
        .iter()
        .map(|(_, room)| *room)
        .fold(Decimal::ZERO, |acc, room| acc + room);
    if total_room.is_zero() {
        return;
    }

    let allocation = released.min(total_room);
    for (symbol, room) in rooms {
        let add = allocation * room / total_room;
        if add > Decimal::ZERO {
            *weights.entry(symbol).or_insert(Decimal::ZERO) += add.min(room);
        }
    }
}

fn finite_decimal(value: f64, default: f64, min: f64, max: f64) -> Decimal {
    let bounded = if value.is_finite() {
        value.clamp(min, max)
    } else {
        default
    };
    Decimal::from_f64(bounded).unwrap_or_else(|| Decimal::from_f64(default).unwrap_or_default())
}

pub(crate) fn score_day_for_signal(
    trading_days: &[NaiveDate],
    signal_day_idx: usize,
    config: &SignalConfig,
) -> Option<NaiveDate> {
    let score_idx = signal_day_idx.checked_sub(1 + config.entry_delay_days)?;
    trading_days.get(score_idx).copied()
}

fn prediction_score_day_for_signal(
    trading_days: &[NaiveDate],
    signal_day_idx: usize,
    config: &PredictionSignalConfig,
) -> Option<NaiveDate> {
    let score_idx = signal_day_idx.checked_sub(1 + config.entry_delay_days)?;
    trading_days.get(score_idx).copied()
}

pub fn score_days_for_signal_dates(
    trading_days: &[NaiveDate],
    signal_dates: &[NaiveDate],
    entry_delay_days: usize,
) -> Vec<NaiveDate> {
    let day_index: HashMap<NaiveDate, usize> = trading_days
        .iter()
        .enumerate()
        .map(|(index, date)| (*date, index))
        .collect();
    let offset = 1 + entry_delay_days;
    let mut score_days = signal_dates
        .iter()
        .filter_map(|signal_date| day_index.get(signal_date).copied())
        .filter_map(|signal_index| signal_index.checked_sub(offset))
        .filter_map(|score_index| trading_days.get(score_index).copied())
        .collect::<Vec<_>>();
    score_days.sort_unstable();
    score_days.dedup();
    score_days
}

pub(crate) fn rebalance_score_days<F>(
    trading_days: &[NaiveDate],
    base_config: &SignalConfig,
    active_config_for_day: F,
) -> Vec<NaiveDate>
where
    F: Fn(NaiveDate, &SignalConfig) -> SignalConfig,
{
    let mut score_days = Vec::new();
    for (i, &day) in trading_days.iter().enumerate() {
        let active_config = active_config_for_day(day, base_config);
        let min_idx = 1 + active_config.entry_delay_days;
        if i < min_idx {
            continue;
        }
        if (i - min_idx) % active_config.rebalance_freq_days.max(1) != 0 {
            continue;
        }
        if let Some(score_day) = score_day_for_signal(trading_days, i, &active_config) {
            score_days.push(score_day);
        }
    }
    normalized_dates(&score_days)
}

pub(crate) fn portfolio_history_lookback_days(config: &PortfolioConstructionConfig) -> usize {
    config
        .correlation_lookback_days
        .max(config.kelly_lookback_days)
        .max(config.risk_budget_lookback_days)
        .max(1)
}

fn portfolio_return_risk_matrix_lookback_days(config: &PortfolioConstructionConfig) -> Vec<usize> {
    let mut lookbacks = Vec::new();
    if candidate_ranking_uses_relative_strength(config.candidate_ranking_profile)
        || config.candidate_risk_filter_profile.params().is_some()
        || config.style_risk_budget_profile.params().is_some()
        || config.risk_contribution_control_profile.params().is_some()
        || matches!(
            config.portfolio_method,
            PortfolioConstructionMethod::RiskBudget | PortfolioConstructionMethod::MinVariance
        )
    {
        lookbacks.push(config.risk_budget_lookback_days.max(1));
    }
    if config.max_pairwise_correlation.is_some() {
        lookbacks.push(config.correlation_lookback_days.max(1));
    }
    if matches!(
        config.portfolio_method,
        PortfolioConstructionMethod::Heuristic
    ) && config.kelly_fraction > 0.0
    {
        lookbacks.push(config.kelly_lookback_days.max(1));
    }
    lookbacks.sort_unstable();
    lookbacks.dedup();
    lookbacks
}

pub(crate) fn return_history_query_start(start_date: NaiveDate, lookback_days: usize) -> NaiveDate {
    start_date - Duration::days((lookback_days as i64).saturating_mul(3))
}

pub(crate) fn average_amount_history_query_start(start_date: NaiveDate, lookback_days: usize) -> NaiveDate {
    start_date - Duration::days((lookback_days as i64).saturating_mul(3).max(1))
}

pub(crate) fn filter_dated_values(
    rows: &[(NaiveDate, f64)],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<(NaiveDate, f64)> {
    rows.iter()
        .copied()
        .filter(|(date, _)| *date >= start_date && *date <= end_date)
        .collect()
}

pub(crate) const SYMBOL_RETURN_HISTORY_SQL: &str = "SELECT symbol, trade_date, pct_change
         FROM market_stock_daily_bar
         WHERE symbol = ANY($1)
           AND trade_date >= $2 AND trade_date <= $3
           AND pct_change IS NOT NULL
         ORDER BY symbol, trade_date";

pub(crate) fn daily_return_from_pct_change(pct_change: Decimal) -> Option<f64> {
    pct_change
        .to_f64()
        .filter(|value| value.is_finite() && *value > -1.0)
}

pub(crate) async fn load_symbol_return_history(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<HashMap<String, Vec<(NaiveDate, f64)>>, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }

    let query_start = return_history_query_start(start_date, lookback_days);
    let rows: Vec<(String, NaiveDate, Decimal)> = sqlx::query_as(SYMBOL_RETURN_HISTORY_SQL)
        .bind(symbols)
        .bind(query_start)
        .bind(end_date)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to load portfolio construction return history: {}",
                e
            )
        })?;

    let mut returns_by_symbol: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
    for (symbol, date, pct_change) in rows {
        if let Some(daily_return) = daily_return_from_pct_change(pct_change) {
            returns_by_symbol
                .entry(symbol)
                .or_default()
                .push((date, daily_return));
        }
    }

    Ok(returns_by_symbol)
}

async fn load_symbol_return_history_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<SymbolReturnHistory>, String> {
    let (missing_symbols, mut history) =
        cache.cached_return_history_symbols(symbols, start_date, end_date, lookback_days);
    if missing_symbols.is_empty() {
        return Ok(Arc::new(history));
    }

    let loaded_history =
        load_symbol_return_history(pool, &missing_symbols, start_date, end_date, lookback_days)
            .await?;
    let loaded_history = cache.insert_return_history_symbols(
        &missing_symbols,
        start_date,
        end_date,
        lookback_days,
        loaded_history,
    );
    history.extend(loaded_history);
    Ok(Arc::new(history))
}

pub(crate) async fn load_symbol_return_history_persistent_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: &str,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<SymbolReturnHistory>, String> {
    let (missing_symbols, mut history) =
        cache.cached_return_history_symbols(symbols, start_date, end_date, lookback_days);
    if missing_symbols.is_empty() {
        return Ok(Arc::new(history));
    }

    let persistent_key = PersistentMarketFeatureCacheKey::new(
        PersistentMarketFeatureKind::ReturnHistory,
        data_version_id,
        start_date,
        end_date,
        lookback_days,
        &missing_symbols,
    );
    match load_persistent_market_feature_cache(pool, &persistent_key, &missing_symbols).await {
        Ok(Some(persistent_history)) => {
            cache.record_persistent_market_feature_hit(PersistentMarketFeatureKind::ReturnHistory);
            let persistent_history = cache.insert_return_history_symbols(
                &missing_symbols,
                start_date,
                end_date,
                lookback_days,
                persistent_history,
            );
            history.extend(persistent_history);
            return Ok(Arc::new(history));
        }
        Ok(None) => {
            cache.record_persistent_market_feature_miss(PersistentMarketFeatureKind::ReturnHistory);
        }
        Err(error) => {
            warn!(
                cache_key = persistent_key.cache_key,
                error = %error,
                "persistent return history cache read failed; falling back to source table"
            );
        }
    }

    let loaded_history =
        load_symbol_return_history(pool, &missing_symbols, start_date, end_date, lookback_days)
            .await?;
    match store_persistent_market_feature_cache(
        pool,
        &persistent_key,
        &missing_symbols,
        &loaded_history,
    )
    .await
    {
        Ok(true) => {
            cache
                .record_persistent_market_feature_write(PersistentMarketFeatureKind::ReturnHistory);
        }
        Ok(false) => {}
        Err(error) => {
            warn!(
                cache_key = persistent_key.cache_key,
                error = %error,
                "persistent return history cache write failed"
            );
        }
    }
    let loaded_history = cache.insert_return_history_symbols(
        &missing_symbols,
        start_date,
        end_date,
        lookback_days,
        loaded_history,
    );
    history.extend(loaded_history);
    Ok(Arc::new(history))
}

pub(crate) async fn load_return_risk_feature_matrix_persistent_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: Option<&str>,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    score_days: &[NaiveDate],
    lookback_days: usize,
    return_history: Option<&SymbolReturnHistory>,
) -> Result<Arc<ScoreDateReturnRiskMatrix>, String> {
    let symbols = normalized_symbol_key(symbols);
    let score_days = normalized_dates(score_days);
    if symbols.is_empty() || score_days.is_empty() {
        return Ok(Arc::new(ScoreDateReturnRiskMatrix::default()));
    }

    let matrix_key = ReturnRiskFeatureMatrixCacheKey::new(
        start_date,
        end_date,
        lookback_days,
        &symbols,
        &score_days,
    );
    if let Some(matrix) = cache.cached_return_risk_feature_matrix(&matrix_key) {
        return Ok(matrix);
    }

    let persistent_key = data_version_id.map(|data_version_id| {
        PersistentMarketFeatureCacheKey::new_for_dates(
            PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
            data_version_id,
            start_date,
            end_date,
            lookback_days,
            &symbols,
            &score_days,
        )
    });

    if let Some(persistent_key) = persistent_key.as_ref() {
        match load_persistent_return_risk_feature_matrix_cache(
            pool,
            persistent_key,
            &symbols,
            &score_days,
        )
        .await
        {
            Ok(Some(matrix)) => {
                cache.record_persistent_market_feature_hit(
                    PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
                );
                cache.record_persistent_return_risk_feature_matrix_payload_loaded(
                    matrix.row_count(),
                    matrix.return_value_count(),
                );
                return Ok(cache.insert_return_risk_feature_matrix(matrix_key, matrix));
            }
            Ok(None) => {
                cache.record_persistent_market_feature_miss(
                    PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
                );
            }
            Err(error) => {
                warn!(
                    cache_key = persistent_key.cache_key,
                    error = %error,
                    "persistent return/risk feature matrix cache read failed; falling back to return history"
                );
            }
        }
    }

    let matrix = if let Some(return_history) = return_history {
        build_score_date_return_risk_matrix(return_history, &score_days, &symbols, lookback_days)
    } else {
        let history = match data_version_id {
            Some(data_version_id) => {
                load_symbol_return_history_persistent_cached(
                    pool,
                    cache,
                    data_version_id,
                    &symbols,
                    start_date,
                    end_date,
                    lookback_days,
                )
                .await?
            }
            None => {
                load_symbol_return_history_cached(
                    pool,
                    cache,
                    &symbols,
                    start_date,
                    end_date,
                    lookback_days,
                )
                .await?
            }
        };
        build_score_date_return_risk_matrix(history.as_ref(), &score_days, &symbols, lookback_days)
    };

    if let Some(persistent_key) = persistent_key.as_ref() {
        match store_persistent_return_risk_feature_matrix_cache(
            pool,
            persistent_key,
            &symbols,
            &score_days,
            &matrix,
        )
        .await
        {
            Ok(true) => {
                cache.record_persistent_market_feature_write(
                    PersistentMarketFeatureKind::ReturnRiskFeatureMatrix,
                );
                cache.record_persistent_return_risk_feature_matrix_payload_written(
                    matrix.row_count(),
                    matrix.return_value_count(),
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    cache_key = persistent_key.cache_key,
                    error = %error,
                    "persistent return/risk feature matrix cache write failed"
                );
            }
        }
    }

    Ok(cache.insert_return_risk_feature_matrix(matrix_key, matrix))
}

pub(crate) async fn load_portfolio_return_risk_feature_matrices_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: Option<&str>,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    score_days: &[NaiveDate],
    config: &PortfolioConstructionConfig,
    return_history: Option<&SymbolReturnHistory>,
) -> Result<HashMap<usize, Arc<ScoreDateReturnRiskMatrix>>, String> {
    let mut matrices = HashMap::new();
    for lookback_days in portfolio_return_risk_matrix_lookback_days(config) {
        let matrix = load_return_risk_feature_matrix_persistent_cached(
            pool,
            cache,
            data_version_id,
            symbols,
            start_date,
            end_date,
            score_days,
            lookback_days,
            return_history,
        )
        .await?;
        matrices.insert(lookback_days, matrix);
    }
    Ok(matrices)
}

pub(crate) async fn load_symbol_return_history_for_snapshot_scope_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    snapshot_scope: Option<&MarketFeatureSnapshotScope>,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<SymbolReturnHistory>, String> {
    if let Some(snapshot_scope) = snapshot_scope {
        load_symbol_return_history_persistent_cached(
            pool,
            cache,
            &snapshot_scope.data_version_id,
            symbols,
            start_date,
            end_date,
            lookback_days,
        )
        .await
    } else {
        load_symbol_return_history_cached(pool, cache, symbols, start_date, end_date, lookback_days)
            .await
    }
}

pub(crate) fn should_load_raw_return_risk_matrices(
    prefer_return_risk_stats_matrices: bool,
    return_risk_stats_matrices_loaded: bool,
) -> bool {
    !prefer_return_risk_stats_matrices || !return_risk_stats_matrices_loaded
}

pub(crate) fn should_prewarm_raw_return_risk_matrix(
    return_risk_feature_cache_mode: ReturnRiskFeatureCacheMode,
    has_score_days: bool,
) -> bool {
    has_score_days
        && matches!(
            return_risk_feature_cache_mode,
            ReturnRiskFeatureCacheMode::RawMatrix
        )
}

async fn load_return_risk_stats_feature_matrix_persistent_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: Option<&str>,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
    return_history: &SymbolReturnHistory,
    pairwise_scope: &ReturnRiskStatsPairwiseScopePlan,
) -> Result<Arc<ScoreDateReturnRiskStatsMatrix>, String> {
    let symbols = normalized_symbol_key(symbols);
    let score_days = normalized_dates(&pairwise_scope.score_days);
    if symbols.is_empty() || score_days.is_empty() {
        return Ok(Arc::new(ScoreDateReturnRiskStatsMatrix::default()));
    }

    let persistent_key = data_version_id.map(|data_version_id| {
        persistent_return_risk_stats_feature_matrix_cache_key(
            data_version_id,
            start_date,
            end_date,
            lookback_days,
            &symbols,
            pairwise_scope,
        )
    });

    if let Some(persistent_key) = persistent_key.as_ref() {
        match load_persistent_return_risk_stats_feature_matrix_cache(
            pool,
            persistent_key,
            &symbols,
            &score_days,
        )
        .await
        {
            Ok(Some(matrix)) => {
                cache.record_persistent_market_feature_hit(
                    PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
                );
                cache.record_persistent_return_risk_stats_feature_matrix_payload_loaded(
                    matrix.row_count(),
                    matrix.pair_row_count(),
                );
                return Ok(Arc::new(matrix));
            }
            Ok(None) => {
                cache.record_persistent_market_feature_miss(
                    PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
                );
            }
            Err(error) => {
                warn!(
                    cache_key = persistent_key.cache_key,
                    error = %error,
                    "persistent return/risk stats feature matrix cache read failed; falling back to return history"
                );
            }
        }
    }

    let matrix = build_score_date_return_risk_stats_matrix_with_pairwise_scope(
        return_history,
        &score_days,
        &symbols,
        lookback_days,
        pairwise_scope,
    );

    if let Some(persistent_key) = persistent_key.as_ref() {
        match store_persistent_return_risk_stats_feature_matrix_cache(
            pool,
            persistent_key,
            &symbols,
            &score_days,
            &matrix,
        )
        .await
        {
            Ok(true) => {
                cache.record_persistent_market_feature_write(
                    PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
                );
                cache.record_persistent_return_risk_stats_feature_matrix_payload_written(
                    matrix.row_count(),
                    matrix.pair_row_count(),
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    cache_key = persistent_key.cache_key,
                    error = %error,
                    "persistent return/risk stats feature matrix cache write failed"
                );
            }
        }
    }

    Ok(Arc::new(matrix))
}

pub(crate) async fn load_portfolio_return_risk_stats_feature_matrices_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: Option<&str>,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    score_days: &[NaiveDate],
    config: &PortfolioConstructionConfig,
    return_history: &SymbolReturnHistory,
    scores_by_date: &FactorScoresByDate,
    signal_config: &SignalConfig,
) -> Result<HashMap<usize, Arc<ScoreDateReturnRiskStatsMatrix>>, String> {
    let lookbacks = portfolio_return_risk_matrix_lookback_days(config);
    if lookbacks.is_empty() {
        return Ok(HashMap::new());
    }
    let Some(pairwise_scope) = return_risk_stats_pairwise_scope_for_factor_scores(
        score_days,
        symbols,
        scores_by_date,
        signal_config,
        DEFAULT_RETURN_RISK_STATS_PAIRWISE_ROW_LIMIT,
    ) else {
        return Ok(HashMap::new());
    };
    let mut matrices = HashMap::new();
    for lookback_days in lookbacks {
        let matrix = load_return_risk_stats_feature_matrix_persistent_cached(
            pool,
            cache,
            data_version_id,
            symbols,
            start_date,
            end_date,
            lookback_days,
            return_history,
            &pairwise_scope,
        )
        .await?;
        matrices.insert(lookback_days, matrix);
    }
    Ok(matrices)
}

async fn load_average_amounts(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<String, f64>, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }

    let rows: Vec<(String, Option<Decimal>)> = sqlx::query_as(
        "SELECT symbol, AVG(amount) as avg_amount
         FROM market_stock_daily_bar_adj
         WHERE symbol = ANY($1)
           AND trade_date >= $2 AND trade_date <= $3
           AND amount > 0
         GROUP BY symbol",
    )
    .bind(symbols)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load portfolio capacity data: {}", e))?;

    Ok(rows
        .into_iter()
        .filter_map(|(symbol, amount)| {
            amount
                .and_then(|value| value.to_f64())
                .filter(|value| value.is_finite() && *value > 0.0)
                .map(|value| (symbol, value))
        })
        .collect())
}

pub(crate) async fn load_average_amounts_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Arc<AverageAmounts>, String> {
    let (missing_symbols, mut amounts) =
        cache.cached_average_amount_symbols(symbols, start_date, end_date);
    if missing_symbols.is_empty() {
        return Ok(Arc::new(amounts));
    }
    let loaded_amounts = load_average_amounts(pool, &missing_symbols, start_date, end_date).await?;
    let loaded_amounts =
        cache.insert_average_amount_symbols(&missing_symbols, start_date, end_date, loaded_amounts);
    amounts.extend(loaded_amounts);
    Ok(Arc::new(amounts))
}

async fn load_average_amount_history(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<AverageAmountHistory, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }

    let query_start = average_amount_history_query_start(start_date, lookback_days);
    let rows: Vec<(String, NaiveDate, Option<Decimal>)> = sqlx::query_as(
        "SELECT symbol, trade_date, amount
         FROM market_stock_daily_bar_adj
         WHERE symbol = ANY($1)
           AND trade_date >= $2 AND trade_date <= $3
           AND amount > 0
         ORDER BY symbol, trade_date",
    )
    .bind(symbols)
    .bind(query_start)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load PIT portfolio capacity history: {}", e))?;

    let mut history = HashMap::new();
    for (symbol, trade_date, amount) in rows {
        let Some(amount) = amount
            .and_then(|value| value.to_f64())
            .filter(|value| value.is_finite() && *value > 0.0)
        else {
            continue;
        };
        history
            .entry(symbol)
            .or_insert_with(Vec::new)
            .push((trade_date, amount));
    }
    Ok(history)
}

async fn load_average_amount_history_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<AverageAmountHistory>, String> {
    let (missing_symbols, mut history) =
        cache.cached_average_amount_history_symbols(symbols, start_date, end_date, lookback_days);
    if missing_symbols.is_empty() {
        return Ok(Arc::new(history));
    }
    let loaded_history =
        load_average_amount_history(pool, &missing_symbols, start_date, end_date, lookback_days)
            .await?;
    let loaded_history = cache.insert_average_amount_history_symbols(
        &missing_symbols,
        start_date,
        end_date,
        lookback_days,
        loaded_history,
    );
    history.extend(loaded_history);
    Ok(Arc::new(history))
}

pub(crate) async fn load_average_amount_history_persistent_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: &str,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<AverageAmountHistory>, String> {
    let (missing_symbols, mut history) =
        cache.cached_average_amount_history_symbols(symbols, start_date, end_date, lookback_days);
    if missing_symbols.is_empty() {
        return Ok(Arc::new(history));
    }

    let persistent_key = PersistentMarketFeatureCacheKey::new(
        PersistentMarketFeatureKind::AverageAmountHistory,
        data_version_id,
        start_date,
        end_date,
        lookback_days,
        &missing_symbols,
    );
    match load_persistent_market_feature_cache(pool, &persistent_key, &missing_symbols).await {
        Ok(Some(persistent_history)) => {
            cache.record_persistent_market_feature_hit(
                PersistentMarketFeatureKind::AverageAmountHistory,
            );
            let persistent_history = cache.insert_average_amount_history_symbols(
                &missing_symbols,
                start_date,
                end_date,
                lookback_days,
                persistent_history,
            );
            history.extend(persistent_history);
            return Ok(Arc::new(history));
        }
        Ok(None) => {
            cache.record_persistent_market_feature_miss(
                PersistentMarketFeatureKind::AverageAmountHistory,
            );
        }
        Err(error) => {
            warn!(
                cache_key = persistent_key.cache_key,
                error = %error,
                "persistent average amount history cache read failed; falling back to source table"
            );
        }
    }

    let loaded_history =
        load_average_amount_history(pool, &missing_symbols, start_date, end_date, lookback_days)
            .await?;
    match store_persistent_market_feature_cache(
        pool,
        &persistent_key,
        &missing_symbols,
        &loaded_history,
    )
    .await
    {
        Ok(true) => {
            cache.record_persistent_market_feature_write(
                PersistentMarketFeatureKind::AverageAmountHistory,
            );
        }
        Ok(false) => {}
        Err(error) => {
            warn!(
                cache_key = persistent_key.cache_key,
                error = %error,
                "persistent average amount history cache write failed"
            );
        }
    }
    let loaded_history = cache.insert_average_amount_history_symbols(
        &missing_symbols,
        start_date,
        end_date,
        lookback_days,
        loaded_history,
    );
    history.extend(loaded_history);
    Ok(Arc::new(history))
}

pub(crate) async fn load_pit_average_amount_matrix_persistent_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: Option<&str>,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    score_days: &[NaiveDate],
    lookback_days: usize,
) -> Result<Arc<AverageAmountsByDate>, String> {
    let symbols = normalized_symbol_key(symbols);
    let score_days = normalized_dates(score_days);
    if symbols.is_empty() || score_days.is_empty() {
        return Ok(Arc::new(HashMap::new()));
    }

    let matrix_key = PitAverageAmountMatrixCacheKey::new(
        start_date,
        end_date,
        lookback_days,
        &symbols,
        &score_days,
    );
    if let Some(matrix) = cache.cached_pit_average_amount_matrix(&matrix_key) {
        return Ok(matrix);
    }

    let Some(data_version_id) = data_version_id else {
        let history = load_average_amount_history_cached(
            pool,
            cache,
            &symbols,
            start_date,
            end_date,
            lookback_days,
        )
        .await?;
        let matrix =
            build_pit_average_amounts_by_date(history.as_ref(), &score_days, lookback_days);
        return Ok(cache.insert_pit_average_amount_matrix(matrix_key, matrix));
    };

    let persistent_key = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::PitAverageAmountMatrix,
        data_version_id,
        start_date,
        end_date,
        lookback_days,
        &symbols,
        &score_days,
    );
    match load_persistent_market_feature_cache(pool, &persistent_key, &symbols).await {
        Ok(Some(persistent_history)) => {
            cache.record_persistent_market_feature_hit(
                PersistentMarketFeatureKind::PitAverageAmountMatrix,
            );
            let matrix = average_amount_symbol_history_to_matrix(&persistent_history, &score_days);
            return Ok(cache.insert_pit_average_amount_matrix(matrix_key, matrix));
        }
        Ok(None) => {
            cache.record_persistent_market_feature_miss(
                PersistentMarketFeatureKind::PitAverageAmountMatrix,
            );
        }
        Err(error) => {
            warn!(
                cache_key = persistent_key.cache_key,
                error = %error,
                "persistent PIT average amount matrix cache read failed; falling back to source table"
            );
        }
    }

    let history =
        load_average_amount_history(pool, &symbols, start_date, end_date, lookback_days).await?;
    let matrix = build_pit_average_amounts_by_date(&history, &score_days, lookback_days);
    let matrix_history = pit_average_amount_matrix_to_symbol_history(&matrix);
    match store_persistent_market_feature_cache(pool, &persistent_key, &symbols, &matrix_history)
        .await
    {
        Ok(true) => {
            cache.record_persistent_market_feature_write(
                PersistentMarketFeatureKind::PitAverageAmountMatrix,
            );
        }
        Ok(false) => {}
        Err(error) => {
            warn!(
                cache_key = persistent_key.cache_key,
                error = %error,
                "persistent PIT average amount matrix cache write failed"
            );
        }
    }
    Ok(cache.insert_pit_average_amount_matrix(matrix_key, matrix))
}

async fn load_industry_classifications(
    pool: &PgPool,
    symbols: &[String],
) -> Result<HashMap<String, String>, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT symbol, industry
         FROM market_stock
         WHERE symbol = ANY($1)
           AND industry IS NOT NULL
           AND trim(industry) <> ''",
    )
    .bind(symbols)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load industry classifications: {}", e))?;

    Ok(rows
        .into_iter()
        .map(|(symbol, industry)| (symbol, industry.trim().to_string()))
        .collect())
}

async fn load_industry_classifications_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
) -> Result<Arc<IndustryMap>, String> {
    let key = SignalDataCacheKey::industry_classifications(symbols);
    if let Some(industries) = cache.cached_industry_classifications(&key) {
        return Ok(industries);
    }
    let industries = load_industry_classifications(pool, symbols).await?;
    Ok(cache.insert_industry_classifications(key, industries))
}

pub(crate) async fn load_portfolio_capacity_inputs(
    pool: &PgPool,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    trading_days: &[NaiveDate],
    config: &PortfolioConstructionConfig,
) -> Result<AverageAmountsByDate, String> {
    if config.uses_capacity_inputs() {
        let history = load_average_amount_history(
            pool,
            symbols,
            start_date,
            end_date,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        )
        .await?;
        Ok(build_pit_average_amounts_by_date(
            &history,
            trading_days,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        ))
    } else {
        Ok(HashMap::new())
    }
}

pub(crate) async fn load_portfolio_capacity_inputs_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    data_version_id: Option<&str>,
    symbols: &[String],
    start_date: NaiveDate,
    end_date: NaiveDate,
    trading_days: &[NaiveDate],
    config: &PortfolioConstructionConfig,
) -> Result<Arc<AverageAmountsByDate>, String> {
    if config.uses_capacity_inputs() {
        load_pit_average_amount_matrix_persistent_cached(
            pool,
            cache,
            data_version_id,
            symbols,
            start_date,
            end_date,
            trading_days,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
        )
        .await
    } else {
        Ok(Arc::new(HashMap::new()))
    }
}

pub(crate) async fn load_portfolio_industry_inputs(
    pool: &PgPool,
    symbols: &[String],
    config: &PortfolioConstructionConfig,
) -> Result<HashMap<String, String>, String> {
    if config.max_industry_weight_pct.is_none() {
        Ok(HashMap::new())
    } else {
        load_industry_classifications(pool, symbols).await
    }
}

pub(crate) async fn load_portfolio_industry_inputs_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    symbols: &[String],
    config: &PortfolioConstructionConfig,
) -> Result<Arc<IndustryMap>, String> {
    if config.max_industry_weight_pct.is_none() {
        Ok(Arc::new(HashMap::new()))
    } else {
        load_industry_classifications_cached(pool, cache, symbols).await
    }
}

async fn load_benchmark_return_history(
    pool: &PgPool,
    benchmark: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Vec<(NaiveDate, f64)>, String> {
    let query_start = start_date - Duration::days((lookback_days as i64).saturating_mul(3));
    let rows: Vec<(NaiveDate, Decimal, Option<Decimal>)> = sqlx::query_as(
        "SELECT trade_date, close, pre_close
         FROM market_index_daily_bar
         WHERE symbol = $1
           AND trade_date >= $2 AND trade_date <= $3
           AND close IS NOT NULL AND close > 0
         ORDER BY trade_date",
    )
    .bind(benchmark)
    .bind(query_start)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load benchmark return history: {}", e))?;

    let mut returns = Vec::with_capacity(rows.len());
    let mut previous_close: Option<f64> = None;
    for (date, close, pre_close) in rows {
        let Some(close) = close
            .to_f64()
            .filter(|value| value.is_finite() && *value > 0.0)
        else {
            continue;
        };
        let base = pre_close
            .and_then(|value| value.to_f64())
            .or(previous_close);
        if let Some(base) = base.filter(|value| value.is_finite() && *value > 0.0) {
            let daily_return = close / base - 1.0;
            if daily_return.is_finite() {
                returns.push((date, daily_return));
            }
        }
        previous_close = Some(close);
    }

    Ok(returns)
}

pub(crate) async fn load_benchmark_return_history_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    benchmark: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
) -> Result<Arc<BenchmarkReturns>, String> {
    let key = SignalDataCacheKey::benchmark_returns(benchmark, start_date, end_date, lookback_days);
    if let Some(returns) = cache.cached_benchmark_returns(&key) {
        return Ok(returns);
    }
    let returns =
        load_benchmark_return_history(pool, benchmark, start_date, end_date, lookback_days).await?;
    Ok(cache.insert_benchmark_returns(key, returns))
}

fn preloaded_return_risk_matrix(
    preloaded_return_risk_matrices: Option<&HashMap<usize, Arc<ScoreDateReturnRiskMatrix>>>,
    lookback_days: usize,
) -> Option<Arc<ScoreDateReturnRiskMatrix>> {
    preloaded_return_risk_matrices
        .and_then(|matrices| matrices.get(&lookback_days.max(1)))
        .cloned()
}

fn preloaded_return_risk_stats_matrix_for_lookback(
    return_risk_stats_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskStatsMatrix>>,
    lookback_days: usize,
) -> Option<Arc<ScoreDateReturnRiskStatsMatrix>> {
    return_risk_stats_matrices
        .get(&lookback_days.max(1))
        .cloned()
}

pub(crate) fn return_risk_stats_matrices_cover_required_lookbacks(
    return_risk_stats_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskStatsMatrix>>,
    config: &PortfolioConstructionConfig,
) -> bool {
    portfolio_return_risk_matrix_lookback_days(config)
        .into_iter()
        .all(|lookback_days| return_risk_stats_matrices.contains_key(&lookback_days.max(1)))
}

pub(crate) fn return_risk_matrices_cover_required_lookbacks(
    return_risk_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskMatrix>>,
    config: &PortfolioConstructionConfig,
) -> bool {
    portfolio_return_risk_matrix_lookback_days(config)
        .into_iter()
        .all(|lookback_days| return_risk_matrices.contains_key(&lookback_days.max(1)))
}

pub(crate) fn build_portfolio_weights(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    config: &PortfolioConstructionConfig,
) -> HashMap<String, Decimal> {
    build_portfolio_weights_with_return_risk_matrices(
        score_day,
        candidates,
        return_history,
        average_amounts,
        industry_by_symbol,
        config,
        None,
    )
}

pub(crate) fn build_portfolio_weights_with_return_risk_matrices(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    config: &PortfolioConstructionConfig,
    preloaded_return_risk_matrices: Option<&HashMap<usize, Arc<ScoreDateReturnRiskMatrix>>>,
) -> HashMap<String, Decimal> {
    let candidate_symbols = candidates
        .iter()
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let uses_risk_matrix =
        candidate_ranking_uses_relative_strength(config.candidate_ranking_profile)
            || config.candidate_risk_filter_profile.params().is_some()
            || config.style_risk_budget_profile.params().is_some()
            || config.risk_contribution_control_profile.params().is_some()
            || matches!(
                config.portfolio_method,
                PortfolioConstructionMethod::RiskBudget
                    | PortfolioConstructionMethod::StressFillAwareRiskBudget
                    | PortfolioConstructionMethod::MinVariance
            );
    let uses_correlation_matrix = config.max_pairwise_correlation.is_some();
    let uses_kelly_matrix = matches!(
        config.portfolio_method,
        PortfolioConstructionMethod::Heuristic
    ) && config.kelly_fraction > 0.0;

    let risk_matrix = uses_risk_matrix.then(|| {
        preloaded_return_risk_matrix(
            preloaded_return_risk_matrices,
            config.risk_budget_lookback_days,
        )
        .unwrap_or_else(|| {
            Arc::new(build_score_date_return_risk_matrix(
                return_history,
                &[score_day],
                &candidate_symbols,
                config.risk_budget_lookback_days,
            ))
        })
    });
    let correlation_matrix = (uses_correlation_matrix
        && (config.correlation_lookback_days != config.risk_budget_lookback_days
            || risk_matrix.is_none()))
    .then(|| {
        preloaded_return_risk_matrix(
            preloaded_return_risk_matrices,
            config.correlation_lookback_days,
        )
        .unwrap_or_else(|| {
            Arc::new(build_score_date_return_risk_matrix(
                return_history,
                &[score_day],
                &candidate_symbols,
                config.correlation_lookback_days,
            ))
        })
    });
    let kelly_matrix = (uses_kelly_matrix
        && (config.kelly_lookback_days != config.risk_budget_lookback_days
            || risk_matrix.is_none())
        && (config.kelly_lookback_days != config.correlation_lookback_days
            || correlation_matrix.is_none()))
    .then(|| {
        preloaded_return_risk_matrix(preloaded_return_risk_matrices, config.kelly_lookback_days)
            .unwrap_or_else(|| {
                Arc::new(build_score_date_return_risk_matrix(
                    return_history,
                    &[score_day],
                    &candidate_symbols,
                    config.kelly_lookback_days,
                ))
            })
    });

    let ranked_candidates = risk_matrix
        .as_ref()
        .map(|matrix| {
            rank_candidates_for_capacity_from_matrix(
                score_day,
                candidates,
                matrix,
                average_amounts,
                config.candidate_ranking_profile,
                return_history,
                config.risk_budget_lookback_days,
            )
        })
        .unwrap_or_else(|| {
            rank_candidates_for_capacity(
                score_day,
                candidates,
                return_history,
                average_amounts,
                config.candidate_ranking_profile,
                config.risk_budget_lookback_days,
            )
        });
    let risk_filtered_candidates = risk_matrix
        .as_ref()
        .map(|matrix| {
            filter_candidate_risk_pool(
                score_day,
                &ranked_candidates,
                matrix,
                average_amounts,
                config,
            )
        })
        .unwrap_or_else(|| {
            let view = super::matrix_view::ReturnHistoryMatrixView::new(
                return_history,
                config.risk_budget_lookback_days,
            );
            filter_candidate_risk_pool(
                score_day,
                &ranked_candidates,
                &view,
                average_amounts,
                config,
            )
        });
    let correlation_matrix_ref = correlation_matrix.as_ref().or_else(|| {
        (config.correlation_lookback_days == config.risk_budget_lookback_days)
            .then(|| risk_matrix.as_ref())
            .flatten()
    });
    let selection_limit =
        cash_utilization_selection_limit(&risk_filtered_candidates, average_amounts, config);
    let selected = correlation_matrix_ref
        .map(|matrix| {
            select_uncorrelated_candidates(
                score_day,
                &risk_filtered_candidates,
                matrix,
                config,
                selection_limit,
            )
        })
        .unwrap_or_else(|| {
            let view = super::matrix_view::ReturnHistoryMatrixView::new(
                return_history,
                config.correlation_lookback_days,
            );
            select_uncorrelated_candidates(
                score_day,
                &risk_filtered_candidates,
                &view,
                config,
                selection_limit,
            )
        });
    if selected.is_empty() {
        return HashMap::new();
    }

    let kelly_matrix_ref = kelly_matrix.as_ref().or_else(|| {
        (config.kelly_lookback_days == config.risk_budget_lookback_days)
            .then(|| risk_matrix.as_ref())
            .flatten()
            .or_else(|| {
                (config.kelly_lookback_days == config.correlation_lookback_days)
                    .then(|| correlation_matrix.as_ref())
                    .flatten()
            })
    });
    let raw_weights = match config.portfolio_method {
        PortfolioConstructionMethod::Heuristic => {
            if config.kelly_fraction > 0.0 {
                if let Some(matrix) = kelly_matrix_ref {
                    build_kelly_raw_weights(score_day, &selected, matrix, config)
                } else {
                    let view = super::matrix_view::ReturnHistoryMatrixView::new(
                        return_history,
                        config.kelly_lookback_days,
                    );
                    build_kelly_raw_weights(score_day, &selected, &view, config)
                }
            } else {
                vec![1.0; selected.len()]
            }
        }
        PortfolioConstructionMethod::RiskBudget => risk_matrix
            .as_ref()
            .map(|matrix| {
                build_risk_budget_raw_weights(
                    score_day,
                    &selected,
                    matrix,
                    average_amounts,
                    config,
                )
            })
            .unwrap_or_else(|| {
                let view = super::matrix_view::ReturnHistoryMatrixView::new(
                    return_history,
                    config.risk_budget_lookback_days,
                );
                build_risk_budget_raw_weights(
                    score_day,
                    &selected,
                    &view,
                    average_amounts,
                    config,
                )
            }),
        PortfolioConstructionMethod::StressFillAwareRiskBudget => risk_matrix
            .as_ref()
            .map(|matrix| {
                build_stress_fill_aware_risk_budget_raw_weights(
                    score_day,
                    &selected,
                    &risk_filtered_candidates,
                    matrix,
                    average_amounts,
                    config,
                )
            })
            .unwrap_or_else(|| {
                let view = super::matrix_view::ReturnHistoryMatrixView::new(
                    return_history,
                    config.risk_budget_lookback_days,
                );
                build_stress_fill_aware_risk_budget_raw_weights(
                    score_day,
                    &selected,
                    &risk_filtered_candidates,
                    &view,
                    average_amounts,
                    config,
                )
            }),
        PortfolioConstructionMethod::MinVariance => risk_matrix
            .as_ref()
            .map(|matrix| {
                build_min_variance_raw_weights(
                    score_day,
                    &selected,
                    matrix,
                    average_amounts,
                    config,
                )
            })
            .unwrap_or_else(|| {
                let view = super::matrix_view::ReturnHistoryMatrixView::new(
                    return_history,
                    config.risk_budget_lookback_days,
                );
                build_min_variance_raw_weights(
                    score_day,
                    &selected,
                    &view,
                    average_amounts,
                    config,
                )
            }),
        PortfolioConstructionMethod::RiskParity => risk_matrix
            .as_ref()
            .map(|matrix| {
                build_risk_parity_raw_weights(
                    score_day,
                    &selected,
                    matrix,
                    average_amounts,
                    config,
                )
            })
            .unwrap_or_else(|| {
                let view = super::matrix_view::ReturnHistoryMatrixView::new(
                    return_history,
                    config.risk_budget_lookback_days,
                );
                build_risk_parity_raw_weights(
                    score_day,
                    &selected,
                    &view,
                    average_amounts,
                    config,
                )
            }),
        PortfolioConstructionMethod::MaxDiversification => risk_matrix
            .as_ref()
            .map(|matrix| {
                build_max_diversification_raw_weights(
                    score_day,
                    &selected,
                    matrix,
                    average_amounts,
                    config,
                )
            })
            .unwrap_or_else(|| {
                let view = super::matrix_view::ReturnHistoryMatrixView::new(
                    return_history,
                    config.risk_budget_lookback_days,
                );
                build_max_diversification_raw_weights(
                    score_day,
                    &selected,
                    &view,
                    average_amounts,
                    config,
                )
            }),
    };

    let mut weights = normalize_and_cap_weights(&selected, &raw_weights, average_amounts, config);
    apply_capacity_risk_budget(&mut weights, average_amounts, config);
    if let Some(matrix) = risk_matrix.as_ref() {
        apply_style_risk_budget(
            &mut weights,
            matrix,
            average_amounts,
            score_day,
            config,
        );
    } else {
        let view = super::matrix_view::ReturnHistoryMatrixView::new(
            return_history,
            config.risk_budget_lookback_days,
        );
        apply_style_risk_budget(
            &mut weights,
            &view,
            average_amounts,
            score_day,
            config,
        );
    }
    apply_industry_cap(&mut weights, industry_by_symbol, config);
    if let Some(matrix) = risk_matrix.as_ref() {
        apply_risk_contribution_control(&mut weights, matrix, score_day, config);
    } else {
        let view = super::matrix_view::ReturnHistoryMatrixView::new(
            return_history,
            config.risk_budget_lookback_days,
        );
        apply_risk_contribution_control(&mut weights, &view, score_day, config);
    }
    weights
}

#[allow(dead_code)]
pub(crate) fn build_portfolio_weights_with_return_risk_stats_matrices(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_risk_stats_matrices: &HashMap<usize, Arc<ScoreDateReturnRiskStatsMatrix>>,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    config: &PortfolioConstructionConfig,
) -> HashMap<String, Decimal> {
    let uses_risk_matrix =
        candidate_ranking_uses_relative_strength(config.candidate_ranking_profile)
            || config.candidate_risk_filter_profile.params().is_some()
            || config.style_risk_budget_profile.params().is_some()
            || config.risk_contribution_control_profile.params().is_some()
            || matches!(
                config.portfolio_method,
                PortfolioConstructionMethod::RiskBudget
                    | PortfolioConstructionMethod::StressFillAwareRiskBudget
                    | PortfolioConstructionMethod::MinVariance
            );
    let uses_correlation_matrix = config.max_pairwise_correlation.is_some();
    let uses_kelly_matrix = matches!(
        config.portfolio_method,
        PortfolioConstructionMethod::Heuristic
    ) && config.kelly_fraction > 0.0;

    let risk_matrix = if uses_risk_matrix {
        let Some(matrix) = preloaded_return_risk_stats_matrix_for_lookback(
            return_risk_stats_matrices,
            config.risk_budget_lookback_days,
        ) else {
            return HashMap::new();
        };
        Some(matrix)
    } else {
        None
    };
    let correlation_matrix = if uses_correlation_matrix
        && (config.correlation_lookback_days != config.risk_budget_lookback_days
            || risk_matrix.is_none())
    {
        let Some(matrix) = preloaded_return_risk_stats_matrix_for_lookback(
            return_risk_stats_matrices,
            config.correlation_lookback_days,
        ) else {
            return HashMap::new();
        };
        Some(matrix)
    } else {
        None
    };
    let kelly_matrix = if uses_kelly_matrix
        && (config.kelly_lookback_days != config.risk_budget_lookback_days || risk_matrix.is_none())
        && (config.kelly_lookback_days != config.correlation_lookback_days
            || correlation_matrix.is_none())
    {
        let Some(matrix) = preloaded_return_risk_stats_matrix_for_lookback(
            return_risk_stats_matrices,
            config.kelly_lookback_days,
        ) else {
            return HashMap::new();
        };
        Some(matrix)
    } else {
        None
    };

    let ranked_candidates = if let Some(matrix) = risk_matrix.as_deref() {
        rank_candidates_for_capacity_from_stats_matrix(
            score_day,
            candidates,
            matrix,
            average_amounts,
            config.candidate_ranking_profile,
        )
    } else {
        candidates.to_vec()
    };
    let risk_filtered_candidates = if let Some(matrix) = risk_matrix.as_deref() {
        filter_candidate_risk_pool(
            score_day,
            &ranked_candidates,
            matrix,
            average_amounts,
            config,
        )
    } else {
        ranked_candidates
    };
    let selection_limit =
        cash_utilization_selection_limit(&risk_filtered_candidates, average_amounts, config);
    let correlation_matrix_ref = correlation_matrix.as_ref().or_else(|| {
        (config.correlation_lookback_days == config.risk_budget_lookback_days)
            .then(|| risk_matrix.as_ref())
            .flatten()
    });
    let selected = if let Some(matrix) = correlation_matrix_ref {
        select_uncorrelated_candidates(
            score_day,
            &risk_filtered_candidates,
            matrix,
            config,
            selection_limit,
        )
    } else {
        risk_filtered_candidates
            .iter()
            .take(selection_limit)
            .map(|(symbol, _)| symbol.clone())
            .collect()
    };
    if selected.is_empty() {
        return HashMap::new();
    }

    let kelly_matrix_ref = kelly_matrix.as_ref().or_else(|| {
        (config.kelly_lookback_days == config.risk_budget_lookback_days)
            .then(|| risk_matrix.as_ref())
            .flatten()
            .or_else(|| {
                (config.kelly_lookback_days == config.correlation_lookback_days)
                    .then(|| correlation_matrix.as_ref())
                    .flatten()
            })
    });
    let raw_weights = match config.portfolio_method {
        PortfolioConstructionMethod::Heuristic => {
            if config.kelly_fraction > 0.0 {
                let Some(matrix) = kelly_matrix_ref else {
                    return HashMap::new();
                };
                build_kelly_raw_weights(score_day, &selected, matrix, config)
            } else {
                vec![1.0; selected.len()]
            }
        }
        PortfolioConstructionMethod::RiskBudget => {
            let Some(matrix) = risk_matrix.as_deref() else {
                return HashMap::new();
            };
            build_risk_budget_raw_weights(
                score_day,
                &selected,
                matrix,
                average_amounts,
                config,
            )
        }
        PortfolioConstructionMethod::StressFillAwareRiskBudget => {
            let Some(matrix) = risk_matrix.as_deref() else {
                return HashMap::new();
            };
            build_stress_fill_aware_risk_budget_raw_weights(
                score_day,
                &selected,
                &risk_filtered_candidates,
                matrix,
                average_amounts,
                config,
            )
        }
        PortfolioConstructionMethod::MinVariance => {
            let Some(matrix) = risk_matrix.as_deref() else {
                return HashMap::new();
            };
            build_min_variance_raw_weights(
                score_day,
                &selected,
                matrix,
                average_amounts,
                config,
            )
        }
        PortfolioConstructionMethod::RiskParity => {
            let Some(matrix) = risk_matrix.as_deref() else {
                return HashMap::new();
            };
            build_risk_parity_raw_weights(
                score_day,
                &selected,
                matrix,
                average_amounts,
                config,
            )
        }
        PortfolioConstructionMethod::MaxDiversification => {
            let Some(matrix) = risk_matrix.as_deref() else {
                return HashMap::new();
            };
            build_max_diversification_raw_weights(
                score_day,
                &selected,
                matrix,
                average_amounts,
                config,
            )
        }
    };

    let mut weights = normalize_and_cap_weights(&selected, &raw_weights, average_amounts, config);
    apply_capacity_risk_budget(&mut weights, average_amounts, config);
    if let Some(matrix) = risk_matrix.as_deref() {
        apply_style_risk_budget(
            &mut weights,
            matrix,
            average_amounts,
            score_day,
            config,
        );
    }
    apply_industry_cap(&mut weights, industry_by_symbol, config);
    if let Some(matrix) = risk_matrix.as_deref() {
        apply_risk_contribution_control(&mut weights, matrix, score_day, config);
    }
    weights
}

#[allow(dead_code)]
pub(crate) fn build_portfolio_weights_with_return_risk_stats_matrix(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    matrix: &ScoreDateReturnRiskStatsMatrix,
    average_amounts: &HashMap<String, f64>,
    industry_by_symbol: &HashMap<String, String>,
    config: &PortfolioConstructionConfig,
) -> HashMap<String, Decimal> {
    let lookback_days = config.risk_budget_lookback_days.max(1);
    let matrices = HashMap::from([(lookback_days, Arc::new(matrix.clone()))]);
    build_portfolio_weights_with_return_risk_stats_matrices(
        score_day,
        candidates,
        &matrices,
        average_amounts,
        industry_by_symbol,
        config,
    )
}

fn rank_candidates_for_capacity(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    average_amounts: &HashMap<String, f64>,
    profile: CandidateRankingProfile,
    lookback_days: usize,
) -> Vec<(String, f64)> {
    let Some(params) = profile.params() else {
        return candidates.to_vec();
    };
    if candidates.len() <= 1 {
        return candidates.to_vec();
    }

    let amount_ranks = liquidity_rank_scores(candidates, average_amounts);
    let relative_strength_ranks = {
        let view = super::matrix_view::ReturnHistoryMatrixView::new(return_history, lookback_days);
        relative_strength_rank_scores(candidates, &view, score_day)
    };
    let volatility_ranks =
        volatility_rank_scores(candidates, return_history, score_day, lookback_days);
    let denominator = candidates.len().saturating_sub(1).max(1) as f64;
    let (alpha_weight, liquidity_weight, relative_strength_weight, volatility_weight) = if params
        .use_regime_aware_weights
    {
        let regime = detect_market_regime_from_returns(return_history, score_day, lookback_days);
        regime_adjusted_weights(params, regime)
    } else {
        (
            params.alpha_rank_weight.max(0.0),
            params.liquidity_rank_weight.max(0.0),
            params.relative_strength_rank_weight.max(0.0),
            params.volatility_rank_weight.max(0.0),
        )
    };
    let weight_sum =
        (alpha_weight + liquidity_weight + relative_strength_weight + volatility_weight)
            .max(f64::EPSILON);
    let mut ranked = candidates
        .iter()
        .enumerate()
        .map(|(idx, (symbol, score))| {
            let alpha_rank = 1.0 - (idx as f64 / denominator);
            let liquidity_rank = amount_ranks.get(symbol).copied().unwrap_or(0.5);
            let relative_strength_rank =
                relative_strength_ranks.get(symbol).copied().unwrap_or(0.5);
            let volatility_rank = volatility_ranks.get(symbol).copied().unwrap_or(0.5);
            let blended_rank = (alpha_weight * alpha_rank
                + liquidity_weight * liquidity_rank
                + relative_strength_weight * relative_strength_rank
                + volatility_weight * volatility_rank)
                / weight_sum;
            (idx, symbol.clone(), *score, blended_rank)
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .3
            .partial_cmp(&left.3)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .map(|(_, symbol, score, _)| (symbol, score))
        .collect()
}

fn rank_candidates_for_capacity_from_matrix(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    matrix: &ScoreDateReturnRiskMatrix,
    average_amounts: &HashMap<String, f64>,
    profile: CandidateRankingProfile,
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    lookback_days: usize,
) -> Vec<(String, f64)> {
    let Some(params) = profile.params() else {
        return candidates.to_vec();
    };
    if candidates.len() <= 1 {
        return candidates.to_vec();
    }

    let amount_ranks = liquidity_rank_scores(candidates, average_amounts);
    let relative_strength_ranks =
        relative_strength_rank_scores(candidates, matrix, score_day);
    let volatility_ranks =
        volatility_rank_scores(candidates, return_history, score_day, lookback_days);
    let denominator = candidates.len().saturating_sub(1).max(1) as f64;
    let (alpha_weight, liquidity_weight, relative_strength_weight, volatility_weight) = if params
        .use_regime_aware_weights
    {
        let regime = detect_market_regime_from_returns(return_history, score_day, lookback_days);
        regime_adjusted_weights(params, regime)
    } else {
        (
            params.alpha_rank_weight.max(0.0),
            params.liquidity_rank_weight.max(0.0),
            params.relative_strength_rank_weight.max(0.0),
            params.volatility_rank_weight.max(0.0),
        )
    };
    let weight_sum =
        (alpha_weight + liquidity_weight + relative_strength_weight + volatility_weight)
            .max(f64::EPSILON);
    let mut ranked = candidates
        .iter()
        .enumerate()
        .map(|(idx, (symbol, score))| {
            let alpha_rank = 1.0 - (idx as f64 / denominator);
            let liquidity_rank = amount_ranks.get(symbol).copied().unwrap_or(0.5);
            let relative_strength_rank =
                relative_strength_ranks.get(symbol).copied().unwrap_or(0.5);
            let volatility_rank = volatility_ranks.get(symbol).copied().unwrap_or(0.5);
            let blended_rank = (alpha_weight * alpha_rank
                + liquidity_weight * liquidity_rank
                + relative_strength_weight * relative_strength_rank
                + volatility_weight * volatility_rank)
                / weight_sum;
            (idx, symbol.clone(), *score, blended_rank)
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .3
            .partial_cmp(&left.3)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .map(|(_, symbol, score, _)| (symbol, score))
        .collect()
}

#[allow(dead_code)]
fn rank_candidates_for_capacity_from_stats_matrix(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    matrix: &ScoreDateReturnRiskStatsMatrix,
    average_amounts: &HashMap<String, f64>,
    profile: CandidateRankingProfile,
) -> Vec<(String, f64)> {
    let Some(params) = profile.params() else {
        return candidates.to_vec();
    };
    if candidates.len() <= 1 {
        return candidates.to_vec();
    }

    let amount_ranks = liquidity_rank_scores(candidates, average_amounts);
    let relative_strength_ranks =
        relative_strength_rank_scores(candidates, matrix, score_day);
    let denominator = candidates.len().saturating_sub(1).max(1) as f64;
    let alpha_weight = params.alpha_rank_weight.max(0.0);
    let liquidity_weight = params.liquidity_rank_weight.max(0.0);
    let relative_strength_weight = params.relative_strength_rank_weight.max(0.0);
    let weight_sum = (alpha_weight + liquidity_weight + relative_strength_weight).max(f64::EPSILON);
    let mut ranked = candidates
        .iter()
        .enumerate()
        .map(|(idx, (symbol, score))| {
            let alpha_rank = 1.0 - (idx as f64 / denominator);
            let liquidity_rank = amount_ranks.get(symbol).copied().unwrap_or(0.5);
            let relative_strength_rank =
                relative_strength_ranks.get(symbol).copied().unwrap_or(0.5);
            let blended_rank = (alpha_weight * alpha_rank
                + liquidity_weight * liquidity_rank
                + relative_strength_weight * relative_strength_rank)
                / weight_sum;
            (idx, symbol.clone(), *score, blended_rank)
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .3
            .partial_cmp(&left.3)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked
        .into_iter()
        .map(|(_, symbol, score, _)| (symbol, score))
        .collect()
}

fn candidate_ranking_uses_relative_strength(profile: CandidateRankingProfile) -> bool {
    profile
        .params()
        .map(|params| params.relative_strength_rank_weight > 0.0)
        .unwrap_or(false)
}

// R9: 三联体合并为单一泛型函数。原 relative_strength_rank_scores /
// _from_matrix / _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
// base 版的 lookback_days 由调用方在构造 ReturnHistoryMatrixView 时冻结。
pub(crate) fn relative_strength_rank_scores<M: super::matrix_view::MatrixView>(
    candidates: &[(String, f64)],
    matrix: &M,
    score_day: NaiveDate,
) -> HashMap<String, f64> {
    let mut ranked_returns = candidates
        .iter()
        .enumerate()
        .filter_map(|(idx, (symbol, _))| {
            matrix
                .total_return(score_day, symbol)
                .map(|total_return| (idx, symbol.clone(), total_return))
        })
        .collect::<Vec<_>>();
    if ranked_returns.is_empty() {
        return HashMap::new();
    }
    ranked_returns.sort_by(|left, right| {
        right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    let denominator = ranked_returns.len().saturating_sub(1).max(1) as f64;
    ranked_returns
        .into_iter()
        .enumerate()
        .map(|(rank, (_, symbol, _))| (symbol, 1.0 - (rank as f64 / denominator)))
        .collect()
}

pub(crate) fn trailing_total_return(returns: &[f64]) -> Option<f64> {
    let mut seen = false;
    let total_return = returns
        .iter()
        .copied()
        .filter(|value| value.is_finite() && *value > -1.0)
        .fold(1.0, |acc, value| {
            seen = true;
            acc * (1.0 + value)
        })
        - 1.0;
    seen.then_some(total_return)
}

fn liquidity_rank_scores(
    candidates: &[(String, f64)],
    average_amounts: &HashMap<String, f64>,
) -> HashMap<String, f64> {
    let mut ranked_amounts = candidates
        .iter()
        .enumerate()
        .filter_map(|(idx, (symbol, _))| {
            average_amounts
                .get(symbol)
                .copied()
                .filter(|amount| amount.is_finite() && *amount > 0.0)
                .map(|amount| (idx, symbol.clone(), amount))
        })
        .collect::<Vec<_>>();
    ranked_amounts.sort_by(|left, right| {
        right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    let denominator = ranked_amounts.len().saturating_sub(1).max(1) as f64;
    ranked_amounts
        .into_iter()
        .enumerate()
        .map(|(rank, (_, symbol, _))| (symbol, 1.0 - (rank as f64 / denominator)))
        .collect()
}

/// Compute inverse-volatility rank scores: lower trailing volatility → higher rank.
/// Uses the trailing volatility computation that is PIT-safe (only data at or before score_day).
fn volatility_rank_scores(
    candidates: &[(String, f64)],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    score_day: NaiveDate,
    lookback_days: usize,
) -> HashMap<String, f64> {
    let mut symbol_vols: Vec<(String, f64)> = candidates
        .iter()
        .filter_map(|(symbol, _)| {
            let closes = return_history.get(symbol)?;
            let vol = trailing_volatility_from_closes(closes, score_day, lookback_days as i64)?;
            Some((symbol.clone(), vol))
        })
        .collect();
    // Sort by volatility ascending (lower vol = better = higher rank)
    symbol_vols.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    let denominator = symbol_vols.len().saturating_sub(1).max(1) as f64;
    symbol_vols
        .into_iter()
        .enumerate()
        .map(|(rank, (symbol, _))| (symbol, 1.0 - (rank as f64 / denominator)))
        .collect()
}

/// Simplified PIT trailing annualized volatility from closes data.
fn trailing_volatility_from_closes(
    closes: &[(NaiveDate, f64)],
    trade_date: NaiveDate,
    lookback_days: i64,
) -> Option<f64> {
    let current_idx = closes
        .iter()
        .position(|(date, close)| *date == trade_date && close.is_finite() && *close > 0.0)?;
    let start_idx = current_idx.checked_sub(lookback_days as usize)?;
    let window = &closes[start_idx..=current_idx];
    let mut daily_returns = Vec::new();
    for pair in window.windows(2) {
        let prev = pair[0].1;
        let curr = pair[1].1;
        if !prev.is_finite() || !curr.is_finite() || prev <= 0.0 || curr <= 0.0 {
            continue;
        }
        daily_returns.push((curr / prev) - 1.0);
    }
    if daily_returns.len() < 20 {
        return None;
    }
    let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
    let variance = daily_returns
        .iter()
        .map(|r| (r - mean) * (r - mean))
        .sum::<f64>()
        / (daily_returns.len() - 1) as f64;
    Some(variance.sqrt() * (252_f64).sqrt())
}

/// Simple market regime detection from benchmark-like composite of candidate returns.
fn detect_market_regime_from_returns(
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    score_day: NaiveDate,
    lookback_days: usize,
) -> MarketRegime {
    // Use the average trailing return across all symbols as a proxy for market regime
    let mut total_return: f64 = 0.0;
    let mut count: usize = 0;
    for (_symbol, closes) in return_history {
        if let Some(current_idx) = closes
            .iter()
            .position(|(date, close)| *date == score_day && close.is_finite() && *close > 0.0)
        {
            if let Some(start_idx) = current_idx.checked_sub(lookback_days) {
                if let (Some((_, current_close)), Some((_, past_close))) =
                    (closes.get(current_idx), closes.get(start_idx))
                {
                    if *past_close > 0.0 {
                        total_return += (current_close / past_close) - 1.0;
                        count += 1;
                    }
                }
            }
        }
    }
    if count == 0 {
        return MarketRegime::Sideways;
    }
    let avg_return = total_return / count as f64;
    if avg_return > 0.10 {
        MarketRegime::Bull
    } else if avg_return < -0.10 {
        MarketRegime::Bear
    } else {
        MarketRegime::Sideways
    }
}

/// Adjust candidate ranking weights based on detected market regime.
/// In bear/high_volatility markets: favor low volatility and liquidity over alpha.
/// In bull markets: allow more alpha weight.
/// In sideways/mixed markets: balanced approach.
fn regime_adjusted_weights(
    params: CandidateRankingParams,
    regime: MarketRegime,
) -> (f64, f64, f64, f64) {
    let (alpha_adj, liq_adj, rs_adj, vol_adj) = match regime {
        MarketRegime::Bull => (1.25, 0.85, 1.10, 0.80),
        MarketRegime::Bear | MarketRegime::HighVolatility => (0.65, 1.30, 0.80, 1.40),
        MarketRegime::Sideways | MarketRegime::Mixed => (1.0, 1.0, 1.0, 1.0),
    };
    (
        (params.alpha_rank_weight * alpha_adj).max(0.0),
        (params.liquidity_rank_weight * liq_adj).max(0.0),
        (params.relative_strength_rank_weight * rs_adj).max(0.0),
        (params.volatility_rank_weight * vol_adj).max(0.0),
    )
}

// R9: 三联体合并为单一泛型函数。原 select_uncorrelated_candidates /
// _from_matrix / _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
// base 版的 correlation_lookback_days 由调用方在构造 ReturnHistoryMatrixView 时冻结。
pub(crate) fn select_uncorrelated_candidates<M: super::matrix_view::MatrixView>(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    matrix: &M,
    config: &PortfolioConstructionConfig,
    selection_limit: usize,
) -> Vec<String> {
    let mut selected: Vec<String> = Vec::new();
    let selection_limit = selection_limit.max(config.top_n).min(candidates.len());
    for (symbol, _) in candidates {
        if selected.len() >= selection_limit {
            break;
        }
        if let Some(limit) = config.max_pairwise_correlation {
            let too_correlated = selected.iter().any(|selected_symbol| {
                matrix
                    .pearson_correlation(score_day, symbol, selected_symbol)
                    .map(|corr| corr.abs() > limit)
                    .unwrap_or(false)
            });
            if too_correlated {
                continue;
            }
        }
        selected.push(symbol.clone());
    }
    selected
}

fn cash_utilization_selection_limit(
    candidates: &[(String, f64)],
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> usize {
    let base_limit = config.top_n.min(candidates.len());
    let Some(params) = config.cash_utilization_profile.params() else {
        return base_limit;
    };
    if candidates.is_empty() || average_amounts.is_empty() {
        return base_limit;
    }

    let target_gross = config
        .max_gross_exposure
        .clamp(0.0, params.min_gross_exposure_pct.clamp(0.0, 1.0));
    if target_gross <= f64::EPSILON {
        return base_limit;
    }

    let hard_limit = params.max_holdings.max(config.top_n).min(candidates.len());
    let max_position_cap = config
        .max_position_pct
        .to_f64()
        .unwrap_or(1.0)
        .clamp(0.0, 1.0);
    let mut cumulative_cap = 0.0;
    let mut count = 0usize;
    for (symbol, _) in candidates.iter().take(hard_limit) {
        let participation_cap_multiplier = config
            .capacity_risk_budget_profile
            .params()
            .map(|params| params.participation_cap_multiplier)
            .unwrap_or(1.0);
        let symbol_cap = participation_weight_cap_with_multiplier(
            symbol,
            average_amounts,
            config,
            participation_cap_multiplier,
        )
        .and_then(|value| value.to_f64())
        .unwrap_or(max_position_cap)
        .min(max_position_cap)
        .clamp(0.0, 1.0);
        cumulative_cap += symbol_cap;
        count += 1;
        if count >= config.top_n && cumulative_cap >= target_gross {
            return count;
        }
    }

    hard_limit
}

// R9: 三联体合并为单一泛型函数。原 filter_candidate_risk_pool /
// _from_matrix / _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
// base 版的 risk_budget_lookback_days 由调用方在构造 ReturnHistoryMatrixView 时冻结。
pub(crate) fn filter_candidate_risk_pool<M: super::matrix_view::MatrixView>(
    score_day: NaiveDate,
    candidates: &[(String, f64)],
    matrix: &M,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Vec<(String, f64)> {
    let Some(params) = config.candidate_risk_filter_profile.params() else {
        return candidates.to_vec();
    };
    if candidates.len() <= config.top_n || candidates.is_empty() {
        return candidates.to_vec();
    }

    let volatility_scores = candidates
        .iter()
        .filter_map(|(symbol, _)| {
            matrix
                .sample_volatility(score_day, symbol)
                .map(|volatility| (symbol.as_str(), volatility))
        })
        .collect::<Vec<_>>();

    let Some(volatility_threshold) = quantile_value(
        volatility_scores.iter().map(|(_, volatility)| *volatility),
        params.max_volatility_quantile,
    ) else {
        return candidates.to_vec();
    };

    let low_volatility_symbols = volatility_scores
        .iter()
        .filter(|(_, volatility)| *volatility <= volatility_threshold)
        .map(|(symbol, _)| *symbol)
        .collect::<HashSet<_>>();

    let mut filtered = candidates
        .iter()
        .filter(|(symbol, _)| {
            if volatility_scores
                .iter()
                .any(|(known, _)| *known == symbol.as_str())
            {
                low_volatility_symbols.contains(symbol.as_str())
            } else {
                true
            }
        })
        .cloned()
        .collect::<Vec<_>>();

    if let Some(max_average_corr) = params.max_average_abs_correlation {
        let reference_limit = params
            .correlation_reference_limit
            .max(config.top_n.saturating_mul(4))
            .max(20);
        let reference_symbols = filtered
            .iter()
            .take(reference_limit)
            .map(|(symbol, _)| symbol.clone())
            .collect::<Vec<_>>();
        filtered.retain(|(symbol, _)| {
            matrix
                .average_abs_correlation_to_reference(score_day, symbol, &reference_symbols)
                .map(|corr| corr <= max_average_corr)
                .unwrap_or(true)
        });
    }

    if let Some(min_liquidity_quantile) = params.min_liquidity_quantile {
        if let Some(liquidity_threshold) = quantile_value(
            filtered
                .iter()
                .filter_map(|(symbol, _)| average_amounts.get(symbol).copied()),
            min_liquidity_quantile,
        ) {
            let liquidity_filtered = filtered
                .iter()
                .filter(|(symbol, _)| {
                    average_amounts
                        .get(symbol)
                        .map(|amount| *amount >= liquidity_threshold)
                        .unwrap_or(false)
                })
                .cloned()
                .collect::<Vec<_>>();
            if liquidity_filtered.len() >= config.top_n {
                filtered = liquidity_filtered;
            }
        }
    }

    if filtered.len() >= config.top_n {
        filtered
    } else {
        candidates.to_vec()
    }
}

fn quantile_value(values: impl Iterator<Item = f64>, quantile: f64) -> Option<f64> {
    let mut values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((values.len().saturating_sub(1) as f64) * quantile.clamp(0.0, 1.0)).floor() as usize;
    values.get(idx.min(values.len() - 1)).copied()
}

pub(crate) fn average_abs_correlation_to_reference(
    symbol: &str,
    reference_symbols: &[String],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    score_day: NaiveDate,
    lookback_days: usize,
) -> Option<f64> {
    let own_returns = trailing_returns(return_history, symbol, score_day, lookback_days);
    if own_returns.len() < 3 {
        return None;
    }
    let correlations = reference_symbols
        .iter()
        .filter(|other| other.as_str() != symbol)
        .filter_map(|other| {
            let other_returns =
                trailing_returns(return_history, other.as_str(), score_day, lookback_days);
            pearson_correlation(&own_returns, &other_returns).map(f64::abs)
        })
        .collect::<Vec<_>>();
    if correlations.is_empty() {
        None
    } else {
        Some(correlations.iter().sum::<f64>() / correlations.len() as f64)
    }
}

// R9: 三联体合并为单一泛型函数。原 build_kelly_raw_weights / _from_matrix /
// _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
pub(crate) fn build_kelly_raw_weights<M: super::matrix_view::MatrixView>(
    score_day: NaiveDate,
    symbols: &[String],
    matrix: &M,
    config: &PortfolioConstructionConfig,
) -> Vec<f64> {
    let mut raw_weights = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let kelly = matrix
            .fractional_kelly_weight(score_day, symbol, config.kelly_fraction)
            .unwrap_or(0.0);
        raw_weights.push(kelly.max(0.0));
    }
    if raw_weights.iter().all(|weight| *weight <= 0.0) {
        vec![1.0; symbols.len()]
    } else {
        raw_weights
    }
}

// R9: 三联体合并为单一泛型函数。原 build_risk_budget_raw_weights /
// _from_matrix / _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
// base 版的 risk_budget_lookback_days 由调用方在构造 ReturnHistoryMatrixView 时冻结。
pub(crate) fn build_risk_budget_raw_weights<M: super::matrix_view::MatrixView>(
    score_day: NaiveDate,
    symbols: &[String],
    matrix: &M,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Vec<f64> {
    let max_amount = symbols
        .iter()
        .filter_map(|symbol| average_amounts.get(symbol).copied())
        .filter(|amount| amount.is_finite() && *amount > 0.0)
        .fold(0.0_f64, f64::max);

    let mut raw_weights = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let volatility = matrix.sample_volatility(score_day, symbol).unwrap_or(0.20);
        let concentration_penalty =
            matrix.covariance_concentration_penalty(score_day, symbol, symbols);
        let capacity_score = capacity_score(symbol, average_amounts, max_amount);
        let capacity_multiplier = capacity_score.powf(config.capacity_penalty_strength.max(0.0));
        let risk_denominator = volatility.max(0.01) * concentration_penalty.max(1.0);
        let raw = capacity_multiplier / risk_denominator;
        raw_weights.push(if raw.is_finite() { raw.max(0.0) } else { 0.0 });
    }

    if raw_weights.iter().all(|weight| *weight <= 0.0) {
        vec![1.0; symbols.len()]
    } else {
        raw_weights
    }
}

fn alpha_rank_lookup(candidates: &[(String, f64)]) -> HashMap<String, f64> {
    if candidates.is_empty() {
        return HashMap::new();
    }
    let denominator = candidates.len().saturating_sub(1).max(1) as f64;
    candidates
        .iter()
        .enumerate()
        .map(|(idx, (symbol, _))| (symbol.clone(), 1.0 - (idx as f64 / denominator)))
        .collect()
}

fn stress_fill_alpha_multiplier(symbol: &str, alpha_ranks: &HashMap<String, f64>) -> f64 {
    alpha_ranks
        .get(symbol)
        .copied()
        .unwrap_or(0.5)
        .clamp(0.0, 1.0)
        .max(0.05)
        .powf(1.25)
}

fn stress_fill_confidence_lookup(candidates: &[(String, f64)]) -> HashMap<String, f64> {
    stress_fill_confidence_lookup_for_direction(candidates, ScoreDirection::Descending)
}

pub(crate) fn stress_fill_confidence_lookup_for_direction(
    candidates: &[(String, f64)],
    score_direction: ScoreDirection,
) -> HashMap<String, f64> {
    let stats = score_stats(candidates.iter().map(|(_, score)| *score));
    let finite = candidates
        .iter()
        .filter_map(|(symbol, score)| {
            score.is_finite().then(|| {
                let z_score = standard_score(*score, stats);
                let z_score = match score_direction {
                    ScoreDirection::Descending => z_score,
                    ScoreDirection::Ascending => -z_score,
                };
                (symbol.clone(), z_score)
            })
        })
        .collect::<Vec<_>>();
    if finite.is_empty() {
        return HashMap::new();
    }

    finite
        .into_iter()
        .map(|(symbol, z_score)| (symbol, (0.5 + z_score / 6.0).clamp(0.0, 1.0)))
        .collect()
}

fn stress_fill_confidence_lookup_for_config(
    candidates: &[(String, f64)],
    config: &PortfolioConstructionConfig,
) -> HashMap<String, f64> {
    match config.stress_fill_confidence_exposure_profile {
        StressFillConfidenceExposureProfile::Off
        | StressFillConfidenceExposureProfile::PredictionConfidenceV1
        | StressFillConfidenceExposureProfile::PredictionConfidenceCapacityHeadroomV1 => {
            stress_fill_confidence_lookup(candidates)
        }
        StressFillConfidenceExposureProfile::PredictionConfidenceAscendingV1
        | StressFillConfidenceExposureProfile::PredictionConfidenceAscendingCapacityHeadroomV1 => {
            stress_fill_confidence_lookup_for_direction(candidates, ScoreDirection::Ascending)
        }
    }
}

fn stress_fill_capacity_headroom_multiplier(
    symbol: &str,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> f64 {
    let max_position = config
        .max_position_pct
        .to_f64()
        .unwrap_or(1.0)
        .clamp(0.0, 1.0);
    if max_position <= f64::EPSILON {
        return 1.0;
    }
    let participation_cap_multiplier = config
        .capacity_risk_budget_profile
        .params()
        .map(|params| params.participation_cap_multiplier)
        .unwrap_or(1.0);
    let symbol_cap = target_weight_cap_with_multiplier(
        symbol,
        average_amounts,
        config,
        participation_cap_multiplier,
    )
    .to_f64()
    .unwrap_or(max_position)
    .clamp(0.0, max_position);
    (symbol_cap / max_position).clamp(0.05, 1.0).powf(0.75)
}

fn stress_fill_confidence_multiplier(
    symbol: &str,
    confidence_scores: &HashMap<String, f64>,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> f64 {
    match config.stress_fill_confidence_exposure_profile {
        StressFillConfidenceExposureProfile::Off => 1.0,
        StressFillConfidenceExposureProfile::PredictionConfidenceV1
        | StressFillConfidenceExposureProfile::PredictionConfidenceAscendingV1 => confidence_scores
            .get(symbol)
            .copied()
            .unwrap_or(0.5)
            .clamp(0.0, 1.0)
            .max(0.05)
            .powf(1.50),
        StressFillConfidenceExposureProfile::PredictionConfidenceCapacityHeadroomV1
        | StressFillConfidenceExposureProfile::PredictionConfidenceAscendingCapacityHeadroomV1 => {
            let confidence = confidence_scores
                .get(symbol)
                .copied()
                .unwrap_or(0.5)
                .clamp(0.0, 1.0)
                .max(0.05)
                .powf(1.50);
            confidence * stress_fill_capacity_headroom_multiplier(symbol, average_amounts, config)
        }
    }
}

// R9: 三联体合并为单一泛型函数。原 build_stress_fill_aware_risk_budget_raw_weights /
// _from_matrix / _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
// base 版的 risk_budget_lookback_days 由调用方在构造 ReturnHistoryMatrixView 时冻结。
pub(crate) fn build_stress_fill_aware_risk_budget_raw_weights<
    M: super::matrix_view::MatrixView,
>(
    score_day: NaiveDate,
    symbols: &[String],
    ranked_candidates: &[(String, f64)],
    matrix: &M,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Vec<f64> {
    let max_amount = symbols
        .iter()
        .filter_map(|symbol| average_amounts.get(symbol).copied())
        .filter(|amount| amount.is_finite() && *amount > 0.0)
        .fold(0.0_f64, f64::max);
    let alpha_ranks = alpha_rank_lookup(ranked_candidates);
    let confidence_scores = stress_fill_confidence_lookup_for_config(ranked_candidates, config);

    let mut raw_weights = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let volatility = matrix.sample_volatility(score_day, symbol).unwrap_or(0.20);
        let concentration_penalty =
            matrix.covariance_concentration_penalty(score_day, symbol, symbols);
        let capacity_score = capacity_score(symbol, average_amounts, max_amount);
        let capacity_multiplier = capacity_score.powf(config.capacity_penalty_strength.max(0.0));
        let alpha_multiplier = stress_fill_alpha_multiplier(symbol, &alpha_ranks);
        let confidence_multiplier =
            stress_fill_confidence_multiplier(symbol, &confidence_scores, average_amounts, config);
        let risk_denominator = volatility.max(0.01) * concentration_penalty.max(1.0);
        let raw = alpha_multiplier * confidence_multiplier * capacity_multiplier / risk_denominator;
        raw_weights.push(if raw.is_finite() { raw.max(0.0) } else { 0.0 });
    }

    if raw_weights.iter().all(|weight| *weight <= 0.0) {
        vec![1.0; symbols.len()]
    } else {
        raw_weights
    }
}

// R9: 三联体合并为单一泛型函数。原 build_min_variance_raw_weights /
// _from_matrix / _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
// base 版的 risk_budget_lookback_days 由调用方在构造 ReturnHistoryMatrixView 时冻结。
pub(crate) fn build_min_variance_raw_weights<M: super::matrix_view::MatrixView>(
    score_day: NaiveDate,
    symbols: &[String],
    matrix: &M,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Vec<f64> {
    let max_amount = symbols
        .iter()
        .filter_map(|symbol| average_amounts.get(symbol).copied())
        .filter(|amount| amount.is_finite() && *amount > 0.0)
        .fold(0.0_f64, f64::max);

    let mut raw_weights = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let volatility = matrix
            .sample_volatility(score_day, symbol)
            .unwrap_or(0.20)
            .max(0.01);
        let concentration_penalty =
            matrix.covariance_concentration_penalty(score_day, symbol, symbols);
        let capacity_score = capacity_score(symbol, average_amounts, max_amount);
        let capacity_multiplier = capacity_score.powf(config.capacity_penalty_strength.max(0.0));
        let variance = volatility * volatility;
        let covariance_penalty = concentration_penalty.max(1.0).powi(2);
        let raw = capacity_multiplier / (variance.max(0.0001) * covariance_penalty);
        raw_weights.push(if raw.is_finite() { raw.max(0.0) } else { 0.0 });
    }

    if raw_weights.iter().all(|weight| *weight <= 0.0) {
        vec![1.0; symbols.len()]
    } else {
        raw_weights
    }
}

/// 风险平价（RiskParity）：权重 ∝ 1/σ，使各资产风险贡献均等。
///
/// 与 RiskBudget 区别：RiskBudget 含协方差集中度惩罚（cov_penalty），RiskParity 是
/// 经典 1/σ（无集中度惩罚，纯风险贡献均等）。适合低相关分散组合。
pub(crate) fn build_risk_parity_raw_weights<M: super::matrix_view::MatrixView>(
    score_day: NaiveDate,
    symbols: &[String],
    matrix: &M,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Vec<f64> {
    let max_amount = symbols
        .iter()
        .filter_map(|symbol| average_amounts.get(symbol).copied())
        .filter(|amount| amount.is_finite() && *amount > 0.0)
        .fold(0.0_f64, f64::max);

    let mut raw_weights = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let volatility = matrix.sample_volatility(score_day, symbol).unwrap_or(0.20).max(0.01);
        let capacity_score = capacity_score(symbol, average_amounts, max_amount);
        let capacity_multiplier = capacity_score.powf(config.capacity_penalty_strength.max(0.0));
        // 经典风险平价：权重 ∝ 1/σ（风险贡献均等）
        let raw = capacity_multiplier / volatility;
        raw_weights.push(if raw.is_finite() { raw.max(0.0) } else { 0.0 });
    }

    if raw_weights.iter().all(|weight| *weight <= 0.0) {
        vec![1.0; symbols.len()]
    } else {
        raw_weights
    }
}

/// 最大分散化（MaxDiversification）：权重 ∝ σ * (1 - avg_abs_corr)，
/// 高波动且与组合低相关的资产优先，最大化组合分散化比率。
///
/// 分散化比率 = 组合加权波动 / 加权平均波动，MaxDiv 优化目标即最大化此比率。
pub(crate) fn build_max_diversification_raw_weights<M: super::matrix_view::MatrixView>(
    score_day: NaiveDate,
    symbols: &[String],
    matrix: &M,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Vec<f64> {
    let max_amount = symbols
        .iter()
        .filter_map(|symbol| average_amounts.get(symbol).copied())
        .filter(|amount| amount.is_finite() && *amount > 0.0)
        .fold(0.0_f64, f64::max);

    let mut raw_weights = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let volatility = matrix.sample_volatility(score_day, symbol).unwrap_or(0.20).max(0.01);
        // 平均绝对相关（相对组合内其他资产）：高相关降低分散化价值
        let avg_abs_corr = matrix
            .average_abs_correlation_to_reference(score_day, symbol, symbols)
            .unwrap_or(0.0)
            .clamp(0.0, 0.99);
        let capacity_score = capacity_score(symbol, average_amounts, max_amount);
        let capacity_multiplier = capacity_score.powf(config.capacity_penalty_strength.max(0.0));
        // 分散化权重：高波动（σ）+ 低相关（1-corr）优先
        let diversification_score = volatility * (1.0 - avg_abs_corr);
        let raw = capacity_multiplier * diversification_score;
        raw_weights.push(if raw.is_finite() { raw.max(0.0) } else { 0.0 });
    }

    if raw_weights.iter().all(|weight| *weight <= 0.0) {
        vec![1.0; symbols.len()]
    } else {
        raw_weights
    }
}

fn normalize_and_cap_weights(
    symbols: &[String],
    raw_weights: &[f64],
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> HashMap<String, Decimal> {
    let positive_sum: f64 = raw_weights
        .iter()
        .copied()
        .filter(|weight| weight.is_finite() && *weight > 0.0)
        .sum();
    if positive_sum <= 0.0 {
        return HashMap::new();
    }

    let gross = config.max_gross_exposure.clamp(0.0, 1.0);
    let mut target_weights = HashMap::new();
    for (symbol, raw_weight) in symbols.iter().zip(raw_weights.iter()) {
        if !raw_weight.is_finite() || *raw_weight <= 0.0 {
            continue;
        }
        let normalized = (*raw_weight / positive_sum * gross).max(0.0);
        let symbol_cap = participation_weight_cap(symbol, average_amounts, config)
            .unwrap_or(config.max_position_pct);
        let weight = Decimal::from_f64(normalized)
            .unwrap_or(Decimal::zero())
            .min(config.max_position_pct)
            .min(symbol_cap);
        if !weight.is_zero() {
            target_weights.insert(symbol.clone(), weight);
        }
    }
    target_weights
}

fn participation_weight_cap(
    symbol: &str,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) -> Option<Decimal> {
    participation_weight_cap_with_multiplier(symbol, average_amounts, config, 1.0)
}

fn participation_weight_cap_with_multiplier(
    symbol: &str,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
    multiplier: f64,
) -> Option<Decimal> {
    let participation_rate = config.max_participation_rate?;
    let notional = config.portfolio_notional_cny?;
    let multiplier = multiplier.clamp(0.0, 1.0);
    if !participation_rate.is_finite()
        || !notional.is_finite()
        || !multiplier.is_finite()
        || participation_rate <= 0.0
        || notional <= 0.0
    {
        return None;
    }
    let average_amount = average_amounts.get(symbol).copied()?;
    if !average_amount.is_finite() || average_amount <= 0.0 {
        return None;
    }
    let cap = (average_amount * participation_rate * multiplier / notional).clamp(0.0, 1.0);
    Decimal::from_f64(cap)
}

fn target_weight_cap_with_multiplier(
    symbol: &str,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
    multiplier: f64,
) -> Decimal {
    participation_weight_cap_with_multiplier(symbol, average_amounts, config, multiplier)
        .unwrap_or(config.max_position_pct)
        .min(config.max_position_pct)
        .max(Decimal::ZERO)
}

fn apply_capacity_risk_budget(
    weights: &mut HashMap<String, Decimal>,
    average_amounts: &HashMap<String, f64>,
    config: &PortfolioConstructionConfig,
) {
    let Some(params) = config.capacity_risk_budget_profile.params() else {
        return;
    };
    if weights.is_empty() {
        return;
    }

    let liquidity_scores = weights
        .keys()
        .filter_map(|symbol| {
            average_amounts
                .get(symbol)
                .copied()
                .filter(|amount| amount.is_finite() && *amount > 0.0)
                .map(|amount| (symbol.clone(), amount))
        })
        .collect::<Vec<_>>();
    if liquidity_scores.is_empty() {
        return;
    }

    let mut low_capacity_symbols =
        low_style_bucket_symbols(&liquidity_scores, params.low_capacity_quantile);
    for symbol in weights.keys() {
        let has_valid_amount = average_amounts
            .get(symbol)
            .copied()
            .map(|amount| amount.is_finite() && amount > 0.0)
            .unwrap_or(false);
        if !has_valid_amount {
            low_capacity_symbols.insert(symbol.clone());
        }
    }

    let caps = weights
        .keys()
        .map(|symbol| {
            (
                symbol.clone(),
                target_weight_cap_with_multiplier(
                    symbol,
                    average_amounts,
                    config,
                    params.participation_cap_multiplier,
                ),
            )
        })
        .collect::<HashMap<_, _>>();
    cap_weights_to_symbol_caps(weights, &caps, 8);

    let low_capacity_cap = Decimal::from_f64(params.low_capacity_max_weight_pct.clamp(0.0, 1.0))
        .unwrap_or(Decimal::ONE);
    cap_bucket_and_redistribute(weights, &low_capacity_symbols, low_capacity_cap, &caps, 8);

    if let Some(min_target_gross_exposure_pct) = params.min_target_gross_exposure_pct {
        let target_gross = Decimal::from_f64(
            config
                .max_gross_exposure
                .clamp(0.0, 1.0)
                .min(min_target_gross_exposure_pct.clamp(0.0, 1.0)),
        )
        .unwrap_or(Decimal::ZERO);
        let current_gross = weights.values().copied().sum::<Decimal>();
        if target_gross > current_gross {
            let floor_caps = weights
                .keys()
                .map(|symbol| {
                    (
                        symbol.clone(),
                        target_weight_cap_with_multiplier(
                            symbol,
                            average_amounts,
                            config,
                            params.floor_refill_cap_multiplier,
                        ),
                    )
                })
                .collect::<HashMap<_, _>>();
            match params.floor_refill_mode {
                CapacityFloorRefillMode::ExistingWeight => {
                    redistribute_weight(
                        weights,
                        &low_capacity_symbols,
                        target_gross - current_gross,
                        &floor_caps,
                        8,
                    );
                }
                CapacityFloorRefillMode::Headroom => {
                    redistribute_weight_by_headroom(
                        weights,
                        &low_capacity_symbols,
                        target_gross - current_gross,
                        &floor_caps,
                        8,
                    );
                }
                CapacityFloorRefillMode::AlphaHeadroom => {
                    redistribute_weight_by_alpha_headroom(
                        weights,
                        &low_capacity_symbols,
                        target_gross - current_gross,
                        &floor_caps,
                        8,
                    );
                }
                CapacityFloorRefillMode::BlendedAlphaHeadroom => {
                    redistribute_weight_by_blended_alpha_headroom(
                        weights,
                        &low_capacity_symbols,
                        target_gross - current_gross,
                        &floor_caps,
                        8,
                    );
                }
            }
        }
    }

    if params.refill_gross_exposure {
        let target_gross =
            Decimal::from_f64(config.max_gross_exposure.clamp(0.0, 1.0)).unwrap_or(Decimal::ONE);
        let current_gross = weights.values().copied().sum::<Decimal>();
        if target_gross > current_gross {
            redistribute_weight(
                weights,
                &low_capacity_symbols,
                target_gross - current_gross,
                &caps,
                8,
            );
        }
    }
    weights.retain(|_, weight| *weight > Decimal::ZERO);
}

fn cap_weights_to_symbol_caps(
    weights: &mut HashMap<String, Decimal>,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    if weights.is_empty() || caps.is_empty() {
        return Decimal::ZERO;
    }

    let mut excess = Decimal::ZERO;
    for (symbol, weight) in weights.iter_mut() {
        let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
        if *weight > cap {
            excess += *weight - cap;
            *weight = cap;
        }
    }

    if excess <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    redistribute_weight(weights, &HashSet::new(), excess, caps, iterations)
}

fn cap_bucket_and_redistribute(
    weights: &mut HashMap<String, Decimal>,
    bucket_symbols: &HashSet<String>,
    cap: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) {
    if weights.is_empty() || bucket_symbols.is_empty() || cap >= Decimal::ONE {
        return;
    }

    let bucket_total = weights
        .iter()
        .filter(|(symbol, _)| bucket_symbols.contains(*symbol))
        .map(|(_, weight)| *weight)
        .sum::<Decimal>();
    if bucket_total <= cap || bucket_total.is_zero() {
        return;
    }

    let scale = cap / bucket_total;
    for (symbol, weight) in weights.iter_mut() {
        if bucket_symbols.contains(symbol) {
            *weight *= scale;
        }
    }
    redistribute_weight(
        weights,
        bucket_symbols,
        bucket_total - cap,
        caps,
        iterations,
    );
    weights.retain(|_, weight| *weight > Decimal::ZERO);
}

fn redistribute_weight(
    weights: &mut HashMap<String, Decimal>,
    excluded_symbols: &HashSet<String>,
    amount: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    let mut remaining = amount.max(Decimal::ZERO);
    if remaining.is_zero() {
        return Decimal::ZERO;
    }
    let epsilon = Decimal::new(1, 8);

    for _ in 0..iterations.max(1) {
        let eligible = weights
            .iter()
            .filter_map(|(symbol, weight)| {
                if excluded_symbols.contains(symbol) {
                    return None;
                }
                let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
                let headroom = cap - *weight;
                if headroom > epsilon {
                    Some((symbol.clone(), *weight, headroom))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            break;
        }

        let base_sum = eligible
            .iter()
            .map(|(_, weight, _)| *weight)
            .sum::<Decimal>();
        let equal_share = Decimal::ONE / Decimal::from(eligible.len() as u64);
        let mut allocated = Decimal::ZERO;
        for (symbol, weight, headroom) in eligible {
            let share = if base_sum > Decimal::ZERO {
                weight / base_sum
            } else {
                equal_share
            };
            let addition = (remaining * share).min(headroom);
            if addition <= Decimal::ZERO {
                continue;
            }
            if let Some(target) = weights.get_mut(&symbol) {
                *target += addition;
                allocated += addition;
            }
        }

        if allocated <= epsilon {
            break;
        }
        remaining -= allocated;
        if remaining <= epsilon {
            return Decimal::ZERO;
        }
    }

    remaining
}

pub(crate) fn redistribute_weight_by_headroom(
    weights: &mut HashMap<String, Decimal>,
    excluded_symbols: &HashSet<String>,
    amount: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    let mut remaining = amount.max(Decimal::ZERO);
    if remaining.is_zero() {
        return Decimal::ZERO;
    }
    let epsilon = Decimal::new(1, 8);

    for _ in 0..iterations.max(1) {
        let eligible = weights
            .iter()
            .filter_map(|(symbol, weight)| {
                if excluded_symbols.contains(symbol) {
                    return None;
                }
                let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
                let headroom = cap - *weight;
                if headroom > epsilon {
                    Some((symbol.clone(), headroom))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            break;
        }

        let headroom_sum = eligible
            .iter()
            .map(|(_, headroom)| *headroom)
            .sum::<Decimal>();
        if headroom_sum <= epsilon {
            break;
        }

        let mut allocated = Decimal::ZERO;
        for (symbol, headroom) in eligible {
            let addition = (remaining * headroom / headroom_sum).min(headroom);
            if addition <= Decimal::ZERO {
                continue;
            }
            if let Some(target) = weights.get_mut(&symbol) {
                *target += addition;
                allocated += addition;
            }
        }

        if allocated <= epsilon {
            break;
        }
        remaining -= allocated;
        if remaining <= epsilon {
            return Decimal::ZERO;
        }
    }

    remaining
}

pub(crate) fn redistribute_weight_by_alpha_headroom(
    weights: &mut HashMap<String, Decimal>,
    excluded_symbols: &HashSet<String>,
    amount: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    let mut remaining = amount.max(Decimal::ZERO);
    if remaining.is_zero() {
        return Decimal::ZERO;
    }
    let epsilon = Decimal::new(1, 8);

    for _ in 0..iterations.max(1) {
        let eligible = weights
            .iter()
            .filter_map(|(symbol, weight)| {
                if excluded_symbols.contains(symbol) {
                    return None;
                }
                let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
                let headroom = cap - *weight;
                if headroom > epsilon {
                    let alpha_weight = (*weight).max(epsilon);
                    Some((symbol.clone(), headroom, alpha_weight * headroom))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            break;
        }

        let score_sum = eligible.iter().map(|(_, _, score)| *score).sum::<Decimal>();
        if score_sum <= epsilon {
            break;
        }

        let mut allocated = Decimal::ZERO;
        for (symbol, headroom, score) in eligible {
            let addition = (remaining * score / score_sum).min(headroom);
            if addition <= Decimal::ZERO {
                continue;
            }
            if let Some(target) = weights.get_mut(&symbol) {
                *target += addition;
                allocated += addition;
            }
        }

        if allocated <= epsilon {
            break;
        }
        remaining -= allocated;
        if remaining <= epsilon {
            return Decimal::ZERO;
        }
    }

    remaining
}

pub(crate) fn redistribute_weight_by_blended_alpha_headroom(
    weights: &mut HashMap<String, Decimal>,
    excluded_symbols: &HashSet<String>,
    amount: Decimal,
    caps: &HashMap<String, Decimal>,
    iterations: usize,
) -> Decimal {
    let mut remaining = amount.max(Decimal::ZERO);
    if remaining.is_zero() {
        return Decimal::ZERO;
    }
    let epsilon = Decimal::new(1, 8);

    for _ in 0..iterations.max(1) {
        let eligible = weights
            .iter()
            .filter_map(|(symbol, weight)| {
                if excluded_symbols.contains(symbol) {
                    return None;
                }
                let cap = caps.get(symbol).copied().unwrap_or(Decimal::ONE);
                let headroom = cap - *weight;
                if headroom > epsilon {
                    Some((symbol.clone(), *weight, headroom))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            break;
        }

        let max_weight = eligible
            .iter()
            .map(|(_, weight, _)| *weight)
            .max()
            .unwrap_or(Decimal::ZERO);
        let score_sum = eligible
            .iter()
            .map(|(_, weight, headroom)| {
                let alpha_boost = if max_weight > epsilon {
                    Decimal::ONE + (*weight / max_weight)
                } else {
                    Decimal::ONE
                };
                *headroom * alpha_boost
            })
            .sum::<Decimal>();
        if score_sum <= epsilon {
            break;
        }

        let mut allocated = Decimal::ZERO;
        for (symbol, weight, headroom) in eligible {
            let alpha_boost = if max_weight > epsilon {
                Decimal::ONE + (weight / max_weight)
            } else {
                Decimal::ONE
            };
            let score = headroom * alpha_boost;
            let addition = (remaining * score / score_sum).min(headroom);
            if addition <= Decimal::ZERO {
                continue;
            }
            if let Some(target) = weights.get_mut(&symbol) {
                *target += addition;
                allocated += addition;
            }
        }

        if allocated <= epsilon {
            break;
        }
        remaining -= allocated;
        if remaining <= epsilon {
            return Decimal::ZERO;
        }
    }

    remaining
}

fn apply_industry_cap(
    weights: &mut HashMap<String, Decimal>,
    industry_by_symbol: &HashMap<String, String>,
    config: &PortfolioConstructionConfig,
) {
    let Some(cap) = config.max_industry_weight_pct else {
        return;
    };
    if weights.is_empty() || industry_by_symbol.is_empty() || !cap.is_finite() {
        return;
    }
    let cap = cap.clamp(0.0, 1.0);
    if cap >= 1.0 {
        return;
    }
    let cap = Decimal::from_f64(cap).unwrap_or(Decimal::ONE);

    let mut industry_totals: HashMap<&str, Decimal> = HashMap::new();
    for (symbol, weight) in weights.iter() {
        if let Some(industry) = industry_by_symbol
            .get(symbol)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            *industry_totals.entry(industry).or_default() += *weight;
        }
    }

    let industry_scales: HashMap<&str, Decimal> = industry_totals
        .into_iter()
        .filter_map(|(industry, total)| {
            if total > cap && !total.is_zero() {
                Some((industry, cap / total))
            } else {
                None
            }
        })
        .collect();
    if industry_scales.is_empty() {
        return;
    }

    for (symbol, weight) in weights.iter_mut() {
        if let Some(scale) = industry_by_symbol
            .get(symbol)
            .map(|value| value.trim())
            .and_then(|industry| industry_scales.get(industry))
        {
            *weight *= *scale;
        }
    }
    weights.retain(|_, weight| !weight.is_zero());
}

// R9: 三联体合并为单一泛型函数。原 apply_style_risk_budget /
// _from_matrix / _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
// base 版的 risk_budget_lookback_days 由调用方在构造 ReturnHistoryMatrixView 时冻结。
fn apply_style_risk_budget<M: super::matrix_view::MatrixView>(
    weights: &mut HashMap<String, Decimal>,
    matrix: &M,
    average_amounts: &HashMap<String, f64>,
    score_day: NaiveDate,
    config: &PortfolioConstructionConfig,
) {
    let Some(params) = config.style_risk_budget_profile.params() else {
        return;
    };
    if weights.is_empty() {
        return;
    }

    let volatility_scores = weights
        .keys()
        .filter_map(|symbol| {
            matrix
                .sample_volatility(score_day, symbol)
                .map(|volatility| (symbol.clone(), volatility))
        })
        .collect::<Vec<_>>();
    let high_volatility_symbols =
        high_style_bucket_symbols(&volatility_scores, params.high_volatility_quantile);
    cap_style_bucket(
        weights,
        &high_volatility_symbols,
        params.high_volatility_max_weight_pct,
    );

    let liquidity_scores = weights
        .keys()
        .filter_map(|symbol| {
            average_amounts
                .get(symbol)
                .copied()
                .filter(|amount| amount.is_finite() && *amount > 0.0)
                .map(|amount| (symbol.clone(), amount))
        })
        .collect::<Vec<_>>();
    let low_liquidity_symbols =
        low_style_bucket_symbols(&liquidity_scores, params.low_liquidity_quantile);
    cap_style_bucket(
        weights,
        &low_liquidity_symbols,
        params.low_liquidity_max_weight_pct,
    );
}

fn high_style_bucket_symbols(scores: &[(String, f64)], quantile: f64) -> HashSet<String> {
    if scores.is_empty() {
        return HashSet::new();
    }
    let mut sorted = scores
        .iter()
        .filter(|(_, value)| value.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return HashSet::new();
    }
    sorted.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let threshold_index =
        ((sorted.len().saturating_sub(1) as f64) * quantile.clamp(0.0, 1.0)).ceil() as usize;
    let threshold = sorted[threshold_index.min(sorted.len() - 1)].1;
    sorted
        .into_iter()
        .filter(|(_, value)| *value >= threshold)
        .map(|(symbol, _)| symbol.clone())
        .collect()
}

fn low_style_bucket_symbols(scores: &[(String, f64)], quantile: f64) -> HashSet<String> {
    if scores.is_empty() {
        return HashSet::new();
    }
    let mut sorted = scores
        .iter()
        .filter(|(_, value)| value.is_finite())
        .collect::<Vec<_>>();
    if sorted.is_empty() {
        return HashSet::new();
    }
    sorted.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let threshold_index =
        ((sorted.len().saturating_sub(1) as f64) * quantile.clamp(0.0, 1.0)).floor() as usize;
    let threshold = sorted[threshold_index.min(sorted.len() - 1)].1;
    sorted
        .into_iter()
        .filter(|(_, value)| *value <= threshold)
        .map(|(symbol, _)| symbol.clone())
        .collect()
}

fn cap_style_bucket(
    weights: &mut HashMap<String, Decimal>,
    bucket_symbols: &HashSet<String>,
    max_weight_pct: f64,
) {
    if weights.is_empty() || bucket_symbols.is_empty() || !max_weight_pct.is_finite() {
        return;
    }
    let cap = max_weight_pct.clamp(0.0, 1.0);
    if cap >= 1.0 {
        return;
    }
    let cap = Decimal::from_f64(cap).unwrap_or(Decimal::ONE);
    let total = weights
        .iter()
        .filter(|(symbol, _)| bucket_symbols.contains(*symbol))
        .map(|(_, weight)| *weight)
        .sum::<Decimal>();
    if total <= cap || total.is_zero() {
        return;
    }
    let scale = cap / total;
    for (symbol, weight) in weights.iter_mut() {
        if bucket_symbols.contains(symbol) {
            *weight *= scale;
        }
    }
    weights.retain(|_, weight| !weight.is_zero());
}

// R9: 三联体合并为单一泛型函数。原 apply_risk_contribution_control /
// _from_matrix / _from_stats_matrix 逻辑同构，仅数据源不同，现统一查 MatrixView trait。
// base 版的 risk_budget_lookback_days 由调用方在构造 ReturnHistoryMatrixView 时冻结。
fn apply_risk_contribution_control<M: super::matrix_view::MatrixView>(
    weights: &mut HashMap<String, Decimal>,
    matrix: &M,
    score_day: NaiveDate,
    config: &PortfolioConstructionConfig,
) {
    let Some(params) = config.risk_contribution_control_profile.params() else {
        return;
    };
    if weights.len() < 2 {
        return;
    }

    let cap = params.max_single_name_contribution_pct.clamp(0.01, 1.0);
    if cap >= 1.0 {
        return;
    }

    for _ in 0..params.iterations.max(1) {
        let symbols = weights.keys().cloned().collect::<Vec<_>>();
        let contributions = symbols
            .iter()
            .filter_map(|symbol| {
                let weight = weights.get(symbol)?.to_f64()?;
                if !weight.is_finite() || weight <= 0.0 {
                    return None;
                }
                let volatility = matrix
                    .sample_volatility(score_day, symbol)
                    .unwrap_or(0.20)
                    .max(0.01);
                let concentration_penalty =
                    matrix.covariance_concentration_penalty(score_day, symbol, &symbols);
                let risk_score = weight * volatility * concentration_penalty.max(1.0);
                if risk_score.is_finite() && risk_score > 0.0 {
                    Some((symbol.clone(), risk_score))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let total_risk = contributions.iter().map(|(_, risk)| *risk).sum::<f64>();
        if total_risk <= 0.0 {
            return;
        }

        let mut changed = false;
        for (symbol, risk_score) in contributions {
            let contribution_pct = risk_score / total_risk;
            if contribution_pct <= cap {
                continue;
            }
            if let Some(weight) = weights.get_mut(&symbol) {
                let scale = Decimal::from_f64((cap / contribution_pct).clamp(0.0, 1.0))
                    .unwrap_or(Decimal::ONE);
                *weight *= scale;
                changed = true;
            }
        }
        weights.retain(|_, weight| !weight.is_zero());
        if !changed {
            break;
        }
    }
}

pub(crate) fn sample_volatility(returns: &[f64]) -> Option<f64> {
    let returns = returns
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if returns.len() < 2 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    if variance <= f64::EPSILON {
        None
    } else {
        Some(variance.sqrt())
    }
}

// Phase 7-ER staged API: consumers are switched over after equivalence coverage is complete.
#[allow(dead_code)]
pub(crate) const EMPTY_RETURN_SERIES: [f64; 0] = [];

#[cfg(test)]
thread_local! {
    static SCORE_DATE_RETURN_RISK_MATRIX_READS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
fn record_score_date_return_risk_matrix_read() {
    SCORE_DATE_RETURN_RISK_MATRIX_READS.with(|reads| reads.set(reads.get() + 1));
}

#[cfg(test)]
pub(crate) fn reset_score_date_return_risk_matrix_read_count() {
    SCORE_DATE_RETURN_RISK_MATRIX_READS.with(|reads| reads.set(0));
}

#[cfg(test)]
pub(crate) fn score_date_return_risk_matrix_read_count() -> usize {
    SCORE_DATE_RETURN_RISK_MATRIX_READS.with(|reads| reads.get())
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub(crate) struct ScoreDateReturnRiskMatrix {
    pub(crate) returns_by_score_symbol: HashMap<(NaiveDate, String), Vec<f64>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReturnRiskFeatureMatrixRow {
    pub(crate) score_day: NaiveDate,
    pub(crate) symbol: String,
    pub(crate) returns: Vec<f64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReturnRiskStatsFeatureMatrixRow {
    pub(crate) score_day: NaiveDate,
    pub(crate) symbol: String,
    pub(crate) return_count: usize,
    pub(crate) total_return: Option<f64>,
    pub(crate) sample_volatility: Option<f64>,
    pub(crate) kelly_mean: Option<f64>,
    pub(crate) kelly_population_variance: Option<f64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReturnRiskPairwiseCorrelationRow {
    pub(crate) score_day: NaiveDate,
    pub(crate) left_symbol: String,
    pub(crate) right_symbol: String,
    pub(crate) correlation: f64,
}

pub(crate) const DEFAULT_RETURN_RISK_STATS_PAIRWISE_ROW_LIMIT: usize = 1_000_000;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReturnRiskStatsFeatureMatrixPayloadStatus {
    WithinBudget,
    RequiresSparsePairwiseCache,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReturnRiskStatsFeatureMatrixPayloadProfile {
    pub(crate) stats_rows: usize,
    pub(crate) pair_rows: usize,
    pub(crate) dense_pair_capacity: Option<usize>,
    pub(crate) max_pair_rows: usize,
    pub(crate) pair_rows_per_stats_row: Option<f64>,
    pub(crate) status: ReturnRiskStatsFeatureMatrixPayloadStatus,
}

#[allow(dead_code)]
impl ReturnRiskStatsFeatureMatrixPayloadProfile {
    pub(crate) fn within_budget(&self) -> bool {
        self.status == ReturnRiskStatsFeatureMatrixPayloadStatus::WithinBudget
    }
}

#[allow(dead_code)]
impl ScoreDateReturnRiskMatrix {
    pub(crate) fn row_count(&self) -> usize {
        self.returns_by_score_symbol.len()
    }

    pub(crate) fn return_value_count(&self) -> usize {
        self.returns_by_score_symbol.values().map(Vec::len).sum()
    }

    pub(crate) fn returns(&self, score_day: NaiveDate, symbol: &str) -> &[f64] {
        #[cfg(test)]
        record_score_date_return_risk_matrix_read();

        self.returns_by_score_symbol
            .get(&(score_day, symbol.to_string()))
            .map(Vec::as_slice)
            .unwrap_or(&EMPTY_RETURN_SERIES)
    }

    pub(crate) fn total_return(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        trailing_total_return(self.returns(score_day, symbol))
    }

    pub(crate) fn sample_volatility(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        sample_volatility(self.returns(score_day, symbol))
    }

    pub(crate) fn fractional_kelly_weight(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        fraction: f64,
    ) -> Option<f64> {
        fractional_kelly_weight(self.returns(score_day, symbol), fraction)
    }

    pub(crate) fn pearson_correlation(
        &self,
        score_day: NaiveDate,
        left: &str,
        right: &str,
    ) -> Option<f64> {
        pearson_correlation(
            self.returns(score_day, left),
            self.returns(score_day, right),
        )
    }

    pub(crate) fn average_abs_correlation_to_reference(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        reference_symbols: &[String],
    ) -> Option<f64> {
        let own_returns = self.returns(score_day, symbol);
        if own_returns.len() < 3 {
            return None;
        }
        let correlations = reference_symbols
            .iter()
            .filter(|other| other.as_str() != symbol)
            .filter_map(|other| {
                pearson_correlation(own_returns, self.returns(score_day, other.as_str()))
                    .map(f64::abs)
            })
            .collect::<Vec<_>>();
        if correlations.is_empty() {
            None
        } else {
            Some(correlations.iter().sum::<f64>() / correlations.len() as f64)
        }
    }

    pub(crate) fn covariance_concentration_penalty(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        symbols: &[String],
    ) -> f64 {
        self.average_abs_correlation_to_reference(score_day, symbol, symbols)
            .map(|average_abs_corr| 1.0 + average_abs_corr)
            .unwrap_or(1.0)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub(crate) struct ScoreDateReturnRiskStatsMatrix {
    stats_by_score_symbol: HashMap<(NaiveDate, String), ReturnRiskSingleSymbolStats>,
    pairwise_correlations: HashMap<(NaiveDate, String, String), f64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReturnRiskStatsPairwiseScopePlan {
    pub(crate) score_days: Vec<NaiveDate>,
    pub(crate) symbols: Vec<String>,
    pub(crate) pair_keys: Vec<(NaiveDate, String, String)>,
}

#[allow(dead_code)]
impl ReturnRiskStatsPairwiseScopePlan {
    pub(crate) fn pair_count(&self) -> usize {
        self.pair_keys.len()
    }

    pub(crate) fn contains(&self, score_day: NaiveDate, left: &str, right: &str) -> bool {
        if left == right {
            return false;
        }
        self.pair_keys
            .binary_search(&pairwise_correlation_key(score_day, left, right))
            .is_ok()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReturnRiskSingleSymbolStats {
    return_count: usize,
    total_return: Option<f64>,
    sample_volatility: Option<f64>,
    kelly_mean: Option<f64>,
    kelly_population_variance: Option<f64>,
}

#[allow(dead_code)]
impl ReturnRiskSingleSymbolStats {
    fn from_returns(returns: &[f64]) -> Self {
        let (kelly_mean, kelly_population_variance) = if returns.len() >= 3 {
            let mean = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns
                .iter()
                .map(|value| {
                    let diff = *value - mean;
                    diff * diff
                })
                .sum::<f64>()
                / returns.len() as f64;
            (Some(mean), Some(variance))
        } else {
            (None, None)
        };

        Self {
            return_count: returns.len(),
            total_return: trailing_total_return(returns),
            sample_volatility: sample_volatility(returns),
            kelly_mean,
            kelly_population_variance,
        }
    }

    fn fractional_kelly_weight(&self, fraction: f64) -> Option<f64> {
        if self.return_count < 3 || fraction <= 0.0 {
            return None;
        }
        let mean = self.kelly_mean?;
        let variance = self.kelly_population_variance?;
        if variance <= f64::EPSILON {
            return None;
        }
        Some((mean / variance * fraction).clamp(0.0, 1.0))
    }
}

#[allow(dead_code)]
impl ScoreDateReturnRiskStatsMatrix {
    pub(crate) fn row_count(&self) -> usize {
        self.stats_by_score_symbol.len()
    }

    pub(crate) fn pair_row_count(&self) -> usize {
        self.pairwise_correlations.len()
    }

    pub(crate) fn stats(&self, score_day: NaiveDate, symbol: &str) -> Option<&ReturnRiskSingleSymbolStats> {
        self.stats_by_score_symbol
            .get(&(score_day, symbol.to_string()))
    }

    pub(crate) fn return_count(&self, score_day: NaiveDate, symbol: &str) -> usize {
        self.stats(score_day, symbol)
            .map(|stats| stats.return_count)
            .unwrap_or_default()
    }

    pub(crate) fn total_return(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        self.stats(score_day, symbol)
            .and_then(|stats| stats.total_return)
    }

    pub(crate) fn sample_volatility(&self, score_day: NaiveDate, symbol: &str) -> Option<f64> {
        self.stats(score_day, symbol)
            .and_then(|stats| stats.sample_volatility)
    }

    pub(crate) fn fractional_kelly_weight(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        fraction: f64,
    ) -> Option<f64> {
        self.stats(score_day, symbol)
            .and_then(|stats| stats.fractional_kelly_weight(fraction))
    }

    pub(crate) fn pearson_correlation(&self, score_day: NaiveDate, left: &str, right: &str) -> Option<f64> {
        if left == right {
            return self.stats(score_day, left).and_then(|stats| {
                (stats.return_count >= 3 && stats.sample_volatility.is_some()).then_some(1.0)
            });
        }
        self.pairwise_correlations
            .get(&pairwise_correlation_key(score_day, left, right))
            .copied()
    }

    pub(crate) fn average_abs_correlation_to_reference(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        reference_symbols: &[String],
    ) -> Option<f64> {
        let correlations = reference_symbols
            .iter()
            .filter(|other| other.as_str() != symbol)
            .filter_map(|other| {
                self.pearson_correlation(score_day, symbol, other)
                    .map(f64::abs)
            })
            .collect::<Vec<_>>();
        if correlations.is_empty() {
            None
        } else {
            Some(correlations.iter().sum::<f64>() / correlations.len() as f64)
        }
    }

    pub(crate) fn covariance_concentration_penalty(
        &self,
        score_day: NaiveDate,
        symbol: &str,
        symbols: &[String],
    ) -> f64 {
        self.average_abs_correlation_to_reference(score_day, symbol, symbols)
            .map(|average_abs_corr| 1.0 + average_abs_corr)
            .unwrap_or(1.0)
    }
}

pub(crate) fn pairwise_correlation_key(
    score_day: NaiveDate,
    left: &str,
    right: &str,
) -> (NaiveDate, String, String) {
    if left <= right {
        (score_day, left.to_string(), right.to_string())
    } else {
        (score_day, right.to_string(), left.to_string())
    }
}

#[allow(dead_code)]
pub(crate) fn return_risk_stats_pairwise_scope_from_symbols(
    score_days: &[NaiveDate],
    symbols: &[String],
    pairwise_symbols: &[String],
) -> ReturnRiskStatsPairwiseScopePlan {
    return_risk_stats_pairwise_scope_from_symbol_groups(
        score_days,
        symbols,
        &[pairwise_symbols.to_vec()],
    )
}

#[allow(dead_code)]
pub(crate) fn return_risk_stats_pairwise_scope_from_symbol_groups(
    score_days: &[NaiveDate],
    symbols: &[String],
    candidate_groups: &[Vec<String>],
) -> ReturnRiskStatsPairwiseScopePlan {
    let score_days = normalized_dates(score_days);
    let symbols = normalized_symbol_key(symbols);
    let symbol_scope = symbols.iter().cloned().collect::<HashSet<_>>();
    let mut pair_keys = BTreeSet::new();

    for score_day in &score_days {
        for group in candidate_groups {
            let group_symbols = normalized_symbol_key(group)
                .into_iter()
                .filter(|symbol| symbol_scope.contains(symbol))
                .collect::<Vec<_>>();
            for left_idx in 0..group_symbols.len() {
                for right_idx in (left_idx + 1)..group_symbols.len() {
                    pair_keys.insert(pairwise_correlation_key(
                        *score_day,
                        &group_symbols[left_idx],
                        &group_symbols[right_idx],
                    ));
                }
            }
        }
    }

    ReturnRiskStatsPairwiseScopePlan {
        score_days,
        symbols,
        pair_keys: pair_keys.into_iter().collect(),
    }
}

#[allow(dead_code)]
pub(crate) fn return_risk_stats_pairwise_scope_for_portfolio_candidate_pool(
    score_day: NaiveDate,
    candidate_symbols: &[String],
    symbols: &[String],
) -> ReturnRiskStatsPairwiseScopePlan {
    return_risk_stats_pairwise_scope_from_symbol_groups(
        &[score_day],
        symbols,
        &[candidate_symbols.to_vec()],
    )
}

#[allow(dead_code)]
pub(crate) fn return_risk_stats_pairwise_scope_for_factor_scores(
    score_days: &[NaiveDate],
    symbols: &[String],
    scores_by_date: &FactorScoresByDate,
    config: &SignalConfig,
    max_pair_rows: usize,
) -> Option<ReturnRiskStatsPairwiseScopePlan> {
    let score_days = normalized_dates(score_days);
    let symbols = normalized_symbol_key(symbols);
    if score_days.is_empty() || symbols.is_empty() {
        return Some(ReturnRiskStatsPairwiseScopePlan {
            score_days,
            symbols,
            pair_keys: Vec::new(),
        });
    }

    let symbol_scope = symbols.iter().cloned().collect::<HashSet<_>>();
    let score_candidate_pool_size =
        normalize_score_candidate_pool_size(config.score_candidate_pool_size);
    let min_candidates = config.top_n.min(5).max(2);
    let mut pair_keys = BTreeSet::new();

    for score_day in &score_days {
        let mut day_scores = scores_by_date.get(score_day).cloned().unwrap_or_default();
        sort_factor_scores(&mut day_scores, config.score_direction);

        let skip_count = if config.skip_top_pct > 0.0 {
            (day_scores.len() as f64 * config.skip_top_pct).ceil() as usize
        } else {
            0
        }
        .min(day_scores.len());

        let mut candidate_symbols = day_scores
            .into_iter()
            .skip(skip_count)
            .map(|(symbol, _score)| symbol)
            .filter(|symbol| symbol_scope.contains(symbol))
            .collect::<Vec<_>>();
        if let Some(limit) = score_candidate_pool_size {
            candidate_symbols.truncate(limit);
        }
        let candidate_symbols = normalized_symbol_key(&candidate_symbols);
        if candidate_symbols.len() < min_candidates {
            continue;
        }

        for left_idx in 0..candidate_symbols.len() {
            for right_idx in (left_idx + 1)..candidate_symbols.len() {
                pair_keys.insert(pairwise_correlation_key(
                    *score_day,
                    &candidate_symbols[left_idx],
                    &candidate_symbols[right_idx],
                ));
                if pair_keys.len() > max_pair_rows {
                    return None;
                }
            }
        }
    }

    Some(ReturnRiskStatsPairwiseScopePlan {
        score_days,
        symbols,
        pair_keys: pair_keys.into_iter().collect(),
    })
}

#[allow(dead_code)]
pub(crate) fn persistent_return_risk_stats_feature_matrix_cache_key(
    data_version_id: impl AsRef<str>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    lookback_days: usize,
    symbols: &[String],
    pairwise_scope: &ReturnRiskStatsPairwiseScopePlan,
) -> PersistentMarketFeatureCacheKey {
    let mut key = PersistentMarketFeatureCacheKey::new_for_dates(
        PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix,
        data_version_id,
        start_date,
        end_date,
        lookback_days,
        symbols,
        &pairwise_scope.score_days,
    );
    let pairwise_scope_hash =
        persistent_return_risk_stats_pairwise_scope_hash(&pairwise_scope.pair_keys);
    key.cache_key = format!(
        "{}:pairs:{}:{}",
        key.cache_key,
        pairwise_scope_hash,
        pairwise_scope.pair_count()
    );
    key
}

#[allow(dead_code)]
fn persistent_return_risk_stats_pairwise_scope_hash(
    pair_keys: &[(NaiveDate, String, String)],
) -> String {
    let mut keys = pair_keys.to_vec();
    keys.sort();
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (score_day, left_symbol, right_symbol) in keys {
        for byte in score_day.to_string().as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xfe;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        for byte in left_symbol.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xfd;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        for byte in right_symbol.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xfc;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[allow(dead_code)]
pub(crate) fn build_score_date_return_risk_matrix(
    return_history: &SymbolReturnHistory,
    score_days: &[NaiveDate],
    symbols: &[String],
    lookback_days: usize,
) -> ScoreDateReturnRiskMatrix {
    let mut returns_by_score_symbol = HashMap::new();
    for score_day in normalized_dates(score_days) {
        for symbol in normalized_symbol_key(symbols) {
            returns_by_score_symbol.insert(
                (score_day, symbol.clone()),
                trailing_returns(return_history, &symbol, score_day, lookback_days),
            );
        }
    }
    ScoreDateReturnRiskMatrix {
        returns_by_score_symbol,
    }
}

#[allow(dead_code)]
pub(crate) fn build_score_date_return_risk_stats_matrix(
    return_history: &SymbolReturnHistory,
    score_days: &[NaiveDate],
    symbols: &[String],
    lookback_days: usize,
) -> ScoreDateReturnRiskStatsMatrix {
    let mut stats_by_score_symbol = HashMap::new();
    let mut pairwise_correlations = HashMap::new();
    let symbols = normalized_symbol_key(symbols);
    for score_day in normalized_dates(score_days) {
        let returns_by_symbol = symbols
            .iter()
            .map(|symbol| {
                (
                    symbol.clone(),
                    trailing_returns(return_history, symbol, score_day, lookback_days),
                )
            })
            .collect::<Vec<_>>();

        for (symbol, returns) in &returns_by_symbol {
            stats_by_score_symbol.insert(
                (score_day, symbol.clone()),
                ReturnRiskSingleSymbolStats::from_returns(returns),
            );
        }

        for left_idx in 0..returns_by_symbol.len() {
            for right_idx in (left_idx + 1)..returns_by_symbol.len() {
                let (left_symbol, left_returns) = &returns_by_symbol[left_idx];
                let (right_symbol, right_returns) = &returns_by_symbol[right_idx];
                if let Some(correlation) = pearson_correlation(left_returns, right_returns) {
                    pairwise_correlations.insert(
                        pairwise_correlation_key(score_day, left_symbol, right_symbol),
                        correlation,
                    );
                }
            }
        }
    }
    ScoreDateReturnRiskStatsMatrix {
        stats_by_score_symbol,
        pairwise_correlations,
    }
}

#[allow(dead_code)]
pub(crate) fn build_score_date_return_risk_stats_matrix_with_pairwise_scope(
    return_history: &SymbolReturnHistory,
    score_days: &[NaiveDate],
    symbols: &[String],
    lookback_days: usize,
    pairwise_scope: &ReturnRiskStatsPairwiseScopePlan,
) -> ScoreDateReturnRiskStatsMatrix {
    let mut stats_by_score_symbol = HashMap::new();
    let mut pairwise_correlations = HashMap::new();
    let score_days = normalized_dates(score_days);
    let symbols = normalized_symbol_key(symbols);
    let symbol_scope = symbols.iter().cloned().collect::<HashSet<_>>();
    let mut pair_keys_by_score_day: BTreeMap<NaiveDate, Vec<(String, String)>> = BTreeMap::new();

    for (score_day, left_symbol, right_symbol) in &pairwise_scope.pair_keys {
        if !score_days.contains(score_day)
            || !symbol_scope.contains(left_symbol)
            || !symbol_scope.contains(right_symbol)
            || left_symbol == right_symbol
        {
            continue;
        }
        pair_keys_by_score_day
            .entry(*score_day)
            .or_default()
            .push((left_symbol.clone(), right_symbol.clone()));
    }

    for score_day in score_days {
        let returns_by_symbol = symbols
            .iter()
            .map(|symbol| {
                (
                    symbol.clone(),
                    trailing_returns(return_history, symbol, score_day, lookback_days),
                )
            })
            .collect::<HashMap<_, _>>();

        for (symbol, returns) in &returns_by_symbol {
            stats_by_score_symbol.insert(
                (score_day, symbol.clone()),
                ReturnRiskSingleSymbolStats::from_returns(returns),
            );
        }

        if let Some(pair_keys) = pair_keys_by_score_day.get(&score_day) {
            for (left_symbol, right_symbol) in pair_keys {
                let Some(left_returns) = returns_by_symbol.get(left_symbol) else {
                    continue;
                };
                let Some(right_returns) = returns_by_symbol.get(right_symbol) else {
                    continue;
                };
                if let Some(correlation) = pearson_correlation(left_returns, right_returns) {
                    pairwise_correlations.insert(
                        pairwise_correlation_key(score_day, left_symbol, right_symbol),
                        correlation,
                    );
                }
            }
        }
    }

    ScoreDateReturnRiskStatsMatrix {
        stats_by_score_symbol,
        pairwise_correlations,
    }
}

#[allow(dead_code)]
pub(crate) fn return_risk_feature_matrix_to_rows(
    matrix: &ScoreDateReturnRiskMatrix,
) -> Vec<ReturnRiskFeatureMatrixRow> {
    let mut rows = matrix
        .returns_by_score_symbol
        .iter()
        .map(
            |((score_day, symbol), returns)| ReturnRiskFeatureMatrixRow {
                score_day: *score_day,
                symbol: symbol.clone(),
                returns: returns.clone(),
            },
        )
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.score_day
            .cmp(&right.score_day)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    rows
}

#[allow(dead_code)]
pub(crate) fn return_risk_feature_matrix_from_rows(
    score_days: &[NaiveDate],
    symbols: &[String],
    rows: Vec<ReturnRiskFeatureMatrixRow>,
) -> Option<ScoreDateReturnRiskMatrix> {
    let score_days = normalized_dates(score_days);
    let symbols = normalized_symbol_key(symbols);
    let expected_keys = score_days
        .iter()
        .flat_map(|score_day| {
            symbols
                .iter()
                .map(move |symbol| (*score_day, symbol.clone()))
        })
        .collect::<HashSet<_>>();
    let mut returns_by_score_symbol = HashMap::with_capacity(expected_keys.len());

    for row in rows {
        if row.returns.iter().any(|value| !value.is_finite()) {
            return None;
        }
        let key = (row.score_day, row.symbol);
        if !expected_keys.contains(&key) || returns_by_score_symbol.contains_key(&key) {
            return None;
        }
        returns_by_score_symbol.insert(key, row.returns);
    }

    if returns_by_score_symbol.len() != expected_keys.len() {
        return None;
    }

    Some(ScoreDateReturnRiskMatrix {
        returns_by_score_symbol,
    })
}

#[allow(dead_code)]
pub(crate) fn return_risk_stats_feature_matrix_to_rows(
    matrix: &ScoreDateReturnRiskStatsMatrix,
) -> (
    Vec<ReturnRiskStatsFeatureMatrixRow>,
    Vec<ReturnRiskPairwiseCorrelationRow>,
) {
    let mut stats_rows = matrix
        .stats_by_score_symbol
        .iter()
        .map(
            |((score_day, symbol), stats)| ReturnRiskStatsFeatureMatrixRow {
                score_day: *score_day,
                symbol: symbol.clone(),
                return_count: stats.return_count,
                total_return: stats.total_return,
                sample_volatility: stats.sample_volatility,
                kelly_mean: stats.kelly_mean,
                kelly_population_variance: stats.kelly_population_variance,
            },
        )
        .collect::<Vec<_>>();
    stats_rows.sort_by(|left, right| {
        left.score_day
            .cmp(&right.score_day)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });

    let mut pair_rows = matrix
        .pairwise_correlations
        .iter()
        .map(|((score_day, left_symbol, right_symbol), correlation)| {
            ReturnRiskPairwiseCorrelationRow {
                score_day: *score_day,
                left_symbol: left_symbol.clone(),
                right_symbol: right_symbol.clone(),
                correlation: *correlation,
            }
        })
        .collect::<Vec<_>>();
    pair_rows.sort_by(|left, right| {
        left.score_day
            .cmp(&right.score_day)
            .then_with(|| left.left_symbol.cmp(&right.left_symbol))
            .then_with(|| left.right_symbol.cmp(&right.right_symbol))
    });

    (stats_rows, pair_rows)
}

fn option_f64_is_finite(value: Option<f64>) -> bool {
    value.map(|value| value.is_finite()).unwrap_or(true)
}

#[allow(dead_code)]
fn return_risk_stats_feature_matrix_dense_pair_capacity(
    score_days: &[NaiveDate],
    symbols: &[String],
) -> Option<usize> {
    let score_day_count = normalized_dates(score_days).len();
    let symbol_count = normalized_symbol_key(symbols).len();
    let pairs_per_day = symbol_count
        .checked_mul(symbol_count.saturating_sub(1))?
        .checked_div(2)?;
    score_day_count.checked_mul(pairs_per_day)
}

#[allow(dead_code)]
pub(crate) fn return_risk_stats_feature_matrix_payload_profile(
    score_days: &[NaiveDate],
    symbols: &[String],
    pair_rows: usize,
    max_pair_rows: usize,
) -> ReturnRiskStatsFeatureMatrixPayloadProfile {
    let score_day_count = normalized_dates(score_days).len();
    let symbol_count = normalized_symbol_key(symbols).len();
    let stats_rows = score_day_count
        .checked_mul(symbol_count)
        .unwrap_or(usize::MAX);
    let dense_pair_capacity =
        return_risk_stats_feature_matrix_dense_pair_capacity(score_days, symbols);
    let pair_rows_per_stats_row = if stats_rows == 0 || stats_rows == usize::MAX {
        None
    } else {
        Some(pair_rows as f64 / stats_rows as f64)
    };
    let requires_sparse_pairwise_cache = pair_rows > max_pair_rows;

    ReturnRiskStatsFeatureMatrixPayloadProfile {
        stats_rows,
        pair_rows,
        dense_pair_capacity,
        max_pair_rows,
        pair_rows_per_stats_row,
        status: if requires_sparse_pairwise_cache {
            ReturnRiskStatsFeatureMatrixPayloadStatus::RequiresSparsePairwiseCache
        } else {
            ReturnRiskStatsFeatureMatrixPayloadStatus::WithinBudget
        },
    }
}

#[allow(dead_code)]
pub(crate) fn return_risk_stats_feature_matrix_from_rows(
    score_days: &[NaiveDate],
    symbols: &[String],
    stats_rows: Vec<ReturnRiskStatsFeatureMatrixRow>,
    pair_rows: Vec<ReturnRiskPairwiseCorrelationRow>,
) -> Option<ScoreDateReturnRiskStatsMatrix> {
    let score_days = normalized_dates(score_days);
    let symbols = normalized_symbol_key(symbols);
    let symbol_set = symbols.iter().cloned().collect::<HashSet<_>>();
    let expected_stats_keys = score_days
        .iter()
        .flat_map(|score_day| {
            symbols
                .iter()
                .map(move |symbol| (*score_day, symbol.clone()))
        })
        .collect::<HashSet<_>>();

    let mut stats_by_score_symbol = HashMap::with_capacity(expected_stats_keys.len());
    for row in stats_rows {
        if !option_f64_is_finite(row.total_return)
            || !option_f64_is_finite(row.sample_volatility)
            || !option_f64_is_finite(row.kelly_mean)
            || !option_f64_is_finite(row.kelly_population_variance)
        {
            return None;
        }
        let key = (row.score_day, row.symbol);
        if !expected_stats_keys.contains(&key) || stats_by_score_symbol.contains_key(&key) {
            return None;
        }
        stats_by_score_symbol.insert(
            key,
            ReturnRiskSingleSymbolStats {
                return_count: row.return_count,
                total_return: row.total_return,
                sample_volatility: row.sample_volatility,
                kelly_mean: row.kelly_mean,
                kelly_population_variance: row.kelly_population_variance,
            },
        );
    }

    if stats_by_score_symbol.len() != expected_stats_keys.len() {
        return None;
    }

    let score_day_set = score_days.iter().copied().collect::<HashSet<_>>();
    let mut pairwise_correlations = HashMap::new();
    for row in pair_rows {
        if !score_day_set.contains(&row.score_day)
            || !symbol_set.contains(&row.left_symbol)
            || !symbol_set.contains(&row.right_symbol)
            || row.left_symbol >= row.right_symbol
            || !row.correlation.is_finite()
            || row.correlation < -1.0 - 1e-12
            || row.correlation > 1.0 + 1e-12
        {
            return None;
        }
        let key = (row.score_day, row.left_symbol, row.right_symbol);
        if pairwise_correlations
            .insert(key, row.correlation.clamp(-1.0, 1.0))
            .is_some()
        {
            return None;
        }
    }

    Some(ScoreDateReturnRiskStatsMatrix {
        stats_by_score_symbol,
        pairwise_correlations,
    })
}

#[allow(dead_code)]
pub(crate) fn persistent_return_risk_feature_matrix_rows_to_matrix(
    score_days: &[NaiveDate],
    symbols: &[String],
    row_count: i64,
    rows: Vec<(NaiveDate, String, Vec<f64>)>,
) -> Option<ScoreDateReturnRiskMatrix> {
    if row_count < 0 || rows.len() as i64 != row_count {
        return None;
    }
    let rows = rows
        .into_iter()
        .map(|(score_day, symbol, returns)| ReturnRiskFeatureMatrixRow {
            score_day,
            symbol,
            returns,
        })
        .collect::<Vec<_>>();
    return_risk_feature_matrix_from_rows(score_days, symbols, rows)
}

#[allow(dead_code)]
pub(crate) fn persistent_return_risk_stats_feature_matrix_rows_to_matrix(
    score_days: &[NaiveDate],
    symbols: &[String],
    stats_row_count: i64,
    pair_row_count: i64,
    stats_rows: Vec<(
        NaiveDate,
        String,
        i64,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
    )>,
    pair_rows: Vec<(NaiveDate, String, String, f64)>,
) -> Option<ScoreDateReturnRiskStatsMatrix> {
    if stats_row_count < 0
        || pair_row_count < 0
        || stats_rows.len() as i64 != stats_row_count
        || pair_rows.len() as i64 != pair_row_count
    {
        return None;
    }

    let stats_rows = stats_rows
        .into_iter()
        .map(
            |(
                score_day,
                symbol,
                return_count,
                total_return,
                sample_volatility,
                kelly_mean,
                kelly_population_variance,
            )| {
                Some(ReturnRiskStatsFeatureMatrixRow {
                    score_day,
                    symbol,
                    return_count: usize::try_from(return_count).ok()?,
                    total_return,
                    sample_volatility,
                    kelly_mean,
                    kelly_population_variance,
                })
            },
        )
        .collect::<Option<Vec<_>>>()?;
    let pair_rows = pair_rows
        .into_iter()
        .map(|(score_day, left_symbol, right_symbol, correlation)| {
            ReturnRiskPairwiseCorrelationRow {
                score_day,
                left_symbol,
                right_symbol,
                correlation,
            }
        })
        .collect::<Vec<_>>();

    return_risk_stats_feature_matrix_from_rows(score_days, symbols, stats_rows, pair_rows)
}

#[allow(dead_code)]
async fn load_persistent_return_risk_feature_matrix_cache(
    pool: &PgPool,
    key: &PersistentMarketFeatureCacheKey,
    requested_symbols: &[String],
    score_days: &[NaiveDate],
) -> Result<Option<ScoreDateReturnRiskMatrix>, String> {
    let requested_symbols = normalized_symbol_key(requested_symbols);
    let score_days = normalized_dates(score_days);
    if requested_symbols.is_empty() || score_days.is_empty() {
        return Ok(Some(ScoreDateReturnRiskMatrix::default()));
    }

    let manifest: Option<(String, i32, i64)> = match sqlx::query_as(
        "SELECT status, symbol_count, row_count
         FROM market_feature_cache_manifest
         WHERE cache_key = $1
           AND feature_kind = $2
           AND data_version_id = $3
           AND start_date = $4
           AND end_date = $5
           AND lookback_days = $6
           AND universe_hash = $7
           AND symbol_count = $8",
    )
    .bind(&key.cache_key)
    .bind(PersistentMarketFeatureKind::ReturnRiskFeatureMatrix.as_str())
    .bind(&key.data_version_id)
    .bind(key.start_date)
    .bind(key.end_date)
    .bind(key.lookback_days as i32)
    .bind(&key.universe_hash)
    .bind(key.symbol_count as i32)
    .fetch_optional(pool)
    .await
    {
        Ok(value) => value,
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Failed to load persistent return/risk feature matrix manifest {}: {}",
                key.cache_key, error
            ));
        }
    };

    let Some((status, symbol_count, row_count)) = manifest else {
        return Ok(None);
    };

    let cached_symbols: Vec<(String,)> =
        sqlx::query_as("SELECT symbol FROM market_feature_cache_symbol WHERE cache_key = $1")
            .bind(&key.cache_key)
            .fetch_all(pool)
            .await
            .map_err(|error| {
                format!(
                    "Failed to load persistent return/risk feature matrix symbols {}: {}",
                    key.cache_key, error
                )
            })?;
    let cached_symbols = cached_symbols
        .into_iter()
        .map(|(symbol,)| symbol)
        .collect::<Vec<_>>();
    if !persistent_market_feature_manifest_is_usable(
        &status,
        symbol_count,
        &cached_symbols,
        &requested_symbols,
    ) {
        return Ok(None);
    }

    let rows: Vec<(NaiveDate, String, Vec<f64>)> = match sqlx::query_as(
        "SELECT score_day, symbol, returns
         FROM market_feature_cache_return_risk_matrix_row
         WHERE cache_key = $1
         ORDER BY score_day, symbol",
    )
    .bind(&key.cache_key)
    .fetch_all(pool)
    .await
    {
        Ok(value) => value,
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Failed to load persistent return/risk feature matrix rows {}: {}",
                key.cache_key, error
            ));
        }
    };

    Ok(persistent_return_risk_feature_matrix_rows_to_matrix(
        &score_days,
        &requested_symbols,
        row_count,
        rows,
    ))
}

#[allow(dead_code)]
async fn store_persistent_return_risk_feature_matrix_cache(
    pool: &PgPool,
    key: &PersistentMarketFeatureCacheKey,
    requested_symbols: &[String],
    score_days: &[NaiveDate],
    matrix: &ScoreDateReturnRiskMatrix,
) -> Result<bool, String> {
    let requested_symbols = normalized_symbol_key(requested_symbols);
    let score_days = normalized_dates(score_days);
    if requested_symbols.is_empty() || score_days.is_empty() {
        return Ok(false);
    }

    let rows = return_risk_feature_matrix_to_rows(matrix);
    if return_risk_feature_matrix_from_rows(&score_days, &requested_symbols, rows.clone()).is_none()
    {
        return Err(format!(
            "Refusing to persist incomplete return/risk feature matrix {}",
            key.cache_key
        ));
    }

    let manifest_result = sqlx::query(
        "INSERT INTO market_feature_cache_manifest (
             cache_key, feature_kind, data_version_id, start_date, end_date,
             lookback_days, universe_hash, symbol_count, row_count, status, metadata
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'building',
                 jsonb_build_object('writer', 'quant-backtest'))
         ON CONFLICT (cache_key) DO UPDATE SET
             feature_kind = EXCLUDED.feature_kind,
             data_version_id = EXCLUDED.data_version_id,
             start_date = EXCLUDED.start_date,
             end_date = EXCLUDED.end_date,
             lookback_days = EXCLUDED.lookback_days,
             universe_hash = EXCLUDED.universe_hash,
             symbol_count = EXCLUDED.symbol_count,
             row_count = 0,
             status = 'building',
             metadata = EXCLUDED.metadata,
             updated_at = now()",
    )
    .bind(&key.cache_key)
    .bind(PersistentMarketFeatureKind::ReturnRiskFeatureMatrix.as_str())
    .bind(&key.data_version_id)
    .bind(key.start_date)
    .bind(key.end_date)
    .bind(key.lookback_days as i32)
    .bind(&key.universe_hash)
    .bind(key.symbol_count as i32)
    .execute(pool)
    .await;

    match manifest_result {
        Ok(_) => {}
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => {
            return Ok(false);
        }
        Err(error) => {
            return Err(format!(
                "Failed to upsert persistent return/risk feature matrix manifest {}: {}",
                key.cache_key, error
            ));
        }
    }

    let clear_rows =
        sqlx::query("DELETE FROM market_feature_cache_return_risk_matrix_row WHERE cache_key = $1")
            .bind(&key.cache_key)
            .execute(pool)
            .await;
    match clear_rows {
        Ok(_) => {}
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => {
            return Ok(false);
        }
        Err(error) => {
            return Err(format!(
                "Failed to clear persistent return/risk feature matrix rows {}: {}",
                key.cache_key, error
            ));
        }
    }

    sqlx::query("DELETE FROM market_feature_cache_value WHERE cache_key = $1")
        .bind(&key.cache_key)
        .execute(pool)
        .await
        .map_err(|error| {
            format!(
                "Failed to clear stale scalar market feature cache values {}: {}",
                key.cache_key, error
            )
        })?;
    sqlx::query("DELETE FROM market_feature_cache_symbol WHERE cache_key = $1")
        .bind(&key.cache_key)
        .execute(pool)
        .await
        .map_err(|error| {
            format!(
                "Failed to clear persistent return/risk feature matrix symbols {}: {}",
                key.cache_key, error
            )
        })?;

    for chunk in requested_symbols.chunks(5_000) {
        let chunk_symbols = chunk.to_vec();
        sqlx::query(
            "INSERT INTO market_feature_cache_symbol (cache_key, symbol)
             SELECT $1, symbol
             FROM UNNEST($2::TEXT[]) AS t(symbol)
             ON CONFLICT (cache_key, symbol) DO NOTHING",
        )
        .bind(&key.cache_key)
        .bind(&chunk_symbols)
        .execute(pool)
        .await
        .map_err(|error| {
            format!(
                "Failed to store persistent return/risk feature matrix symbols {}: {}",
                key.cache_key, error
            )
        })?;
    }

    let mut row_count = 0_i64;
    for chunk in rows.chunks(1_000) {
        row_count +=
            insert_persistent_return_risk_feature_matrix_row_chunk(pool, &key.cache_key, chunk)
                .await?;
    }

    sqlx::query(
        "UPDATE market_feature_cache_manifest
         SET row_count = $2,
             status = 'ready',
             updated_at = now()
         WHERE cache_key = $1",
    )
    .bind(&key.cache_key)
    .bind(row_count)
    .execute(pool)
    .await
    .map_err(|error| {
        format!(
            "Failed to mark persistent return/risk feature matrix ready {}: {}",
            key.cache_key, error
        )
    })?;

    Ok(true)
}

#[allow(dead_code)]
async fn insert_persistent_return_risk_feature_matrix_row_chunk(
    pool: &PgPool,
    cache_key: &str,
    rows: &[ReturnRiskFeatureMatrixRow],
) -> Result<i64, String> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO market_feature_cache_return_risk_matrix_row \
         (cache_key, score_day, symbol, returns) ",
    );
    builder.push_values(rows, |mut row_builder, row| {
        row_builder
            .push_bind(cache_key)
            .push_bind(row.score_day)
            .push_bind(&row.symbol)
            .push_bind(row.returns.clone());
    });
    builder.push(
        " ON CONFLICT (cache_key, score_day, symbol) DO UPDATE SET \
          returns = EXCLUDED.returns",
    );
    let result = builder.build().execute(pool).await.map_err(|error| {
        format!(
            "Failed to store persistent return/risk feature matrix rows {}: {}",
            cache_key, error
        )
    })?;
    Ok(result.rows_affected() as i64)
}

#[allow(dead_code)]
async fn load_persistent_return_risk_stats_feature_matrix_cache(
    pool: &PgPool,
    key: &PersistentMarketFeatureCacheKey,
    requested_symbols: &[String],
    score_days: &[NaiveDate],
) -> Result<Option<ScoreDateReturnRiskStatsMatrix>, String> {
    let requested_symbols = normalized_symbol_key(requested_symbols);
    let score_days = normalized_dates(score_days);
    if requested_symbols.is_empty() || score_days.is_empty() {
        return Ok(Some(ScoreDateReturnRiskStatsMatrix::default()));
    }

    let manifest: Option<(String, i32, i64, i64)> = match sqlx::query_as(
        "SELECT status,
                symbol_count,
                row_count,
                COALESCE((metadata->>'pair_row_count')::BIGINT, -1) AS pair_row_count
         FROM market_feature_cache_manifest
         WHERE cache_key = $1
           AND feature_kind = $2
           AND data_version_id = $3
           AND start_date = $4
           AND end_date = $5
           AND lookback_days = $6
           AND universe_hash = $7
           AND symbol_count = $8",
    )
    .bind(&key.cache_key)
    .bind(PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix.as_str())
    .bind(&key.data_version_id)
    .bind(key.start_date)
    .bind(key.end_date)
    .bind(key.lookback_days as i32)
    .bind(&key.universe_hash)
    .bind(key.symbol_count as i32)
    .fetch_optional(pool)
    .await
    {
        Ok(value) => value,
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Failed to load persistent return/risk stats feature matrix manifest {}: {}",
                key.cache_key, error
            ));
        }
    };

    let Some((status, symbol_count, stats_row_count, pair_row_count)) = manifest else {
        return Ok(None);
    };

    let cached_symbols: Vec<(String,)> =
        sqlx::query_as("SELECT symbol FROM market_feature_cache_symbol WHERE cache_key = $1")
            .bind(&key.cache_key)
            .fetch_all(pool)
            .await
            .map_err(|error| {
                format!(
                    "Failed to load persistent return/risk stats feature matrix symbols {}: {}",
                    key.cache_key, error
                )
            })?;
    let cached_symbols = cached_symbols
        .into_iter()
        .map(|(symbol,)| symbol)
        .collect::<Vec<_>>();
    if !persistent_market_feature_manifest_is_usable(
        &status,
        symbol_count,
        &cached_symbols,
        &requested_symbols,
    ) {
        return Ok(None);
    }

    let stats_rows: Vec<(
        NaiveDate,
        String,
        i64,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
    )> = match sqlx::query_as(
        "SELECT score_day,
                symbol,
                return_count,
                total_return,
                sample_volatility,
                kelly_mean,
                kelly_population_variance
         FROM market_feature_cache_return_risk_stats_row
         WHERE cache_key = $1
         ORDER BY score_day, symbol",
    )
    .bind(&key.cache_key)
    .fetch_all(pool)
    .await
    {
        Ok(value) => value,
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Failed to load persistent return/risk stats feature matrix rows {}: {}",
                key.cache_key, error
            ));
        }
    };

    let pair_rows: Vec<(NaiveDate, String, String, f64)> = match sqlx::query_as(
        "SELECT score_day, left_symbol, right_symbol, correlation
         FROM market_feature_cache_return_risk_pairwise_row
         WHERE cache_key = $1
         ORDER BY score_day, left_symbol, right_symbol",
    )
    .bind(&key.cache_key)
    .fetch_all(pool)
    .await
    {
        Ok(value) => value,
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Failed to load persistent return/risk pairwise correlation rows {}: {}",
                key.cache_key, error
            ));
        }
    };

    Ok(persistent_return_risk_stats_feature_matrix_rows_to_matrix(
        &score_days,
        &requested_symbols,
        stats_row_count,
        pair_row_count,
        stats_rows,
        pair_rows,
    ))
}

#[allow(dead_code)]
async fn store_persistent_return_risk_stats_feature_matrix_cache(
    pool: &PgPool,
    key: &PersistentMarketFeatureCacheKey,
    requested_symbols: &[String],
    score_days: &[NaiveDate],
    matrix: &ScoreDateReturnRiskStatsMatrix,
) -> Result<bool, String> {
    let requested_symbols = normalized_symbol_key(requested_symbols);
    let score_days = normalized_dates(score_days);
    if requested_symbols.is_empty() || score_days.is_empty() {
        return Ok(false);
    }

    let (stats_rows, pair_rows) = return_risk_stats_feature_matrix_to_rows(matrix);
    if return_risk_stats_feature_matrix_from_rows(
        &score_days,
        &requested_symbols,
        stats_rows.clone(),
        pair_rows.clone(),
    )
    .is_none()
    {
        return Err(format!(
            "Refusing to persist incomplete return/risk stats feature matrix {}",
            key.cache_key
        ));
    }
    let pair_row_count = pair_rows.len() as i64;
    let payload_profile = return_risk_stats_feature_matrix_payload_profile(
        &score_days,
        &requested_symbols,
        pair_rows.len(),
        DEFAULT_RETURN_RISK_STATS_PAIRWISE_ROW_LIMIT,
    );
    if !payload_profile.within_budget() {
        return Err(format!(
            "Refusing to persist dense return/risk stats feature matrix {}: pair_rows={}, dense_pair_capacity={:?}, max_pair_rows={}",
            key.cache_key,
            payload_profile.pair_rows,
            payload_profile.dense_pair_capacity,
            payload_profile.max_pair_rows
        ));
    }

    let manifest_result = sqlx::query(
        "INSERT INTO market_feature_cache_manifest (
             cache_key, feature_kind, data_version_id, start_date, end_date,
             lookback_days, universe_hash, symbol_count, row_count, status, metadata
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'building',
                 jsonb_build_object(
                     'writer', 'quant-backtest',
                     'payload', 'return_risk_stats_feature_matrix',
                     'pair_row_count', $9::BIGINT
                 ))
         ON CONFLICT (cache_key) DO UPDATE SET
             feature_kind = EXCLUDED.feature_kind,
             data_version_id = EXCLUDED.data_version_id,
             start_date = EXCLUDED.start_date,
             end_date = EXCLUDED.end_date,
             lookback_days = EXCLUDED.lookback_days,
             universe_hash = EXCLUDED.universe_hash,
             symbol_count = EXCLUDED.symbol_count,
             row_count = 0,
             status = 'building',
             metadata = EXCLUDED.metadata,
             updated_at = now()",
    )
    .bind(&key.cache_key)
    .bind(PersistentMarketFeatureKind::ReturnRiskStatsFeatureMatrix.as_str())
    .bind(&key.data_version_id)
    .bind(key.start_date)
    .bind(key.end_date)
    .bind(key.lookback_days as i32)
    .bind(&key.universe_hash)
    .bind(key.symbol_count as i32)
    .bind(pair_row_count)
    .execute(pool)
    .await;

    match manifest_result {
        Ok(_) => {}
        Err(error) if is_missing_persistent_market_feature_cache_table(&error) => {
            return Ok(false);
        }
        Err(error) => {
            return Err(format!(
                "Failed to upsert persistent return/risk stats feature matrix manifest {}: {}",
                key.cache_key, error
            ));
        }
    }

    for (table_name, description) in [
        (
            "market_feature_cache_return_risk_stats_row",
            "stats feature matrix rows",
        ),
        (
            "market_feature_cache_return_risk_pairwise_row",
            "pairwise correlation rows",
        ),
        ("market_feature_cache_value", "stale scalar cache values"),
        (
            "market_feature_cache_symbol",
            "stats feature matrix symbols",
        ),
    ] {
        let clear_sql = format!("DELETE FROM {table_name} WHERE cache_key = $1");
        let clear_result = sqlx::query(&clear_sql)
            .bind(&key.cache_key)
            .execute(pool)
            .await;
        match clear_result {
            Ok(_) => {}
            Err(error) if is_missing_persistent_market_feature_cache_table(&error) => {
                return Ok(false);
            }
            Err(error) => {
                return Err(format!(
                    "Failed to clear persistent return/risk {} {}: {}",
                    description, key.cache_key, error
                ));
            }
        }
    }

    for chunk in requested_symbols.chunks(5_000) {
        let chunk_symbols = chunk.to_vec();
        sqlx::query(
            "INSERT INTO market_feature_cache_symbol (cache_key, symbol)
             SELECT $1, symbol
             FROM UNNEST($2::TEXT[]) AS t(symbol)
             ON CONFLICT (cache_key, symbol) DO NOTHING",
        )
        .bind(&key.cache_key)
        .bind(&chunk_symbols)
        .execute(pool)
        .await
        .map_err(|error| {
            format!(
                "Failed to store persistent return/risk stats feature matrix symbols {}: {}",
                key.cache_key, error
            )
        })?;
    }

    let mut stats_row_count = 0_i64;
    for chunk in stats_rows.chunks(1_000) {
        stats_row_count += insert_persistent_return_risk_stats_feature_matrix_row_chunk(
            pool,
            &key.cache_key,
            chunk,
        )
        .await?;
    }

    let mut inserted_pair_row_count = 0_i64;
    for chunk in pair_rows.chunks(1_000) {
        inserted_pair_row_count +=
            insert_persistent_return_risk_pairwise_row_chunk(pool, &key.cache_key, chunk).await?;
    }

    sqlx::query(
        "UPDATE market_feature_cache_manifest
         SET row_count = $2,
             status = 'ready',
             metadata = jsonb_set(metadata, '{pair_row_count}', to_jsonb($3::BIGINT), true),
             updated_at = now()
         WHERE cache_key = $1",
    )
    .bind(&key.cache_key)
    .bind(stats_row_count)
    .bind(inserted_pair_row_count)
    .execute(pool)
    .await
    .map_err(|error| {
        format!(
            "Failed to mark persistent return/risk stats feature matrix ready {}: {}",
            key.cache_key, error
        )
    })?;

    Ok(true)
}

#[allow(dead_code)]
async fn insert_persistent_return_risk_stats_feature_matrix_row_chunk(
    pool: &PgPool,
    cache_key: &str,
    rows: &[ReturnRiskStatsFeatureMatrixRow],
) -> Result<i64, String> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO market_feature_cache_return_risk_stats_row \
         (cache_key, score_day, symbol, return_count, total_return, sample_volatility, \
          kelly_mean, kelly_population_variance) ",
    );
    builder.push_values(rows, |mut row_builder, row| {
        row_builder
            .push_bind(cache_key)
            .push_bind(row.score_day)
            .push_bind(&row.symbol)
            .push_bind(row.return_count as i64)
            .push_bind(row.total_return)
            .push_bind(row.sample_volatility)
            .push_bind(row.kelly_mean)
            .push_bind(row.kelly_population_variance);
    });
    builder.push(
        " ON CONFLICT (cache_key, score_day, symbol) DO UPDATE SET \
          return_count = EXCLUDED.return_count, \
          total_return = EXCLUDED.total_return, \
          sample_volatility = EXCLUDED.sample_volatility, \
          kelly_mean = EXCLUDED.kelly_mean, \
          kelly_population_variance = EXCLUDED.kelly_population_variance",
    );
    let result = builder.build().execute(pool).await.map_err(|error| {
        format!(
            "Failed to store persistent return/risk stats feature matrix rows {}: {}",
            cache_key, error
        )
    })?;
    Ok(result.rows_affected() as i64)
}

#[allow(dead_code)]
async fn insert_persistent_return_risk_pairwise_row_chunk(
    pool: &PgPool,
    cache_key: &str,
    rows: &[ReturnRiskPairwiseCorrelationRow],
) -> Result<i64, String> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO market_feature_cache_return_risk_pairwise_row \
         (cache_key, score_day, left_symbol, right_symbol, correlation) ",
    );
    builder.push_values(rows, |mut row_builder, row| {
        row_builder
            .push_bind(cache_key)
            .push_bind(row.score_day)
            .push_bind(&row.left_symbol)
            .push_bind(&row.right_symbol)
            .push_bind(row.correlation);
    });
    builder.push(
        " ON CONFLICT (cache_key, score_day, left_symbol, right_symbol) DO UPDATE SET \
          correlation = EXCLUDED.correlation",
    );
    let result = builder.build().execute(pool).await.map_err(|error| {
        format!(
            "Failed to store persistent return/risk pairwise correlation rows {}: {}",
            cache_key, error
        )
    })?;
    Ok(result.rows_affected() as i64)
}

pub(crate) fn covariance_concentration_penalty(
    symbol: &str,
    symbols: &[String],
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    score_day: NaiveDate,
    config: &PortfolioConstructionConfig,
) -> f64 {
    let own_returns = trailing_returns(
        return_history,
        symbol,
        score_day,
        config.risk_budget_lookback_days,
    );
    if own_returns.len() < 3 {
        return 1.0;
    }

    let average_abs_corr = symbols
        .iter()
        .filter(|other| other.as_str() != symbol)
        .filter_map(|other| {
            let other_returns = trailing_returns(
                return_history,
                other,
                score_day,
                config.risk_budget_lookback_days,
            );
            pearson_correlation(&own_returns, &other_returns).map(f64::abs)
        })
        .collect::<Vec<_>>();

    if average_abs_corr.is_empty() {
        1.0
    } else {
        1.0 + average_abs_corr.iter().sum::<f64>() / average_abs_corr.len() as f64
    }
}

fn capacity_score(symbol: &str, average_amounts: &HashMap<String, f64>, max_amount: f64) -> f64 {
    if max_amount <= 0.0 {
        return 1.0;
    }
    average_amounts
        .get(symbol)
        .copied()
        .filter(|amount| amount.is_finite() && *amount > 0.0)
        .map(|amount| (amount / max_amount).clamp(0.05, 1.0))
        .unwrap_or(0.50)
}

pub(crate) fn trailing_returns(
    return_history: &HashMap<String, Vec<(NaiveDate, f64)>>,
    symbol: &str,
    score_day: NaiveDate,
    lookback_days: usize,
) -> Vec<f64> {
    let mut values: Vec<f64> = return_history
        .get(symbol)
        .map(|rows| {
            rows.iter()
                .filter(|(date, value)| *date < score_day && value.is_finite())
                .map(|(_, value)| *value)
                .collect()
        })
        .unwrap_or_default();
    if values.len() > lookback_days {
        values = values[values.len() - lookback_days..].to_vec();
    }
    values
}

pub(crate) fn pearson_correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    let len = left.len().min(right.len());
    if len < 3 {
        return None;
    }
    let left = &left[left.len() - len..];
    let right = &right[right.len() - len..];
    let left_mean = left.iter().sum::<f64>() / len as f64;
    let right_mean = right.iter().sum::<f64>() / len as f64;
    let mut covariance = 0.0;
    let mut left_var = 0.0;
    let mut right_var = 0.0;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let left_diff = *left_value - left_mean;
        let right_diff = *right_value - right_mean;
        covariance += left_diff * right_diff;
        left_var += left_diff * left_diff;
        right_var += right_diff * right_diff;
    }
    if left_var <= f64::EPSILON || right_var <= f64::EPSILON {
        return None;
    }
    Some(covariance / (left_var.sqrt() * right_var.sqrt()))
}

pub(crate) fn fractional_kelly_weight(returns: &[f64], fraction: f64) -> Option<f64> {
    if returns.len() < 3 || fraction <= 0.0 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / returns.len() as f64;
    if variance <= f64::EPSILON {
        return None;
    }
    Some((mean / variance * fraction).clamp(0.0, 1.0))
}
