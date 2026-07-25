//! Core signal generation entry points.
use super::*;

/// Generate daily strategy signals from factor combo scores.
///
/// For each rebalance date, ranks all stocks by combo score (higher = better),
/// selects top-N, and assigns equal weight.
///
/// **Delayed entry**: when `entry_delay_days > 0`, the signal uses scores from
/// `entry_delay_days` trading days earlier than normal. This skips the initial
/// "dip period" where newly-ranked stocks tend to decline before outperforming.
pub async fn generate_signals(
    pool: &PgPool,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let mut cache = SignalDataCache::default();
    generate_signals_with_cache(pool, config, start_date, end_date, &mut cache).await
}

/// Generate factor signals using a caller-owned cache for repeated trial batches.
pub async fn generate_signals_with_cache(
    pool: &PgPool,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    generate_signals_with_cache_internal(pool, config, start_date, end_date, cache, None).await
}

pub async fn generate_signals_with_cache_and_market_feature_snapshot(
    pool: &PgPool,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
    snapshot_scope: &MarketFeatureSnapshotScope,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    generate_signals_with_cache_internal(
        pool,
        config,
        start_date,
        end_date,
        cache,
        Some(snapshot_scope),
    )
    .await
}

async fn generate_signals_with_cache_internal(
    pool: &PgPool,
    config: &SignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
    snapshot_scope: Option<&MarketFeatureSnapshotScope>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let trading_days = load_open_trading_days_cached(pool, cache, start_date, end_date).await?;
    let score_days = rebalance_score_days(trading_days.as_ref(), config, |_day, base| base.clone());
    let score_cache = load_combo_scores_for_dates_cached(pool, cache, config, &score_days).await?;
    let mut adjusted_scores;
    let scores_by_date: &FactorScoresByDate = if config.min_daily_amount_cny.is_some()
        || config.prediction_blend.is_some()
        || config.event_gate.is_some()
    {
        adjusted_scores = Arc::as_ref(&score_cache).clone();
        if config.min_daily_amount_cny.is_some() {
            apply_factor_liquidity_filter(
                pool,
                cache,
                &mut adjusted_scores,
                config,
                start_date,
                end_date,
            )
            .await?;
        }
        if let Some(blend) = config.prediction_blend.as_ref() {
            let prediction_scores = load_prediction_scores_by_date_cached(
                pool,
                cache,
                &blend.prediction_set_id,
                start_date,
                end_date,
            )
            .await?;
            blend_factor_prediction_scores(
                &mut adjusted_scores,
                prediction_scores.as_ref(),
                blend,
                config.score_direction,
            );
        }
        if let Some(event_gate) = config.event_gate.as_ref() {
            let event_scores =
                load_event_gate_scores_for_dates_cached(pool, cache, event_gate, &score_days)
                    .await?;
            apply_event_gate_scores(&mut adjusted_scores, event_scores.as_ref(), event_gate);
        }
        &adjusted_scores
    } else {
        score_cache.as_ref()
    };

    let score_source_configs = score_source_configs(config);
    let base_source_key = FactorScoreSourceKey::from_config(config);
    let mut score_sources: HashMap<FactorScoreSourceKey, FactorScoresByDate> = HashMap::new();
    for source_config in score_source_configs {
        let key = FactorScoreSourceKey::from_config(&source_config);
        if score_sources.contains_key(&key) {
            continue;
        }
        if key == base_source_key {
            score_sources.insert(key, scores_by_date.clone());
        } else {
            let overlay_scores =
                load_combo_scores_for_dates_cached(pool, cache, &source_config, &score_days)
                    .await?;
            score_sources.insert(key, Arc::as_ref(&overlay_scores).clone());
        }
    }

    let all_symbols: Vec<String> = score_sources
        .values()
        .flat_map(|scores_by_date| scores_by_date.values())
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let portfolio_config = PortfolioConstructionConfig::from(config);
    let return_lookback_days = portfolio_history_lookback_days(&portfolio_config);
    let prefer_return_risk_stats_matrices = snapshot_scope
        .map(MarketFeatureSnapshotScope::prefer_return_risk_stats_cache)
        .unwrap_or(false);
    let mut return_history = None;
    let return_risk_stats_matrices = if let Some(snapshot_scope) =
        snapshot_scope.filter(|_| prefer_return_risk_stats_matrices)
    {
        let loaded_return_history = load_symbol_return_history_for_snapshot_scope_cached(
            pool,
            cache,
            Some(snapshot_scope),
            &all_symbols,
            start_date,
            end_date,
            return_lookback_days,
        )
        .await?;
        load_portfolio_return_risk_stats_feature_matrices_cached(
            pool,
            cache,
            Some(snapshot_scope.data_version_id.as_str()),
            &all_symbols,
            start_date,
            end_date,
            &score_days,
            &portfolio_config,
            loaded_return_history.as_ref(),
            scores_by_date,
            config,
        )
        .await
        .map(|matrices| {
            return_history = Some(loaded_return_history);
            matrices
        })?
    } else {
        HashMap::new()
    };
    let return_risk_stats_matrices_loaded = return_risk_stats_matrices_cover_required_lookbacks(
        &return_risk_stats_matrices,
        &portfolio_config,
    );
    let return_risk_matrices = if snapshot_scope.is_some()
        && should_load_raw_return_risk_matrices(
            prefer_return_risk_stats_matrices,
            return_risk_stats_matrices_loaded,
        ) {
        let snapshot_scope = snapshot_scope.expect("snapshot scope checked");
        load_portfolio_return_risk_feature_matrices_cached(
            pool,
            cache,
            Some(snapshot_scope.data_version_id.as_str()),
            &all_symbols,
            start_date,
            end_date,
            &score_days,
            &portfolio_config,
            None,
        )
        .await?
    } else {
        HashMap::new()
    };
    let return_history = if let Some(return_history) = return_history {
        return_history
    } else if return_risk_stats_matrices_loaded
        || return_risk_matrices_cover_required_lookbacks(&return_risk_matrices, &portfolio_config)
    {
        Arc::new(HashMap::new())
    } else {
        load_symbol_return_history_for_snapshot_scope_cached(
            pool,
            cache,
            snapshot_scope,
            &all_symbols,
            start_date,
            end_date,
            return_lookback_days,
        )
        .await?
    };
    let average_amounts = load_portfolio_capacity_inputs_cached(
        pool,
        cache,
        snapshot_scope.map(|scope| scope.data_version_id.as_str()),
        &all_symbols,
        start_date,
        end_date,
        &score_days,
        &portfolio_config,
    )
    .await?;
    if let Some(snapshot_scope) = snapshot_scope {
        cache.insert_market_feature_snapshot_from_cached_histories(
            snapshot_scope.snapshot_key(return_lookback_days, &all_symbols),
            return_lookback_days,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
            &all_symbols,
            start_date,
            end_date,
        );
    }
    let industry_by_symbol =
        load_portfolio_industry_inputs_cached(pool, cache, &all_symbols, &portfolio_config).await?;

    build_rebalance_factor_signals_with_score_selector_and_return_risk_matrices(
        trading_days.as_ref(),
        config,
        return_history.as_ref(),
        average_amounts.as_ref(),
        industry_by_symbol.as_ref(),
        &return_risk_matrices,
        &return_risk_stats_matrices,
        prefer_return_risk_stats_matrices,
        |score_day, active_config| {
            score_rows_for_active_config(&score_sources, score_day, active_config)
        },
        |_day, base| base.clone(),
    )
}

/// Generate factor signals with market-regime-aware parameter overlays.
pub async fn generate_regime_signals(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let mut cache = SignalDataCache::default();
    generate_regime_signals_with_cache(pool, config, policy, start_date, end_date, &mut cache).await
}

/// Generate regime-aware factor signals using a caller-owned cache for repeated trial batches.
pub async fn generate_regime_signals_with_cache(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    generate_regime_signals_with_cache_internal(
        pool, config, policy, start_date, end_date, cache, None,
    )
    .await
}

pub async fn generate_regime_signals_with_cache_and_market_feature_snapshot(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
    snapshot_scope: &MarketFeatureSnapshotScope,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    generate_regime_signals_with_cache_internal(
        pool,
        config,
        policy,
        start_date,
        end_date,
        cache,
        Some(snapshot_scope),
    )
    .await
}

async fn generate_regime_signals_with_cache_internal(
    pool: &PgPool,
    config: &SignalConfig,
    policy: &MarketRegimePolicy,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cache: &mut SignalDataCache,
    snapshot_scope: Option<&MarketFeatureSnapshotScope>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let trading_days = load_open_trading_days_cached(pool, cache, start_date, end_date).await?;
    let portfolio_config = PortfolioConstructionConfig::from(config);
    let max_lookback = portfolio_history_lookback_days(&portfolio_config).max(policy.lookback_days);
    let benchmark_returns = load_benchmark_return_history_cached(
        pool,
        cache,
        &policy.benchmark,
        start_date,
        end_date,
        max_lookback,
    )
    .await?;

    let score_days = rebalance_score_days(trading_days.as_ref(), config, |day, base| {
        let returns =
            trailing_market_returns(benchmark_returns.as_ref(), day, policy.lookback_days);
        let regime = classify_market_regime(&returns, policy);
        policy.apply(base, regime)
    });
    let score_source_configs = regime_score_source_configs(config, policy);
    let mut score_sources: HashMap<FactorScoreSourceKey, FactorScoresByDate> = HashMap::new();
    for source_config in score_source_configs {
        let key = FactorScoreSourceKey::from_config(&source_config);
        if score_sources.contains_key(&key) {
            continue;
        }
        let scores =
            load_regime_base_scores_for_dates_cached(pool, cache, &source_config, &score_days)
                .await?;
        score_sources.insert(key, scores);
    }

    if let Some(event_gate) = config
        .event_gate
        .as_ref()
        .filter(|gate| !gate.active_regimes.is_empty())
    {
        let event_scores =
            load_event_gate_scores_for_dates_cached(pool, cache, event_gate, &score_days).await?;
        for scores in score_sources.values_mut() {
            apply_event_gate_scores_for_regime(scores, event_scores.as_ref(), event_gate, |day| {
                let returns =
                    trailing_market_returns(benchmark_returns.as_ref(), day, policy.lookback_days);
                classify_market_regime(&returns, policy)
            });
        }
    }

    let all_symbols: Vec<String> = score_sources
        .values()
        .flat_map(|scores_by_date| scores_by_date.values())
        .flat_map(|v| v.iter().map(|(s, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let scores_by_score_day: FactorScoresByDate = score_days
        .iter()
        .filter_map(|score_day| {
            let returns = trailing_market_returns(
                benchmark_returns.as_ref(),
                *score_day,
                policy.lookback_days,
            );
            let regime = classify_market_regime(&returns, policy);
            let active_config = policy.apply(config, regime);
            score_rows_for_active_config(&score_sources, *score_day, &active_config)
                .map(|scores| (*score_day, scores))
        })
        .collect();
    let prefer_return_risk_stats_matrices = snapshot_scope
        .map(MarketFeatureSnapshotScope::prefer_return_risk_stats_cache)
        .unwrap_or(false);
    let mut return_history = None;
    let return_risk_stats_matrices = if let Some(snapshot_scope) =
        snapshot_scope.filter(|_| prefer_return_risk_stats_matrices)
    {
        let loaded_return_history = load_symbol_return_history_for_snapshot_scope_cached(
            pool,
            cache,
            Some(snapshot_scope),
            &all_symbols,
            start_date,
            end_date,
            max_lookback,
        )
        .await?;
        load_portfolio_return_risk_stats_feature_matrices_cached(
            pool,
            cache,
            Some(snapshot_scope.data_version_id.as_str()),
            &all_symbols,
            start_date,
            end_date,
            &score_days,
            &portfolio_config,
            loaded_return_history.as_ref(),
            &scores_by_score_day,
            config,
        )
        .await
        .map(|matrices| {
            return_history = Some(loaded_return_history);
            matrices
        })?
    } else {
        HashMap::new()
    };
    let return_risk_stats_matrices_loaded = return_risk_stats_matrices_cover_required_lookbacks(
        &return_risk_stats_matrices,
        &portfolio_config,
    );
    let return_risk_matrices = if snapshot_scope.is_some()
        && should_load_raw_return_risk_matrices(
            prefer_return_risk_stats_matrices,
            return_risk_stats_matrices_loaded,
        ) {
        let snapshot_scope = snapshot_scope.expect("snapshot scope checked");
        load_portfolio_return_risk_feature_matrices_cached(
            pool,
            cache,
            Some(snapshot_scope.data_version_id.as_str()),
            &all_symbols,
            start_date,
            end_date,
            &score_days,
            &portfolio_config,
            None,
        )
        .await?
    } else {
        HashMap::new()
    };
    let return_history = if let Some(return_history) = return_history {
        return_history
    } else if return_risk_stats_matrices_loaded
        || return_risk_matrices_cover_required_lookbacks(&return_risk_matrices, &portfolio_config)
    {
        Arc::new(HashMap::new())
    } else {
        load_symbol_return_history_for_snapshot_scope_cached(
            pool,
            cache,
            snapshot_scope,
            &all_symbols,
            start_date,
            end_date,
            max_lookback,
        )
        .await?
    };
    let average_amounts = load_portfolio_capacity_inputs_cached(
        pool,
        cache,
        snapshot_scope.map(|scope| scope.data_version_id.as_str()),
        &all_symbols,
        start_date,
        end_date,
        &score_days,
        &portfolio_config,
    )
    .await?;
    if let Some(snapshot_scope) = snapshot_scope {
        cache.insert_market_feature_snapshot_from_cached_histories(
            snapshot_scope.snapshot_key(max_lookback, &all_symbols),
            max_lookback,
            PIT_CAPACITY_AVERAGE_AMOUNT_LOOKBACK_DAYS,
            &all_symbols,
            start_date,
            end_date,
        );
    }
    let industry_by_symbol =
        load_portfolio_industry_inputs_cached(pool, cache, &all_symbols, &portfolio_config).await?;

    build_rebalance_factor_signals_with_score_selector_and_return_risk_matrices(
        trading_days.as_ref(),
        config,
        return_history.as_ref(),
        average_amounts.as_ref(),
        industry_by_symbol.as_ref(),
        &return_risk_matrices,
        &return_risk_stats_matrices,
        prefer_return_risk_stats_matrices,
        |score_day, active_config| {
            score_rows_for_active_config(&score_sources, score_day, active_config)
        },
        |day, base| {
            let returns =
                trailing_market_returns(benchmark_returns.as_ref(), day, policy.lookback_days);
            let regime = classify_market_regime(&returns, policy);
            policy.apply(base, regime)
        },
    )
}

pub(crate) fn regime_score_source_configs(
    base_config: &SignalConfig,
    policy: &MarketRegimePolicy,
) -> Vec<SignalConfig> {
    let mut configs = Vec::new();
    let mut seen = HashSet::new();
    for config in std::iter::once(base_config.clone()).chain(
        [
            MarketRegime::Bull,
            MarketRegime::Bear,
            MarketRegime::HighVolatility,
            MarketRegime::Sideways,
            MarketRegime::Mixed,
        ]
        .into_iter()
        .map(|regime| policy.apply(base_config, regime)),
    ) {
        let key = FactorScoreSourceKey::from_config(&config);
        if seen.insert(key) {
            configs.push(config.clone());
        }
        if let Some(overlay) = config.score_overlay.as_ref() {
            let overlay_config = score_source_config_for_overlay(&config, overlay);
            let key = FactorScoreSourceKey::from_config(&overlay_config);
            if seen.insert(key) {
                configs.push(overlay_config);
            }
        }
        if let Some(sleeve) = config.portfolio_sleeve.as_ref() {
            let sleeve_config = score_source_config_for_portfolio_sleeve(&config, sleeve);
            let key = FactorScoreSourceKey::from_config(&sleeve_config);
            if seen.insert(key) {
                configs.push(sleeve_config);
            }
        }
    }
    configs
}

pub(crate) fn score_source_configs(base_config: &SignalConfig) -> Vec<SignalConfig> {
    let mut configs = Vec::new();
    let mut seen = HashSet::new();
    let key = FactorScoreSourceKey::from_config(base_config);
    if seen.insert(key) {
        configs.push(base_config.clone());
    }
    if let Some(overlay) = base_config.score_overlay.as_ref() {
        let overlay_config = score_source_config_for_overlay(base_config, overlay);
        let key = FactorScoreSourceKey::from_config(&overlay_config);
        if seen.insert(key) {
            configs.push(overlay_config);
        }
    }
    if let Some(sleeve) = base_config.portfolio_sleeve.as_ref() {
        let sleeve_config = score_source_config_for_portfolio_sleeve(base_config, sleeve);
        let key = FactorScoreSourceKey::from_config(&sleeve_config);
        if seen.insert(key) {
            configs.push(sleeve_config);
        }
    }
    configs
}

pub(crate) fn score_rows_for_active_config(
    score_sources: &HashMap<FactorScoreSourceKey, FactorScoresByDate>,
    score_day: NaiveDate,
    active_config: &SignalConfig,
) -> Option<Vec<(String, f64)>> {
    let base_key = FactorScoreSourceKey::from_config(active_config);
    let base_rows = score_sources.get(&base_key)?.get(&score_day)?.clone();
    let Some(overlay) = active_config.score_overlay.as_ref() else {
        return Some(base_rows);
    };
    let overlay_config = score_source_config_for_overlay(active_config, overlay);
    let overlay_key = FactorScoreSourceKey::from_config(&overlay_config);
    let Some(overlay_rows) = score_sources
        .get(&overlay_key)
        .and_then(|scores_by_date| scores_by_date.get(&score_day))
        .cloned()
    else {
        return Some(base_rows);
    };
    if overlay_rows.is_empty() {
        return Some(base_rows);
    }

    Some(blend_factor_overlay_scores(
        base_rows,
        overlay_rows,
        active_config.score_direction,
        overlay.score_direction,
        overlay.weight,
    ))
}

pub(crate) fn score_source_config_for_overlay(
    base_config: &SignalConfig,
    overlay: &FactorScoreOverlayConfig,
) -> SignalConfig {
    let mut config = base_config.clone();
    config.combo_name = overlay.combo_name.clone();
    config.version = overlay.version.clone();
    config.score_direction = overlay.score_direction;
    config.score_overlay = None;
    config.portfolio_sleeve = None;
    config
}

pub(crate) fn score_source_config_for_portfolio_sleeve(
    base_config: &SignalConfig,
    sleeve: &FactorPortfolioSleeveConfig,
) -> SignalConfig {
    let mut config = base_config.clone();
    config.combo_name = sleeve.combo_name.clone();
    config.version = sleeve.version.clone();
    config.score_direction = sleeve.score_direction;
    config.score_overlay = None;
    config.portfolio_sleeve = None;
    config
}

fn blend_factor_overlay_scores(
    base_rows: Vec<(String, f64)>,
    overlay_rows: Vec<(String, f64)>,
    base_direction: ScoreDirection,
    overlay_direction: ScoreDirection,
    overlay_weight: f64,
) -> Vec<(String, f64)> {
    let overlay_weight = overlay_weight.clamp(0.0, 1.0);
    if overlay_weight <= f64::EPSILON {
        return base_rows;
    }
    let base_weight = 1.0 - overlay_weight;
    let base_stats = score_stats(base_rows.iter().map(|(_, score)| *score));
    let overlay_stats = score_stats(overlay_rows.iter().map(|(_, score)| *score));
    let overlay_scores: HashMap<String, f64> = overlay_rows
        .into_iter()
        .filter(|(_, score)| score.is_finite())
        .map(|(symbol, score)| {
            (
                symbol,
                oriented_standard_score(score, overlay_stats, overlay_direction),
            )
        })
        .collect();

    base_rows
        .into_iter()
        .filter(|(_, score)| score.is_finite())
        .map(|(symbol, score)| {
            let base_good = oriented_standard_score(score, base_stats, base_direction);
            let overlay_good = overlay_scores.get(&symbol).copied().unwrap_or(0.0);
            let blended_good = base_weight * base_good + overlay_weight * overlay_good;
            let blended_score = match base_direction {
                ScoreDirection::Descending => blended_good,
                ScoreDirection::Ascending => -blended_good,
            };
            (symbol, blended_score)
        })
        .collect()
}

pub(crate) fn oriented_standard_score(value: f64, stats: (f64, f64), direction: ScoreDirection) -> f64 {
    let score = standard_score(value, stats);
    match direction {
        ScoreDirection::Descending => score,
        ScoreDirection::Ascending => -score,
    }
}

pub(crate) async fn load_regime_base_scores_for_dates_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    config: &SignalConfig,
    score_days: &[NaiveDate],
) -> Result<FactorScoresByDate, String> {
    let score_cache = load_combo_scores_for_dates_cached(pool, cache, config, score_days).await?;
    let mut adjusted_scores = score_cache.as_ref().clone();
    let score_span = date_span(score_days);

    if config.min_daily_amount_cny.is_some() {
        if let Some((start_date, end_date)) = score_span {
            apply_factor_liquidity_filter(
                pool,
                cache,
                &mut adjusted_scores,
                config,
                start_date,
                end_date,
            )
            .await?;
        }
    }
    if let Some(blend) = config.prediction_blend.as_ref() {
        if let Some((start_date, end_date)) = score_span {
            let prediction_scores = load_prediction_scores_by_date_cached(
                pool,
                cache,
                &blend.prediction_set_id,
                start_date,
                end_date,
            )
            .await?;
            blend_factor_prediction_scores(
                &mut adjusted_scores,
                prediction_scores.as_ref(),
                blend,
                config.score_direction,
            );
        }
    }
    if let Some(event_gate) = config
        .event_gate
        .as_ref()
        .filter(|gate| gate.active_regimes.is_empty())
    {
        let event_scores =
            load_event_gate_scores_for_dates_cached(pool, cache, event_gate, score_days).await?;
        apply_event_gate_scores(&mut adjusted_scores, event_scores.as_ref(), event_gate);
    }

    Ok(adjusted_scores)
}

/// Generate daily strategy signals from persisted model predictions.
///
/// This mirrors factor-combo signal timing: a signal generated on day D uses
/// scores from the previous trading day by default, then the runner executes it
/// on the next trading day.
pub async fn generate_prediction_signals(
    pool: &PgPool,
    config: &PredictionSignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let prediction_start_date = prediction_load_start_date(start_date, config.entry_delay_days);
    let rows: Vec<(String, NaiveDate, f64, Option<i32>)> = sqlx::query_as(
        "SELECT symbol, trade_date, score, rank
         FROM model_prediction
         WHERE prediction_set_id = $1
           AND trade_date >= $2 AND trade_date <= $3
           AND available_at <= trade_date
         ORDER BY trade_date, rank NULLS LAST, score DESC, symbol",
    )
    .bind(&config.prediction_set_id)
    .bind(prediction_start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load model predictions: {}", e))?;

    if rows.is_empty() {
        return Err("No model predictions found".into());
    }

    let prediction_rows = rows
        .into_iter()
        .map(|(symbol, trade_date, score, rank)| PredictionScoreRow {
            symbol,
            trade_date,
            score,
            rank,
        })
        .collect();

    build_prediction_signals_from_rows(pool, config, start_date, end_date, prediction_rows).await
}

pub(crate) fn prediction_load_start_date(start_date: NaiveDate, entry_delay_days: usize) -> NaiveDate {
    let calendar_buffer_days = 30 + (entry_delay_days as i64 * 3);
    start_date - chrono::Duration::days(calendar_buffer_days)
}

async fn build_prediction_signals_from_rows(
    pool: &PgPool,
    config: &PredictionSignalConfig,
    start_date: NaiveDate,
    end_date: NaiveDate,
    rows: Vec<PredictionScoreRow>,
) -> Result<HashMap<NaiveDate, StrategySignal>, String> {
    let mut scores_by_date: HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>> = HashMap::new();
    for row in rows {
        if row.score.is_finite() {
            scores_by_date
                .entry(row.trade_date)
                .or_default()
                .push((row.symbol, row.score, row.rank));
        }
    }

    sort_prediction_scores(&mut scores_by_date, config.score_direction);
    apply_prediction_liquidity_filter(pool, &mut scores_by_date, config, start_date, end_date)
        .await?;

    let trading_days = load_open_trading_days(pool, start_date, end_date).await?;
    let all_symbols: Vec<String> = scores_by_date
        .values()
        .flat_map(|v| v.iter().map(|(s, _, _)| s.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let return_history = load_symbol_return_history(
        pool,
        &all_symbols,
        start_date,
        end_date,
        portfolio_history_lookback_days(&PortfolioConstructionConfig::from(config)),
    )
    .await?;
    let average_amounts = load_portfolio_capacity_inputs(
        pool,
        &all_symbols,
        start_date,
        end_date,
        &trading_days,
        &PortfolioConstructionConfig::from(config),
    )
    .await?;
    let industry_by_symbol = load_portfolio_industry_inputs(
        pool,
        &all_symbols,
        &PortfolioConstructionConfig::from(config),
    )
    .await?;

    build_rebalance_prediction_signals(
        &trading_days,
        &scores_by_date,
        config,
        &return_history,
        &average_amounts,
        &industry_by_symbol,
    )
}

pub(crate) fn sort_prediction_scores(
    scores_by_date: &mut HashMap<NaiveDate, Vec<(String, f64, Option<i32>)>>,
    direction: ScoreDirection,
) {
    for items in scores_by_date.values_mut() {
        items.sort_by(|left, right| {
            let score_order = match direction {
                ScoreDirection::Descending => right.1.partial_cmp(&left.1),
                ScoreDirection::Ascending => left.1.partial_cmp(&right.1),
            }
            .unwrap_or(std::cmp::Ordering::Equal);
            score_order
                .then_with(|| left.2.unwrap_or(i32::MAX).cmp(&right.2.unwrap_or(i32::MAX)))
                .then_with(|| left.0.cmp(&right.0))
        });
    }
}

pub(crate) fn sort_factor_scores(items: &mut [(String, f64)], direction: ScoreDirection) {
    items.sort_by(|a, b| {
        let score_order = match direction {
            ScoreDirection::Descending => b.1.partial_cmp(&a.1),
            ScoreDirection::Ascending => a.1.partial_cmp(&b.1),
        }
        .unwrap_or(std::cmp::Ordering::Equal);
        score_order.then_with(|| a.0.cmp(&b.0))
    });
}

async fn load_prediction_scores_by_date(
    pool: &PgPool,
    prediction_set_id: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<PredictionScoresByDate, String> {
    let rows: Vec<(String, NaiveDate, f64, Option<i32>)> = sqlx::query_as(
        "SELECT symbol, trade_date, score, rank
         FROM model_prediction
         WHERE prediction_set_id = $1
           AND trade_date >= $2 AND trade_date <= $3
           AND available_at <= trade_date
         ORDER BY trade_date, symbol",
    )
    .bind(prediction_set_id)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load model predictions for blend: {}", e))?;

    if rows.is_empty() {
        return Err(format!(
            "No model predictions found for blend prediction_set_id={}",
            prediction_set_id
        ));
    }

    let mut scores_by_date = HashMap::new();
    for (symbol, trade_date, score, rank) in rows {
        if score.is_finite() {
            scores_by_date
                .entry(trade_date)
                .or_insert_with(Vec::new)
                .push((symbol, score, rank));
        }
    }
    Ok(scores_by_date)
}

async fn load_prediction_scores_by_date_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    prediction_set_id: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Arc<PredictionScoresByDate>, String> {
    let key = SignalDataCacheKey::prediction_scores(prediction_set_id, start_date, end_date);
    if let Some(scores) = cache.cached_prediction_scores(&key) {
        return Ok(scores);
    }

    let scores =
        load_prediction_scores_by_date(pool, prediction_set_id, start_date, end_date).await?;
    Ok(cache.insert_prediction_scores(key, scores))
}

pub(crate) fn blend_factor_prediction_scores(
    factor_scores: &mut FactorScoresByDate,
    prediction_scores: &PredictionScoresByDate,
    blend: &PredictionBlendConfig,
    score_direction: ScoreDirection,
) {
    let factor_weight = blend.factor_weight.max(0.0);
    let prediction_weight = blend.prediction_weight.max(0.0);
    let gross_weight = factor_weight + prediction_weight;
    if gross_weight <= f64::EPSILON {
        return;
    }
    let factor_weight = factor_weight / gross_weight;
    let prediction_weight = prediction_weight / gross_weight;

    factor_scores.retain(|date, rows| {
        let Some(predictions) = prediction_scores.get(date) else {
            return false;
        };
        let prediction_percentiles = prediction_percentiles_by_symbol(
            predictions
                .iter()
                .map(|(symbol, score, _)| (symbol.as_str(), *score)),
        );
        let min_prediction_percentile = blend
            .prediction_min_percentile
            .map(|value| value.clamp(0.0, 1.0));
        let min_prediction_score = blend.prediction_min_score;
        let paired = rows
            .iter()
            .filter_map(|(symbol, factor_score)| {
                let (prediction_score, prediction_percentile) =
                    prediction_percentiles.get(symbol.as_str())?;
                if min_prediction_percentile
                    .map(|threshold| *prediction_percentile < threshold)
                    .unwrap_or(false)
                {
                    return None;
                }
                if min_prediction_score
                    .map(|threshold| *prediction_score < threshold)
                    .unwrap_or(false)
                {
                    return None;
                }
                if factor_score.is_finite() && prediction_score.is_finite() {
                    Some((symbol.clone(), *factor_score, *prediction_score))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if paired.is_empty() {
            return false;
        }

        let factor_stats = score_stats(paired.iter().map(|(_, score, _)| *score));
        let prediction_stats = score_stats(paired.iter().map(|(_, _, score)| *score));
        *rows = paired
            .into_iter()
            .map(|(symbol, factor_score, prediction_score)| {
                let prediction_score = standard_score(prediction_score, prediction_stats);
                let prediction_score = match score_direction {
                    ScoreDirection::Descending => prediction_score,
                    ScoreDirection::Ascending => -prediction_score,
                };
                let score = factor_weight * standard_score(factor_score, factor_stats)
                    + prediction_weight * prediction_score;
                (symbol, score)
            })
            .collect();
        true
    });
}

pub(crate) async fn load_event_gate_scores_for_dates_cached(
    pool: &PgPool,
    cache: &mut SignalDataCache,
    event_gate: &EventGateConfig,
    score_days: &[NaiveDate],
) -> Result<Arc<FactorScoresByDate>, String> {
    let mut gate_config = SignalConfig {
        combo_name: event_gate.combo_name.clone(),
        version: event_gate.version.clone(),
        score_direction: event_gate.score_direction,
        score_candidate_pool_size: None,
        universe_profile: TradableUniverseProfile::All,
        event_gate: None,
        prediction_blend: None,
        ..Default::default()
    };
    gate_config.min_daily_amount_cny = None;
    load_combo_scores_for_dates_cached(pool, cache, &gate_config, score_days).await
}

pub(crate) fn apply_event_gate_scores(
    factor_scores: &mut FactorScoresByDate,
    event_scores: &FactorScoresByDate,
    gate: &EventGateConfig,
) {
    apply_event_gate_scores_when(factor_scores, event_scores, gate, |_| true);
}

pub(crate) fn apply_event_gate_scores_for_regime<F>(
    factor_scores: &mut FactorScoresByDate,
    event_scores: &FactorScoresByDate,
    gate: &EventGateConfig,
    regime_for_date: F,
) where
    F: Fn(NaiveDate) -> MarketRegime,
{
    apply_event_gate_scores_when(factor_scores, event_scores, gate, |date| {
        gate.active_regimes.is_empty() || gate.active_regimes.contains(&regime_for_date(date))
    });
}

pub(crate) fn apply_event_gate_scores_when<F>(
    factor_scores: &mut FactorScoresByDate,
    event_scores: &FactorScoresByDate,
    gate: &EventGateConfig,
    active_for_date: F,
) where
    F: Fn(NaiveDate) -> bool,
{
    let min_score = if gate.min_score.is_finite() {
        gate.min_score
    } else {
        0.0
    };
    let boost_weight = gate.boost_weight.max(0.0);
    factor_scores.retain(|date, rows| {
        if !active_for_date(*date) {
            return true;
        }
        let event_by_symbol = event_scores
            .get(date)
            .map(|scores| {
                scores
                    .iter()
                    .filter(|(_, score)| score.is_finite())
                    .map(|(symbol, score)| (symbol.as_str(), *score))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        match gate.mode {
            EventGateMode::BoostPositive => {
                let event_stats = score_stats(event_by_symbol.values().copied());
                for (symbol, factor_score) in rows.iter_mut() {
                    let Some(event_score) = event_by_symbol.get(symbol.as_str()).copied() else {
                        continue;
                    };
                    if event_score > min_score && boost_weight > 0.0 {
                        *factor_score += boost_weight * standard_score(event_score, event_stats);
                    }
                }
            }
            EventGateMode::ExcludeNegative => {
                rows.retain(|(symbol, _)| {
                    event_by_symbol
                        .get(symbol.as_str())
                        .map(|score| *score >= min_score)
                        .unwrap_or(true)
                });
            }
            EventGateMode::RequirePositive => {
                rows.retain(|(symbol, _)| {
                    event_by_symbol
                        .get(symbol.as_str())
                        .map(|score| *score > min_score)
                        .unwrap_or(false)
                });
            }
        }
        !rows.is_empty()
    });
}

fn prediction_percentiles_by_symbol<'a>(
    predictions: impl Iterator<Item = (&'a str, f64)>,
) -> HashMap<&'a str, (f64, f64)> {
    let mut finite = predictions
        .filter(|(_, score)| score.is_finite())
        .collect::<Vec<_>>();
    finite.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let denominator = finite.len().saturating_sub(1).max(1) as f64;
    finite
        .into_iter()
        .enumerate()
        .map(|(index, (symbol, score))| (symbol, (score, index as f64 / denominator)))
        .collect()
}

pub(crate) fn score_stats(values: impl Iterator<Item = f64>) -> (f64, f64) {
    let values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.is_empty() {
        return (0.0, 1.0);
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / values.len().max(1) as f64;
    let std_dev = variance.sqrt();
    (mean, if std_dev > f64::EPSILON { std_dev } else { 1.0 })
}

pub(crate) fn standard_score(value: f64, (mean, std_dev): (f64, f64)) -> f64 {
    (value - mean) / std_dev
}
