//! Combo score reuse plan and candidate pool filtering.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComboScoreReusePlan {
    RankedPool {
        requested_size: usize,
        requested_start: NaiveDate,
        requested_end: NaiveDate,
        source_span_days: i64,
    },
    UnboundedPool {
        requested_size: usize,
        score_direction: ScoreDirection,
        requested_start: NaiveDate,
        requested_end: NaiveDate,
        source_span_days: i64,
    },
    FullPool {
        requested_start: NaiveDate,
        requested_end: NaiveDate,
        source_span_days: i64,
    },
}

impl ComboScoreReusePlan {
pub(crate) fn source_span_days(self) -> i64 {
        match self {
            Self::RankedPool {
                source_span_days, ..
            }
            | Self::UnboundedPool {
                source_span_days, ..
            }
            | Self::FullPool {
                source_span_days, ..
            } => source_span_days,
        }
    }
}

pub(crate) fn reusable_combo_candidate_pool(
    candidate: &SignalDataCacheKey,
    requested: &SignalDataCacheKey,
) -> Option<ComboScoreReusePlan> {
    let (
        SignalDataCacheKey::ComboScores {
            combo_name: candidate_combo_name,
            version: candidate_version,
            start_date: candidate_start_date,
            end_date: candidate_end_date,
            score_direction: candidate_score_direction,
            score_candidate_pool_size: candidate_pool_size,
            universe_profile: candidate_universe_profile,
        },
        SignalDataCacheKey::ComboScores {
            combo_name: requested_combo_name,
            version: requested_version,
            start_date: requested_start_date,
            end_date: requested_end_date,
            score_direction: requested_score_direction,
            score_candidate_pool_size: requested_pool_size,
            universe_profile: requested_universe_profile,
        },
    ) = (candidate, requested)
    else {
        return None;
    };

    let same_score_source = candidate_combo_name == requested_combo_name
        && candidate_version == requested_version
        && candidate_universe_profile == requested_universe_profile;
    if !same_score_source {
        return None;
    }

    let covers_requested_window =
        candidate_start_date <= requested_start_date && candidate_end_date >= requested_end_date;
    if !covers_requested_window {
        return None;
    }
    let source_span_days = candidate_end_date
        .signed_duration_since(*candidate_start_date)
        .num_days();

    let Some(requested_pool_size) = requested_pool_size else {
        if candidate_pool_size.is_none() {
            return Some(ComboScoreReusePlan::FullPool {
                requested_start: *requested_start_date,
                requested_end: *requested_end_date,
                source_span_days,
            });
        }
        return None;
    };

    if let Some(candidate_pool_size) = candidate_pool_size {
        if candidate_score_direction == requested_score_direction
            && candidate_pool_size >= requested_pool_size
        {
            return Some(ComboScoreReusePlan::RankedPool {
                requested_size: *requested_pool_size,
                requested_start: *requested_start_date,
                requested_end: *requested_end_date,
                source_span_days,
            });
        }
    }

    if candidate_pool_size.is_none() {
        if let Some(score_direction) = requested_score_direction {
            return Some(ComboScoreReusePlan::UnboundedPool {
                requested_size: *requested_pool_size,
                score_direction: *score_direction,
                requested_start: *requested_start_date,
                requested_end: *requested_end_date,
                source_span_days,
            });
        }
    }

    None
}

pub(crate) fn filter_factor_scores_by_date(
    scores_by_date: &FactorScoresByDate,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> FactorScoresByDate {
    scores_by_date
        .iter()
        .filter_map(|(date, rows)| {
            if *date >= start_date && *date <= end_date {
                Some((*date, rows.clone()))
            } else {
                None
            }
        })
        .collect()
}

pub(crate) fn prune_factor_scores_by_date(
    scores_by_date: &FactorScoresByDate,
    requested_size: usize,
) -> FactorScoresByDate {
    scores_by_date
        .iter()
        .map(|(date, rows)| {
            (
                *date,
                rows.iter()
                    .take(requested_size)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

pub(crate) fn rank_and_prune_factor_scores_by_date(
    scores_by_date: &FactorScoresByDate,
    requested_size: usize,
    score_direction: ScoreDirection,
) -> FactorScoresByDate {
    scores_by_date
        .iter()
        .map(|(date, rows)| {
            let mut ranked = rows.clone();
            ranked.sort_by(|left, right| match score_direction {
                ScoreDirection::Descending => right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0)),
                ScoreDirection::Ascending => left
                    .1
                    .total_cmp(&right.1)
                    .then_with(|| left.0.cmp(&right.0)),
            });
            ranked.truncate(requested_size);
            (*date, ranked)
        })
        .collect()
}
