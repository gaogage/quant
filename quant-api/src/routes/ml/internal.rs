use super::*;
use chrono::NaiveDate;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub(crate) enum MarketRegimeTag {
    Bull,
    Bear,
    Sideways,
}

impl MarketRegimeTag {
    pub(crate) fn from_benchmark_trailing(
        closes: &[(NaiveDate, f64)],
        trade_date: NaiveDate,
    ) -> Self {
        let feature = benchmark_trailing_regime_feature(closes, trade_date);
        if feature > 0.0 {
            Self::Bull
        } else if feature < 0.0 {
            Self::Bear
        } else {
            Self::Sideways
        }
    }
}

pub(crate) fn benchmark_trailing_regime_feature(
    benchmark_closes: &[(NaiveDate, f64)],
    trade_date: NaiveDate,
) -> f64 {
    let Some(current_idx) = benchmark_closes
        .iter()
        .position(|(date, close)| *date == trade_date && close.is_finite() && *close > 0.0)
    else {
        return 0.0;
    };
    let lookback_idx = current_idx.saturating_sub(60);
    let Some((_, lookback_close)) = benchmark_closes.get(lookback_idx) else {
        return 0.0;
    };
    let Some((_, current_close)) = benchmark_closes.get(current_idx) else {
        return 0.0;
    };
    if *lookback_close <= 0.0 {
        return 0.0;
    }
    let trailing_return = (current_close / lookback_close) - 1.0;
    (trailing_return / 0.10).clamp(-3.0, 3.0)
}

pub(crate) fn split_samples_by_regime(
    samples: &[TrainingSample],
) -> (
    Vec<TrainingSample>,
    Vec<TrainingSample>,
    Vec<TrainingSample>,
) {
    let mut bull = Vec::new();
    let mut bear = Vec::new();
    let mut sideways = Vec::new();
    for sample in samples {
        match sample.regime_tag {
            Some(MarketRegimeTag::Bull) => bull.push(sample.clone()),
            Some(MarketRegimeTag::Bear) => bear.push(sample.clone()),
            _ => sideways.push(sample.clone()),
        }
    }
    (bull, bear, sideways)
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RegimeSplitModel {
    pub(crate) bull: Option<NonlinearQuantileRanker>,
    pub(crate) bear: Option<NonlinearQuantileRanker>,
    pub(crate) sideways: Option<NonlinearQuantileRanker>,
    pub(crate) bull_samples: usize,
    pub(crate) bear_samples: usize,
    pub(crate) sideways_samples: usize,
    pub(crate) factor_count: usize,
}

impl RegimeSplitModel {
    pub(crate) fn score_row(&self, features: &[f64], regime: MarketRegimeTag) -> Option<f64> {
        let model = match regime {
            MarketRegimeTag::Bull => self.bull.as_ref(),
            MarketRegimeTag::Bear => self.bear.as_ref(),
            MarketRegimeTag::Sideways => self.sideways.as_ref(),
        };
        let model = model?;
        if features.len() != model.factor_count || features.iter().any(|v| !v.is_finite()) {
            return None;
        }
        let mut score = 0.0;
        for table in &model.tables {
            let feature = features[table.factor_idx];
            let bucket_idx = table.cutpoints.partition_point(|c| feature >= *c);
            let bucket_score = table
                .bucket_scores
                .get(bucket_idx)
                .copied()
                .unwrap_or(model.global_label_mean);
            score += bucket_score;
        }
        for ptable in &model.pairwise_tables {
            let bucket_i = model.tables[ptable.factor_i]
                .cutpoints
                .partition_point(|c| features[ptable.factor_i] >= *c)
                .min(model.bucket_count - 1);
            let bucket_j = model.tables[ptable.factor_j]
                .cutpoints
                .partition_point(|c| features[ptable.factor_j] >= *c)
                .min(model.bucket_count - 1);
            let pscore = ptable.scores[bucket_i * model.bucket_count + bucket_j];
            if pscore.is_finite() {
                score += pscore;
            }
        }
        if score.is_finite() {
            Some(score)
        } else {
            None
        }
    }

    pub(crate) fn any_model(&self) -> bool {
        self.bull.is_some() || self.bear.is_some() || self.sideways.is_some()
    }
}
