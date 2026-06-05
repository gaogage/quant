//! Multi-asset Mean-Variance Optimization (MVO) allocator.
//!
//! PIT-compliant: all computations use only past data (expanding window).
//! Uses Ledoit-Wolf shrinkage for stable covariance estimates.
//! Grid-search optimization with constraints (min stock weight, no shorting).

use ndarray::{Array1, Array2, Axis};

/// Compute sample covariance matrix from a T×N returns matrix.
/// `returns`: rows = time periods, columns = assets.
pub fn sample_covariance(returns: &Array2<f64>) -> Array2<f64> {
    let n_assets = returns.ncols();
    let n_periods = returns.nrows();
    if n_periods < 2 {
        return Array2::eye(n_assets);
    }

    let means = returns.mean_axis(Axis(0)).unwrap();
    let centered = returns - &means;

    // (X'X) / (T-1)
    let cov = centered.t().dot(&centered) / (n_periods as f64 - 1.0);
    cov
}

/// Ledoit-Wolf shrinkage estimator for covariance matrix.
/// Shrinks the sample covariance toward a diagonal target (constant correlation simplified).
/// Returns the shrunk covariance matrix.
pub fn ledoit_wolf_shrinkage(returns: &Array2<f64>) -> Array2<f64> {
    let sample_cov = sample_covariance(returns);
    let n_assets = sample_cov.ncols();
    let n_periods = returns.nrows();

    // Shrinkage target: diagonal matrix (variances only, zero covariances)
    let mut target = Array2::zeros((n_assets, n_assets));
    for i in 0..n_assets {
        target[(i, i)] = sample_cov[(i, i)];
    }

    // Shrinkage intensity: bounded by N/T (simplified Ledoit-Wolf)
    let rho = (n_assets as f64 / n_periods as f64).clamp(0.0, 1.0);

    // Shrunk covariance
    let shrunk = (1.0 - rho) * &sample_cov + rho * &target;
    shrunk
}

/// Compute downside semi-covariance matrix (only negative returns contribute).
/// For Sortino-ratio optimization: only penalizes downside co-movement.
/// Returns annualized semi-covariance (×12).
pub fn downside_semi_covariance(returns: &Array2<f64>) -> Array2<f64> {
    let n_assets = returns.ncols();
    let n_periods = returns.nrows();
    let mut semi_cov = Array2::zeros((n_assets, n_assets));
    if n_periods < 2 {
        return semi_cov;
    }

    // Center: use 0 as threshold (only negative deviations contribute)
    let means = returns.mean_axis(Axis(0)).unwrap();
    let centered = returns - &means;

    for t in 0..n_periods {
        let row = centered.row(t);
        // Only include periods where the portfolio return would be negative
        // Simplified: if ANY asset has negative centered return, contribute
        for i in 0..n_assets {
            for j in 0..n_assets {
                let ri = row[i];
                let rj = row[j];
                // Include if both centered returns are negative (downside co-movement)
                if ri < 0.0 && rj < 0.0 {
                    semi_cov[(i, j)] += ri * rj;
                }
            }
        }
    }
    semi_cov / (n_periods as f64 - 1.0) * 12.0
}

/// Compute annualized mean returns from a T×N monthly returns matrix.
pub fn annualized_returns(monthly_returns: &Array2<f64>) -> Array1<f64> {
    monthly_returns.mean_axis(Axis(0)).unwrap() * 12.0
}

/// MVO optimization result.
#[derive(Debug, Clone)]
pub struct MvoWeights {
    /// Optimal weights for each asset (sums to 1.0).
    pub weights: Array1<f64>,
    /// Expected annualized portfolio Sharpe ratio.
    pub sharpe: f64,
    /// Shrinkage intensity used.
    pub rho: f64,
}

// ── MVO Constants ──────────────────────────────────────────────

/// Risk-free rate (2% annual)
const RISK_FREE: f64 = 0.02;
/// Grid search step size (5%)
const GRID_STEP: f64 = 0.05;
/// Maximum single-asset allocation (75%)
const MAX_SINGLE: f64 = 0.75;
// ── Grid Search (shared by optimize_mvo and mvo_allocate_with_target) ──────

enum GridObjective {
    MaxSharpe,
    MinVariance { target: f64 },
    MaxSortino { target: f64 },
}

struct GridSearchResult {
    weights: Option<Array1<f64>>,
    best_sharpe: f64,
}

/// Single grid search implementation used by both optimize_mvo and mvo_allocate_with_target.
fn grid_search_5asset(
    mu_annual: &Array1<f64>,
    cov: &Array2<f64>,
    min_stock: f64,
    max_single: f64,
    objective: GridObjective,
) -> GridSearchResult {
    grid_search_n_asset(mu_annual, cov, min_stock, max_single, objective, GRID_STEP)
}

/// Generic N-asset grid search with configurable grid step.
/// Uses recursive enumeration for any number of assets.
/// For N assets: O(step_count^(N-1)) combinations.
/// Recommended grid_step: 5% for ≤5 assets, 10% for 6-8 assets.
fn grid_search_n_asset(
    mu_annual: &Array1<f64>,
    cov: &Array2<f64>,
    min_stock: f64,
    max_single: f64,
    objective: GridObjective,
    grid_step: f64,
) -> GridSearchResult {
    grid_search_n_asset_ext(mu_annual, cov, None, min_stock, max_single, objective, grid_step)
}

/// Extended grid search with optional downside semi-covariance for Sortino-max.
fn grid_search_n_asset_ext(
    mu_annual: &Array1<f64>,
    cov: &Array2<f64>,
    semi_cov: Option<&Array2<f64>>,
    min_stock: f64,
    max_single: f64,
    objective: GridObjective,
    grid_step: f64,
) -> GridSearchResult {
    let n_assets = mu_annual.len();
    if n_assets < 2 {
        return GridSearchResult { weights: None, best_sharpe: f64::NEG_INFINITY };
    }

    let steps = ((1.0 / grid_step) as i32) + 1;
    let step_values: Vec<f64> = (0..steps).map(|i| i as f64 * grid_step).collect();

    let mut best_sharpe = f64::NEG_INFINITY;
    let mut best_sortino = f64::NEG_INFINITY;
    let mut fallback_weights: Option<Array1<f64>> = None;
    let mut best_var = f64::INFINITY;
    let mut target_weights: Option<Array1<f64>> = None;
    let mut current = vec![0.0f64; n_assets];

    // Recursive search
    fn search_level(
        level: usize,
        n_assets: usize,
        remaining: f64,
        min_stock: f64,
        max_single: f64,
        step_values: &[f64],
        current: &mut [f64],
        mu_annual: &Array1<f64>,
        cov: &Array2<f64>,
        semi_cov: Option<&Array2<f64>>,
        best_sharpe: &mut f64,
        best_sortino: &mut f64,
        fallback_weights: &mut Option<Array1<f64>>,
        best_var: &mut f64,
        target_weights: &mut Option<Array1<f64>>,
        objective: &GridObjective,
    ) {
        if level == n_assets - 1 {
            let w_last = remaining.max(0.0);
            if w_last > max_single + 0.001 { return; }
            current[level] = w_last;

            let weights = Array1::from_vec(current.to_vec());
            let w_sum = weights.sum();
            if w_sum <= 0.0 { return; }
            let w = &weights / w_sum;

            let port_mu = w.dot(mu_annual) - RISK_FREE;
            let port_var = w.dot(&cov.dot(&w));
            if port_var <= 0.0 { return; }
            let sharpe = port_mu / port_var.sqrt();

            if sharpe > *best_sharpe {
                *best_sharpe = sharpe;
                *fallback_weights = Some(w.clone());
            }

            match objective {
                GridObjective::MaxSharpe => {
                    *target_weights = fallback_weights.clone();
                }
                GridObjective::MinVariance { target } => {
                    if port_mu >= target - RISK_FREE && port_var < *best_var {
                        *best_var = port_var;
                        *target_weights = Some(w);
                    }
                }
                GridObjective::MaxSortino { target } => {
                    if let Some(sc) = semi_cov {
                        let port_down_var = w.dot(&sc.dot(&w));
                        if port_down_var > 0.0 {
                            let sortino = (port_mu + RISK_FREE - *target) / port_down_var.sqrt();
                            if sortino > *best_sortino {
                                *best_sortino = sortino;
                                *target_weights = Some(w.clone());
                            }
                        } else if port_mu + RISK_FREE > *target {
                            // No downside at all — this is excellent
                            let sortino = (port_mu + RISK_FREE - *target) / port_var.sqrt();
                            if sortino > *best_sortino {
                                *best_sortino = sortino;
                                *target_weights = Some(w.clone());
                            }
                        }
                    }
                }
            }
            return;
        }

        let min_val = if level == 0 { min_stock.min(max_single) } else { 0.0 };
        let max_val = remaining.min(max_single);

        for &sv in step_values {
            if sv < min_val - 0.001 || sv > max_val + 0.001 { continue; }
            current[level] = sv;
            search_level(
                level + 1, n_assets, remaining - sv,
                min_stock, max_single, step_values, current,
                mu_annual, cov, semi_cov, best_sharpe, best_sortino,
                fallback_weights, best_var, target_weights, objective,
            );
        }
    }

    search_level(
        0, n_assets, 1.0,
        min_stock, max_single, &step_values, &mut current,
        mu_annual, cov, semi_cov, &mut best_sharpe, &mut best_sortino,
        &mut fallback_weights, &mut best_var, &mut target_weights, &objective,
    );

    GridSearchResult {
        weights: target_weights.or(fallback_weights),
        best_sharpe,
    }
}

/// Find optimal weights maximizing Sharpe ratio.
///
/// Constraints:
/// - `weights[0] >= min_stock` (stock asset is always first column)
/// - `weights[i] >= 0` for all i (no shorting)
/// - `sum(weights) == 1.0`
/// - `weights[i] <= 0.75` for any single asset
///
/// Uses grid search with 5% steps (efficient for ≤5 assets).
pub fn optimize_mvo(
    mu_annual: &Array1<f64>,
    cov: &Array2<f64>,
    min_stock: f64,
) -> Option<MvoWeights> {
    if mu_annual.len() < 2 {
        return None;
    }

    let result = grid_search_5asset(mu_annual, cov, min_stock, MAX_SINGLE, GridObjective::MaxSharpe);
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho: 0.0,
    })
}

/// Full MVO pipeline: given monthly returns, compute optimal weights.
pub fn mvo_allocate(
    monthly_returns: &Array2<f64>,
    min_stock: f64,
) -> Option<MvoWeights> {
    let cov = ledoit_wolf_shrinkage(monthly_returns);
    let mu = annualized_returns(monthly_returns);
    let n_assets = monthly_returns.ncols();
    let rho = (n_assets as f64 / monthly_returns.nrows() as f64).clamp(0.0, 1.0);

    optimize_mvo(&mu, &cov, min_stock).map(|mut result| {
        result.rho = rho;
        result
    })
}

/// MVO with return target: minimize variance subject to return >= target.
/// Falls back to max-Sharpe if target is unachievable.
pub fn mvo_allocate_with_target(
    monthly_returns: &Array2<f64>,
    min_stock: f64,
    return_target: f64,
) -> Option<MvoWeights> {
    let cov = ledoit_wolf_shrinkage(monthly_returns);
    let mu = annualized_returns(monthly_returns);
    let n_assets = monthly_returns.ncols();
    let rho = (n_assets as f64 / monthly_returns.nrows() as f64).clamp(0.0, 1.0);

    let result = grid_search_5asset(&mu, &cov, min_stock, MAX_SINGLE, GridObjective::MinVariance { target: return_target });
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho,
    })
}

/// MVO with Sortino-max objective: maximize Sortino ratio with target return.
/// Sortino = (expected_return - target_return) / downside_deviation.
/// Uses downside semi-covariance matrix for penalty — only downside co-movement counts.
pub fn mvo_allocate_sortino_n(
    monthly_returns: &Array2<f64>,
    min_stock: f64,
    sortino_target: f64,
    grid_step: f64,
) -> Option<MvoWeights> {
    let cov = ledoit_wolf_shrinkage(monthly_returns);
    let semi_cov = downside_semi_covariance(monthly_returns);
    let mu = annualized_returns(monthly_returns);
    let n_assets = monthly_returns.ncols();
    let rho = (n_assets as f64 / monthly_returns.nrows() as f64).clamp(0.0, 1.0);

    let result = grid_search_n_asset_ext(
        &mu, &cov, Some(&semi_cov),
        min_stock, MAX_SINGLE,
        GridObjective::MaxSortino { target: sortino_target },
        grid_step,
    );
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho,
    })
}

/// MVO with return target and configurable grid step (for N > 5 assets).
/// Uses recursive grid search with `grid_step` (e.g. 0.10 for 8-asset optimization).
pub fn mvo_allocate_with_target_n(
    monthly_returns: &Array2<f64>,
    min_stock: f64,
    return_target: f64,
    grid_step: f64,
) -> Option<MvoWeights> {
    let cov = ledoit_wolf_shrinkage(monthly_returns);
    let mu = annualized_returns(monthly_returns);
    let n_assets = monthly_returns.ncols();
    let rho = (n_assets as f64 / monthly_returns.nrows() as f64).clamp(0.0, 1.0);

    let result = grid_search_n_asset(&mu, &cov, min_stock, MAX_SINGLE, GridObjective::MinVariance { target: return_target }, grid_step);
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho,
    })
}

/// Compute monthly returns from daily prices for each asset.
///
/// `daily_prices`: Vec of (date_string, Vec<f64> prices for each asset)
/// Returns a T×N matrix of monthly returns.
pub fn monthly_returns_from_daily(
    daily_prices: &[(String, Vec<f64>)],
) -> Array2<f64> {
    let n_assets = daily_prices.first().map(|(_, p)| p.len()).unwrap_or(0);
    if n_assets == 0 || daily_prices.len() < 2 {
        return Array2::zeros((0, n_assets));
    }

    // Group by month, compute monthly returns
    let mut monthly_data: Vec<(String, Vec<f64>, Vec<f64>)> = Vec::new();
    // (month_key, first_prices, last_prices)

    let mut current_month: Option<(&str, Vec<f64>, Vec<f64>)> = None;
    // (month_key, first_prices, last_prices)

    for (date, prices) in daily_prices {
        let month_key = &date[..7]; // "2020-01"
        match &mut current_month {
            Some((m, first, last)) if *m == month_key => {
                *last = prices.clone();
            }
            _ => {
                // Save previous month
                if let Some((_, first, last)) = current_month.take() {
                    monthly_data.push((
                        String::new(), // placeholder
                        first,
                        last,
                    ));
                }
                current_month = Some((month_key, prices.clone(), prices.clone()));
            }
        }
    }
    // Don't forget last month
    if let Some((_, first, last)) = current_month {
        monthly_data.push((String::new(), first, last));
    }

    // Convert to returns matrix
    let n_months = monthly_data.len();
    let mut returns = Array2::zeros((n_months, n_assets));
    for (i, (_, first, last)) in monthly_data.iter().enumerate() {
        for j in 0..n_assets {
            if first[j] > 0.0 && last[j] > 0.0 {
                returns[(i, j)] = last[j] / first[j] - 1.0;
            }
        }
    }

    returns
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::arr2;

    #[test]
    fn test_sample_covariance() {
        // 2 assets, 3 periods
        let returns = arr2(&[
            [0.01, 0.02],
            [-0.01, 0.01],
            [0.02, -0.01],
        ]);
        let cov = sample_covariance(&returns);
        assert_eq!(cov.ncols(), 2);
        assert_eq!(cov.nrows(), 2);
        // Diagonal should be positive
        assert!(cov[(0, 0)] > 0.0);
        assert!(cov[(1, 1)] > 0.0);
    }

    #[test]
    fn test_ledoit_wolf_shrinkage() {
        let returns = arr2(&[
            [0.01, 0.02, 0.005, 0.01, 0.015],
            [-0.01, 0.01, 0.003, -0.005, 0.02],
            [0.02, -0.01, 0.004, 0.015, -0.01],
            [0.005, 0.015, 0.002, 0.01, 0.01],
            [-0.005, 0.005, 0.003, 0.005, 0.005],
        ]);
        let cov = ledoit_wolf_shrinkage(&returns);
        assert_eq!(cov.ncols(), 5);
        // Should be positive definite
        for i in 0..5 {
            assert!(cov[(i, i)] > 0.0, "zero variance for asset {}", i);
        }
    }

    #[test]
    fn test_optimize_mvo_basic() {
        let mu = Array1::from_vec(vec![0.15, 0.08, 0.03, 0.10, 0.12]);
        let cov = Array2::eye(5) * 0.04; // diagonal covariance
        let result = optimize_mvo(&mu, &cov, 0.50);
        assert!(result.is_some());
        let w = result.unwrap().weights;
        // Stock weight should be >= 50%
        assert!(w[0] >= 0.49);
        // Sum should be ~1.0
        assert!((w.sum() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_mvo_allocate_pipeline() {
        let returns = arr2(&[
            [0.01, 0.02, 0.005, 0.01, 0.015],
            [-0.01, 0.01, 0.003, -0.005, 0.02],
            [0.02, -0.01, 0.004, 0.015, -0.01],
            [0.005, 0.015, 0.002, 0.01, 0.01],
            [-0.005, 0.005, 0.003, 0.005, 0.005],
            [0.015, 0.01, 0.004, 0.02, 0.01],
            [0.01, -0.005, 0.002, 0.015, -0.005],
            [-0.02, 0.02, 0.001, -0.01, 0.025],
            [0.03, 0.005, 0.005, 0.02, 0.015],
            [0.01, 0.015, 0.003, 0.01, 0.02],
        ]);
        let result = mvo_allocate(&returns, 0.50);
        assert!(result.is_some());
        let w = result.unwrap().weights;
        assert!(w[0] >= 0.49, "stock weight too low: {}", w[0]);
        assert!((w.sum() - 1.0).abs() < 0.02, "weights don't sum to 1: {}", w.sum());
    }
}
