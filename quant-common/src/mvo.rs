//! Multi-asset Mean-Variance Optimization (MVO) allocator.
//!
//! PIT-compliant: all computations use only past data (expanding window).
//! Uses Ledoit-Wolf shrinkage for stable covariance estimates.
//! Also supports analytical nonlinear shrinkage (Ledoit-Wolf 2017).
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

// ── Eigenvalue Decomposition (Jacobi method for symmetric matrices) ──────

/// Result of symmetric eigenvalue decomposition.
/// eigenvalues are sorted descending, eigenvectors are columns of `vectors`.
#[derive(Debug, Clone)]
struct EigenDecomp {
    eigenvalues: Vec<f64>,
    eigenvectors: Array2<f64>,
}

/// Jacobi eigenvalue decomposition for symmetric N×N matrix.
/// Efficient for N ≤ 10 (our MVO use case).
fn sym_eigen_decomp(matrix: &Array2<f64>) -> EigenDecomp {
    let n = matrix.ncols();
    assert_eq!(matrix.nrows(), n, "matrix must be square");

    // Initialize eigenvectors to identity
    let mut eigvecs = Array2::eye(n);
    let mut eigvals = matrix.clone();

    let max_iter = 100;
    let tol = 1e-12;

    for _iter in 0..max_iter {
        // Find max off-diagonal element
        let mut max_off = 0.0f64;
        let mut p = 0usize;
        let mut q = 1usize;
        for i in 0..n {
            for j in (i + 1)..n {
                let abs_val = eigvals[(i, j)].abs();
                if abs_val > max_off {
                    max_off = abs_val;
                    p = i;
                    q = j;
                }
            }
        }

        if max_off < tol {
            break;
        }

        // Compute Jacobi rotation
        let theta = if (eigvals[(p, p)] - eigvals[(q, q)]).abs() < 1e-15 {
            std::f64::consts::FRAC_PI_4
        } else {
            0.5 * (2.0 * eigvals[(p, q)] / (eigvals[(p, p)] - eigvals[(q, q)])).atan()
        };

        let c = theta.cos();
        let s = theta.sin();

        // Apply rotation: A' = J^T * A * J
        let mut new_vals = eigvals.clone();

        // Update rows/columns p and q
        for i in 0..n {
            if i != p && i != q {
                let a_ip = eigvals[(i, p)];
                let a_iq = eigvals[(i, q)];
                new_vals[(i, p)] = c * a_ip - s * a_iq;
                new_vals[(p, i)] = new_vals[(i, p)];
                new_vals[(i, q)] = s * a_ip + c * a_iq;
                new_vals[(q, i)] = new_vals[(i, q)];
            }
        }
        new_vals[(p, p)] = c * c * eigvals[(p, p)] + s * s * eigvals[(q, q)]
            - 2.0 * s * c * eigvals[(p, q)];
        new_vals[(q, q)] = s * s * eigvals[(p, p)] + c * c * eigvals[(q, q)]
            + 2.0 * s * c * eigvals[(p, q)];
        new_vals[(p, q)] = (c * c - s * s) * eigvals[(p, q)]
            + s * c * (eigvals[(p, p)] - eigvals[(q, q)]);
        new_vals[(q, p)] = new_vals[(p, q)];

        eigvals = new_vals;

        // Update eigenvectors: V' = V * J
        let mut new_vecs = Array2::zeros((n, n));
        for i in 0..n {
            for j in 0..n {
                if j == p {
                    new_vecs[(i, j)] = c * eigvecs[(i, p)] + s * eigvecs[(i, q)];
                } else if j == q {
                    new_vecs[(i, j)] = -s * eigvecs[(i, p)] + c * eigvecs[(i, q)];
                } else {
                    new_vecs[(i, j)] = eigvecs[(i, j)];
                }
            }
        }
        eigvecs = new_vecs;
    }

    // Extract eigenvalues from diagonal, sort descending
    let mut ev_pairs: Vec<(f64, Vec<f64>)> = (0..n)
        .map(|i| (eigvals[(i, i)], eigvecs.column(i).to_vec()))
        .collect();
    ev_pairs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let eigenvalues: Vec<f64> = ev_pairs.iter().map(|(v, _)| *v).collect();
    let mut eigenvectors = Array2::zeros((n, n));
    for (j, (_, vec)) in ev_pairs.iter().enumerate() {
        for i in 0..n {
            eigenvectors[(i, j)] = vec[i];
        }
    }

    EigenDecomp {
        eigenvalues,
        eigenvectors,
    }
}

// ── Nonlinear Shrinkage (Ledoit-Wolf 2017) ──────────────────────────────

/// Kernel density estimator using Epanechnikov kernel.
fn epanechnikov_kernel(x: f64) -> f64 {
    if x.abs() <= 1.0 {
        0.75 * (1.0 - x * x)
    } else {
        0.0
    }
}

/// Silverman's rule-of-thumb bandwidth for kernel density estimation.
fn silverman_bandwidth(values: &[f64]) -> f64 {
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let std_dev = (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n).sqrt();

    // Silverman: h = 0.9 * min(σ, IQR/1.34) * n^(-1/5)
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q1 = sorted[(n * 0.25) as usize];
    let q3 = sorted[(n * 0.75) as usize];
    let iqr = q3 - q1;

    let sigma = std_dev.min(iqr / 1.34).max(1e-10);
    0.9 * sigma * n.powf(-0.2)
}

/// Analytical Nonlinear Shrinkage (Ledoit & Wolf 2017, JFE).
///
/// Unlike linear LW shrinkage (same shrinkage intensity for all eigenvalues),
/// nonlinear shrinkage individually shrinks each eigenvalue:
/// - Large eigenvalues (signal/diversification) → less shrinkage
/// - Small eigenvalues (noise) → more shrinkage
///
/// Formula: d_i* = λ_i / ((1-c-cλ_i·Ĥ(λ_i))² + (π·c·λ_i·f̂(λ_i))²)
/// where c = N/T, f̂ = kernel density, Ĥ = kernel-smoothed Hilbert transform.
///
/// `returns`: T×N matrix (rows = time, cols = assets)
/// Returns the nonlinearly-shrunk covariance matrix.
pub fn nonlinear_shrinkage(returns: &Array2<f64>) -> Array2<f64> {
    let sample_cov = sample_covariance(returns);
    let n_assets = sample_cov.ncols();
    let n_periods = returns.nrows();

    if n_assets < 2 || n_periods < n_assets + 2 {
        // Fall back to linear LW for degenerate cases
        return ledoit_wolf_shrinkage(returns);
    }

    // 1. Convert to correlation matrix for standardized shrinkage
    let mut volatilities = Vec::with_capacity(n_assets);
    let mut corr = Array2::zeros((n_assets, n_assets));
    for i in 0..n_assets {
        let vol = sample_cov[(i, i)].sqrt();
        volatilities.push(vol);
    }
    for i in 0..n_assets {
        for j in 0..n_assets {
            if volatilities[i] > 0.0 && volatilities[j] > 0.0 {
                corr[(i, j)] = sample_cov[(i, j)] / (volatilities[i] * volatilities[j]);
            }
        }
    }

    // 2. Eigendecompose correlation matrix
    let decomp = sym_eigen_decomp(&corr);
    let lambda = &decomp.eigenvalues;
    let n = n_assets as f64;
    let t = n_periods as f64;
    let c = n / t;

    // 3. Kernel bandwidth
    let h = silverman_bandwidth(lambda);

    // 4. Shrink each eigenvalue individually
    let mut shrunk_lambda = vec![0.0f64; n_assets];
    for k in 0..n_assets {
        let lk = lambda[k];

        // Kernel density estimate at λ_k
        let mut f_hat = 0.0;
        for j in 0..n_assets {
            let x = (lambda[j] - lk) / h;
            f_hat += epanechnikov_kernel(x);
        }
        f_hat /= n * h;

        // Kernel-smoothed Hilbert transform: Ĥ(λ) = (1/N) Σ K_h(λ_j - λ) * (λ - λ_j)
        // Using the relationship: Hilbert transform = convolution with 1/x
        // Kernel-smoothed version: Ĥ(λ_k) ≈ (1/N) Σ_{j≠k} (λ_k - λ_j)/((λ_k - λ_j)² + ε²)
        let eps = h * 0.01; // regularization to avoid singularity
        let mut h_hat = 0.0;
        for j in 0..n_assets {
            if j != k {
                let diff = lk - lambda[j];
                h_hat += diff / (diff * diff + eps * eps);
            }
        }
        h_hat /= n;

        // 5. Analytical nonlinear shrinkage formula
        let denom = (1.0 - c - c * lk * h_hat).powi(2)
            + (std::f64::consts::PI * c * lk * f_hat).powi(2);

        if denom > 1e-15 {
            shrunk_lambda[k] = lk / denom;
        } else {
            shrunk_lambda[k] = lk; // fallback: no shrinkage
        }

        // Clamp to prevent negative or extreme eigenvalues
        shrunk_lambda[k] = shrunk_lambda[k].clamp(1e-6, 10.0);
    }

    // 6. Ensure eigenvalues sum = N (preserve trace of correlation matrix)
    let sum_orig: f64 = lambda.iter().sum();
    let sum_shrunk: f64 = shrunk_lambda.iter().sum();
    if sum_shrunk > 0.0 {
        let scale = sum_orig / sum_shrunk;
        for v in shrunk_lambda.iter_mut() {
            *v *= scale;
        }
    }

    // 7. Reconstruct shrunk correlation matrix: R_nl = U * diag(λ*) * U^T
    let u = &decomp.eigenvectors;
    let mut r_nl = Array2::zeros((n_assets, n_assets));
    for i in 0..n_assets {
        for j in 0..n_assets {
            let mut sum = 0.0;
            for k in 0..n_assets {
                sum += u[(i, k)] * shrunk_lambda[k] * u[(j, k)];
            }
            r_nl[(i, j)] = sum;
        }
    }

    // 8. Ensure proper correlation matrix: clamp diagonal to ~1
    for i in 0..n_assets {
        r_nl[(i, i)] = r_nl[(i, i)].clamp(0.9, 1.1);
    }

    // 9. Convert back to covariance: S_nl[i][j] = R_nl[i][j] * σ_i * σ_j
    let mut cov_nl = Array2::zeros((n_assets, n_assets));
    for i in 0..n_assets {
        for j in 0..n_assets {
            cov_nl[(i, j)] = r_nl[(i, j)] * volatilities[i] * volatilities[j];
        }
    }

    cov_nl
}

/// RMT-based eigenvalue filtering covariance.
///
/// Uses Random Matrix Theory: eigenvalues within the Marčenko-Pastur
/// bounds are considered "noise" and replaced by their average.
/// Only eigenvalues above the upper MP bound are kept as "signal."
///
/// This is a simpler alternative to full nonlinear shrinkage,
/// well-suited for portfolios where most eigenvalues represent noise.
pub fn rmt_filtered_covariance(returns: &Array2<f64>) -> Array2<f64> {
    let sample_cov = sample_covariance(returns);
    let n_assets = sample_cov.ncols();
    let n_periods = returns.nrows();

    if n_assets < 2 || n_periods < n_assets + 2 {
        return ledoit_wolf_shrinkage(returns);
    }

    let decomp = sym_eigen_decomp(&sample_cov);
    let lambda = &decomp.eigenvalues;
    let n = n_assets as f64;
    let t = n_periods as f64;
    let c = n / t;

    // Marčenko-Pastur upper bound: λ_+ = σ²(1 + √c)²
    // σ² ≈ mean of eigenvalues (trace/N)
    let sigma_sq = lambda.iter().sum::<f64>() / n;
    let mp_upper = sigma_sq * (1.0 + c.sqrt()).powi(2);

    // Separate signal (λ > MP upper) and noise (λ ≤ MP upper)
    let noise_avg: f64 = lambda.iter()
        .filter(|&&lv| lv <= mp_upper)
        .sum::<f64>()
        / lambda.iter().filter(|&&lv| lv <= mp_upper).count().max(1) as f64;

    // Replace noise eigenvalues with their average, keep signal
    let shrunk_lambda: Vec<f64> = lambda.iter()
        .map(|&lv| if lv > mp_upper { lv } else { noise_avg })
        .collect();

    let u = &decomp.eigenvectors;
    let mut cov_rmt = Array2::zeros((n_assets, n_assets));
    for i in 0..n_assets {
        for j in 0..n_assets {
            let mut sum = 0.0;
            for k in 0..n_assets {
                sum += u[(i, k)] * shrunk_lambda[k] * u[(j, k)];
            }
            cov_rmt[(i, j)] = sum;
        }
    }

    cov_rmt
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

/// Compute EWMA (exponentially weighted) covariance matrix.
/// Recent observations have higher weight: w_t = (1-λ) * λ^(T-1-t) / (1-λ^T)
/// `lambda`: decay factor, typically 0.94 (RiskMetrics) or 0.97
pub fn ewma_covariance(returns: &Array2<f64>, lambda: f64) -> Array2<f64> {
    let n_assets = returns.ncols();
    let n_periods = returns.nrows();
    let mut cov = Array2::zeros((n_assets, n_assets));
    if n_periods < 2 { return cov; }
    let means = returns.mean_axis(Axis(0)).unwrap();
    let centered = returns - &means;
    // Compute weights: normalized exponential decay
    let mut weights = Vec::with_capacity(n_periods);
    let mut w_sum = 0.0;
    for t in 0..n_periods {
        let w = lambda.powi((n_periods - 1 - t) as i32);
        weights.push(w);
        w_sum += w;
    }
    for t in 0..n_periods {
        let row = centered.row(t);
        let w = weights[t] / w_sum;
        for i in 0..n_assets {
            for j in 0..n_assets {
                cov[(i, j)] += w * row[i] * row[j];
            }
        }
    }
    // Bias correction
    let scale = 1.0 / (1.0 - w_sum * w_sum / (w_sum * w_sum));
    cov * scale
}

/// MVO with custom expected returns (momentum-adjusted mu) + LW covariance.
pub fn mvo_allocate_with_custom_mu(
    monthly_returns: &Array2<f64>,
    custom_mu: &Array1<f64>,
    min_stock: f64,
    return_target: f64,
    grid_step: f64,
) -> Option<MvoWeights> {
    let cov = ledoit_wolf_shrinkage(monthly_returns);
    let n_assets = monthly_returns.ncols();
    let rho = (n_assets as f64 / monthly_returns.nrows() as f64).clamp(0.0, 1.0);
    let result = grid_search_n_asset(custom_mu, &cov, min_stock, MAX_SINGLE, GridObjective::MinVariance { target: return_target }, grid_step);
    result.weights.map(|w| MvoWeights { weights: w, sharpe: result.best_sharpe, rho })
}

/// MVO with custom mu + NONLINEAR shrinkage covariance (Ledoit-Wolf 2017).
/// Otherwise identical to mvo_allocate_with_custom_mu.
pub fn mvo_allocate_with_custom_mu_nl(
    monthly_returns: &Array2<f64>,
    custom_mu: &Array1<f64>,
    min_stock: f64,
    return_target: f64,
    grid_step: f64,
) -> Option<MvoWeights> {
    let cov = nonlinear_shrinkage(monthly_returns);
    let n_assets = monthly_returns.ncols();
    let rho = (n_assets as f64 / monthly_returns.nrows() as f64).clamp(0.0, 1.0);
    let result = grid_search_n_asset(
        custom_mu, &cov, min_stock, MAX_SINGLE,
        GridObjective::MinVariance { target: return_target },
        grid_step,
    );
    result.weights.map(|w| MvoWeights { weights: w, sharpe: result.best_sharpe, rho })
}

/// MVO with custom mu + RMT eigenvalue filtering covariance.
/// Uses Random Matrix Theory to filter noise eigenvalues.
pub fn mvo_allocate_with_custom_mu_rmt(
    monthly_returns: &Array2<f64>,
    custom_mu: &Array1<f64>,
    min_stock: f64,
    return_target: f64,
    grid_step: f64,
) -> Option<MvoWeights> {
    let cov = rmt_filtered_covariance(monthly_returns);
    let n_assets = monthly_returns.ncols();
    let rho = (n_assets as f64 / monthly_returns.nrows() as f64).clamp(0.0, 1.0);
    let result = grid_search_n_asset(
        custom_mu, &cov, min_stock, MAX_SINGLE,
        GridObjective::MinVariance { target: return_target },
        grid_step,
    );
    result.weights.map(|w| MvoWeights { weights: w, sharpe: result.best_sharpe, rho })
}

/// Covariance estimation method for MVO.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CovMethod {
    /// Ledoit-Wolf linear shrinkage (default)
    LinearLW,
    /// Analytical nonlinear shrinkage (Ledoit-Wolf 2017)
    Nonlinear,
    /// RMT eigenvalue filtering (Marčenko-Pastur)
    RMT,
}

impl std::str::FromStr for CovMethod {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "linear" | "lw" | "ledoit_wolf" => Ok(CovMethod::LinearLW),
            "nonlinear" | "nl" | "nl_shrink" => Ok(CovMethod::Nonlinear),
            "rmt" | "rmt_filter" => Ok(CovMethod::RMT),
            _ => Err(format!("Unknown cov method: {} (use linear/nonlinear/rmt)", s)),
        }
    }
}

/// MVO with custom mu and configurable covariance method.
/// Routes to the appropriate shrinkage implementation.
pub fn mvo_allocate_with_cov_method(
    monthly_returns: &Array2<f64>,
    custom_mu: &Array1<f64>,
    min_stock: f64,
    return_target: f64,
    grid_step: f64,
    cov_method: CovMethod,
) -> Option<MvoWeights> {
    match cov_method {
        CovMethod::LinearLW => mvo_allocate_with_custom_mu(monthly_returns, custom_mu, min_stock, return_target, grid_step),
        CovMethod::Nonlinear => mvo_allocate_with_custom_mu_nl(monthly_returns, custom_mu, min_stock, return_target, grid_step),
        CovMethod::RMT => mvo_allocate_with_custom_mu_rmt(monthly_returns, custom_mu, min_stock, return_target, grid_step),
    }
}

/// MVO with EWMA covariance + Ledoit-Wolf shrinkage, configurable grid step.
/// Applies LW shrinkage on top of EWMA covariance for stability.
pub fn mvo_allocate_ewma_n(
    monthly_returns: &Array2<f64>,
    min_stock: f64,
    return_target: f64,
    grid_step: f64,
    lambda: f64,
) -> Option<MvoWeights> {
    // Apply LW shrinkage to EWMA covariance for stability
    let raw_cov = ewma_covariance(monthly_returns, lambda);
    let lw_cov = ledoit_wolf_shrinkage(monthly_returns);
    // Blend: 70% EWMA + 30% LW shrinkage (keep responsiveness while maintaining stability)
    let cov = 0.7 * &raw_cov + 0.3 * &lw_cov;
    let mu = annualized_returns(monthly_returns);
    let n_assets = monthly_returns.ncols();
    let rho = (n_assets as f64 / monthly_returns.nrows() as f64).clamp(0.0, 1.0);
    let result = grid_search_n_asset(&mu, &cov, min_stock, MAX_SINGLE, GridObjective::MinVariance { target: return_target }, grid_step);
    result.weights.map(|w| MvoWeights { weights: w, sharpe: result.best_sharpe, rho })
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
