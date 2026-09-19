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
        new_vals[(p, p)] =
            c * c * eigvals[(p, p)] + s * s * eigvals[(q, q)] - 2.0 * s * c * eigvals[(p, q)];
        new_vals[(q, q)] =
            s * s * eigvals[(p, p)] + c * c * eigvals[(q, q)] + 2.0 * s * c * eigvals[(p, q)];
        new_vals[(p, q)] =
            (c * c - s * s) * eigvals[(p, q)] + s * c * (eigvals[(p, p)] - eigvals[(q, q)]);
        new_vals[(q, p)] = new_vals[(p, q)];

        eigvals = new_vals;

        // Update eigenvectors: V' = V * J（与上方 A' = J^T·A·J 的 J 同一约定，
        // J(p,q)=+s, J(q,p)=-s。此前符号相反（V·J^T）使特征向量旋转方向错误，
        // 谱收缩在错误基上进行）
        let mut new_vecs = Array2::zeros((n, n));
        for i in 0..n {
            for j in 0..n {
                if j == p {
                    new_vecs[(i, j)] = c * eigvecs[(i, p)] - s * eigvecs[(i, q)];
                } else if j == q {
                    new_vecs[(i, j)] = s * eigvecs[(i, p)] + c * eigvecs[(i, q)];
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
        for &lj in lambda {
            let x = (lj - lk) / h;
            f_hat += epanechnikov_kernel(x);
        }
        f_hat /= n * h;

        // Kernel-smoothed Hilbert transform: Ĥ(λ) = (1/N) Σ K_h(λ_j - λ) * (λ - λ_j)
        // Using the relationship: Hilbert transform = convolution with 1/x
        // Kernel-smoothed version: Ĥ(λ_k) ≈ (1/N) Σ_{j≠k} (λ_k - λ_j)/((λ_k - λ_j)² + ε²)
        let eps = h * 0.01; // regularization to avoid singularity
        let mut h_hat = 0.0;
        for (j, &lj) in lambda.iter().enumerate() {
            if j != k {
                let diff = lk - lj;
                h_hat += diff / (diff * diff + eps * eps);
            }
        }
        h_hat /= n;

        // 5. Analytical nonlinear shrinkage formula
        let denom =
            (1.0 - c - c * lk * h_hat).powi(2) + (std::f64::consts::PI * c * lk * f_hat).powi(2);

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

// ── Genetic Algorithm Optimizer ──────────────────────────────────────
//
// Grid Search degenerates with >7 assets because the search space grows as
// O(step_count^(N-1)). With 10% steps and 8 assets, that's 11^7 ≈ 19M
// combinations — barely tractable; with 9 assets it's 11^8 ≈ 214M.
//
// Genetic Algorithm (GA) scales linearly with population size regardless of
// asset count. It naturally handles simplex constraints (sum=1, wi≥0) and
// can find near-optimal solutions with far fewer evaluations than Grid Search.
//
// The GA uses:
//   - Tournament selection (size 3)
//   - Simulated Binary Crossover (SBX) for real-coded weights
//   - Gaussian mutation with adaptive re-normalization
//   - Elitism (top 2 individuals survive)
//   - Dirichlet initialization for uniform simplex coverage

use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

/// GA optimization parameters
struct GaParams {
    population_size: usize,
    generations: usize,
    crossover_prob: f64,
    mutation_prob: f64,
    mutation_scale: f64, // σ for Gaussian mutation
    elite_count: usize,
}

impl Default for GaParams {
    fn default() -> Self {
        Self {
            population_size: 500,
            generations: 200,
            crossover_prob: 0.8,
            mutation_prob: 0.3,
            mutation_scale: 0.05,
            elite_count: 3,
        }
    }
}

/// Generate a random weight vector uniformly distributed on the simplex.
fn random_simplex(n: usize, rng: &mut impl Rng) -> Vec<f64> {
    // Generate n exponential(1) random variates = -ln(U(0,1))
    let mut v: Vec<f64> = (0..n).map(|_| -rng.gen::<f64>().max(1e-12).ln()).collect();
    let s: f64 = v.iter().sum();
    v.iter_mut().for_each(|x| *x /= s);
    v
}

/// Generate a random weight vector respecting min/max constraints.
fn random_feasible_weights(
    n: usize,
    min_stock: f64,
    max_single: f64,
    rng: &mut impl Rng,
) -> Vec<f64> {
    // 可行性预检：min_stock > max_single 或 max_single·n < 1 时约束集为空，
    // 无条件重采样会无限循环（配置错误会让 GA 初始化挂死）。回退等权，
    // 由调用方后续 enforce_constraints 尽力钳制。
    if min_stock > max_single || max_single * (n as f64) < 1.0 - 1e-9 {
        let mut w = vec![1.0 / n as f64; n];
        enforce_constraints(&mut w, min_stock.min(max_single), max_single);
        return w;
    }
    // 有限尝试：随机采样多数情况几次内命中；失败则等权回退。
    // 返回前统一 enforce：采样内 w[0]=max(w0,min_stock) 后归一化会使 w[0]
    // 略低于 min_stock（sum>1 摊薄），enforce_constraints 补齐下限语义。
    for _ in 0..1000 {
        let mut w = random_simplex(n, rng);
        // Enforce min_stock on first asset (A股)
        w[0] = w[0].max(min_stock);
        // Check max_single constraint
        if w.iter().any(|&wi| wi > max_single + 1e-6) {
            continue;
        }
        // Re-normalize
        let s: f64 = w.iter().sum();
        if s > 0.0 {
            w.iter_mut().for_each(|x| *x /= s);
        }
        // Check max_single again after normalization
        if w.iter().any(|&wi| wi > max_single + 1e-6) {
            continue;
        }
        enforce_constraints(&mut w, min_stock, max_single);
        return w;
    }
    let mut w = vec![1.0 / n as f64; n];
    enforce_constraints(&mut w, min_stock, max_single);
    w
}

/// Tournament selection: pick k random individuals, return the best.
fn tournament_select(
    fitness: &[(Vec<f64>, f64)], // (weights, fitness), higher fitness = better
    k: usize,
    rng: &mut impl Rng,
) -> usize {
    let mut best_idx = 0usize;
    let mut best_fit = f64::NEG_INFINITY;
    for _ in 0..k {
        let idx = rng.gen_range(0..fitness.len());
        if fitness[idx].1 > best_fit {
            best_fit = fitness[idx].1;
            best_idx = idx;
        }
    }
    best_idx
}

/// Simulated Binary Crossover (SBX) for real-coded GA.
/// Produces two children from two parents with the given η (distribution index).
fn sbx_crossover(
    parent1: &[f64],
    parent2: &[f64],
    eta: f64,
    rng: &mut impl Rng,
) -> (Vec<f64>, Vec<f64>) {
    let n = parent1.len();
    let mut child1 = parent1.to_vec();
    let mut child2 = parent2.to_vec();

    for i in 0..n {
        if rng.gen::<f64>() > 0.5 {
            // 50% chance to crossover each gene
            continue;
        }
        let y1 = parent1[i].min(parent2[i]);
        let y2 = parent1[i].max(parent2[i]);
        if y2 - y1 < 1e-10 {
            continue;
        }
        let u = rng.gen::<f64>();
        let beta = if u <= 0.5 {
            (2.0 * u).powf(1.0 / (eta + 1.0))
        } else {
            (0.5 / (1.0 - u)).powf(1.0 / (eta + 1.0))
        };
        child1[i] = 0.5 * ((y1 + y2) - beta * (y2 - y1));
        child2[i] = 0.5 * ((y1 + y2) + beta * (y2 - y1));
    }
    (child1, child2)
}

/// Gaussian mutation with simplex re-normalization.
fn mutate(weights: &mut [f64], scale: f64, rng: &mut impl Rng) {
    let n = weights.len();
    for w in weights.iter_mut() {
        if rng.gen::<f64>() < 1.0 / n as f64 {
            let delta = rng.sample::<f64, _>(rand_distr::StandardNormal) * scale;
            let new_val = *w + delta;
            *w = new_val.clamp(0.0, 0.75);
        }
    }
    // Re-normalize to sum=1
    let s: f64 = weights.iter().sum();
    if s > 0.0 {
        for w in weights.iter_mut() {
            *w /= s;
        }
    }
}

/// Ensure weights satisfy constraints: sum=1, min_stock, max_single.
///
/// Clamp + re-normalize must iterate: a single normalize after clamping can push
/// already-capped weights back above max_single (e.g. [0.80,0.05] → normalize →
/// [0.94,0.06]). Repeat clamp→redistribute until the cap holds, distributing the
/// excess onto the uncapped assets (water-filling), so no single asset exceeds
/// max_single in the returned vector.
fn enforce_constraints(weights: &mut [f64], min_stock: f64, max_single: f64) {
    let n = weights.len();
    // No-shorting + initial normalize
    for w in weights.iter_mut() {
        if *w < 0.0 {
            *w = 0.0;
        }
    }
    let s: f64 = weights.iter().sum();
    if s > 0.0 {
        for w in weights.iter_mut() {
            *w /= s;
        }
    } else {
        for w in weights.iter_mut() {
            *w = 1.0 / n as f64;
        }
    }

    // Iteratively cap at max_single, redistributing excess to uncapped assets.
    // max_single * n >= 1 guarantees feasibility; bounded iterations as a backstop.
    for _ in 0..n + 2 {
        let mut excess = 0.0;
        let mut uncapped_sum = 0.0;
        for w in weights.iter() {
            if *w > max_single + 1e-9 {
                excess += *w - max_single;
            } else {
                uncapped_sum += *w;
            }
        }
        if excess <= 1e-9 {
            break;
        }
        for w in weights.iter_mut() {
            if *w > max_single + 1e-9 {
                *w = max_single;
            } else if uncapped_sum > 0.0 {
                *w += excess * (*w / uncapped_sum);
            }
        }
    }

    // Enforce min_stock on A股 (first asset); take the deficit pro-rata from others.
    if weights[0] < min_stock {
        let deficit = min_stock - weights[0];
        let others: f64 = weights[1..].iter().sum();
        weights[0] = min_stock;
        if others > 0.0 {
            for w in weights[1..].iter_mut() {
                *w -= deficit * (*w / others);
                if *w < 0.0 {
                    *w = 0.0;
                }
            }
        }
    }
}

/// Genetic Algorithm MVO optimization.
///
/// Supports two objective modes:
/// - MaxSharpe: maximize Sharpe ratio (portfolio return / portfolio volatility)
/// - MinVariance{target}: minimize portfolio variance subject to return ≥ target
///
/// Constraints:
/// - sum(weights) = 1
/// - weights[i] ≥ 0 (no shorting)
/// - weights[0] ≥ min_stock (A股 minimum)
/// - weights[i] ≤ max_single for all i
///
/// Returns the best weight vector found with its Sharpe ratio.
pub fn ga_optimize(
    mu_annual: &Array1<f64>,
    cov: &Array2<f64>,
    min_stock: f64,
    max_single: f64,
) -> Option<MvoWeights> {
    ga_optimize_with_params(
        mu_annual,
        cov,
        min_stock,
        max_single,
        &GaParams::default(),
        None,
    )
}

/// GA optimization with MinVariance objective (minimize variance subject to return ≥ target).
/// This matches the Grid Search MinVariance behavior but uses GA instead of brute force.
pub fn ga_optimize_min_variance(
    mu_annual: &Array1<f64>,
    cov: &Array2<f64>,
    min_stock: f64,
    max_single: f64,
    return_target: f64,
) -> Option<MvoWeights> {
    ga_optimize_with_params(
        mu_annual,
        cov,
        min_stock,
        max_single,
        &GaParams::default(),
        Some(return_target),
    )
}

fn ga_optimize_with_params(
    mu_annual: &Array1<f64>,
    cov: &Array2<f64>,
    min_stock: f64,
    max_single: f64,
    params: &GaParams,
    return_target: Option<f64>, // Some(target) = MinVariance mode, None = MaxSharpe mode
) -> Option<MvoWeights> {
    let n_assets = mu_annual.len();
    if n_assets < 2 {
        return None;
    }

    // 确定性种子：从输入(mu+cov对角)派生，保证同输入同结果(可复现)，
    // 不同调仓窗口因数据不同而独立。500×200 种群代数保证收敛到全局最优附近。
    let mut seed: u64 = 0x9E3779B97F4A7C15;
    for v in mu_annual.iter() {
        seed = seed
            .wrapping_mul(31)
            .wrapping_add(((v * 1e6) as i64) as u64);
    }
    for i in 0..n_assets {
        seed = seed
            .wrapping_mul(31)
            .wrapping_add(((cov[(i, i)] * 1e6) as i64) as u64);
    }
    let mut rng = StdRng::seed_from_u64(seed);

    // Fitness function: MinVariance or MaxSharpe depending on target
    let fitness = |w: &[f64]| -> f64 {
        let port_mu: f64 = w
            .iter()
            .zip(mu_annual.iter())
            .map(|(wi, mui)| wi * mui)
            .sum();
        let mut port_var = 0.0f64;
        for i in 0..n_assets {
            for j in 0..n_assets {
                port_var += w[i] * cov[(i, j)] * w[j];
            }
        }
        if port_var <= 0.0 {
            return f64::NEG_INFINITY;
        }
        if let Some(target) = return_target {
            // MinVariance: two-stage ranking (matching Grid Search logic)
            // Stage 1: Feasibility — portfolios meeting target always beat those that don't
            // Stage 2: Among feasible, minimize variance; among infeasible, minimize shortfall
            if port_mu >= target {
                // Feasible: higher fitness = lower variance. Range: [0, -0.1] typically
                -port_var
            } else {
                // Infeasible: ranked below ALL feasible, by shortfall magnitude
                // Worst feasible fitness ≈ -0.1, so use -1.0 base to separate
                -1.0 - (target - port_mu)
            }
        } else {
            // MaxSharpe mode。cov 为月度口径，×12 年化后再开方——此前直接
            // sqrt(月度方差) 使 sharpe 虚大 √12≈3.46 倍（排序不变，绝对值失真）。
            (port_mu - RISK_FREE) / (port_var * 12.0).sqrt()
        }
    };

    // Initialize population
    let mut population: Vec<Vec<f64>> = (0..params.population_size)
        .map(|_| random_feasible_weights(n_assets, min_stock, max_single, &mut rng))
        .collect();

    // Inject boundary portfolios to ensure exploration of extreme weights
    // (random simplex init tends to produce balanced portfolios, missing corners)
    population[0] = vec![1.0 / n_assets as f64; n_assets]; // equal weight
    enforce_constraints(&mut population[0], min_stock, max_single);
    // Max single-asset corner cases (each asset at max_single, rest evenly distributed)
    for i in 0..n_assets.min(params.population_size - 1) {
        let mut w = vec![(1.0 - max_single) / (n_assets - 1) as f64; n_assets];
        w[i] = max_single;
        enforce_constraints(&mut w, min_stock, max_single);
        if i + 1 < population.len() {
            population[i + 1] = w;
        }
    }

    let mut best_weights: Option<Vec<f64>> = None;
    let mut best_sharpe = f64::NEG_INFINITY;

    for gen in 0..params.generations {
        // Evaluate fitness
        let mut fitness: Vec<(Vec<f64>, f64)> =
            population.iter().map(|w| (w.clone(), fitness(w))).collect();

        // Sort by fitness descending
        fitness.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Track best
        if fitness[0].1 > best_sharpe {
            best_sharpe = fitness[0].1;
            best_weights = Some(fitness[0].0.clone());
        }

        // Check convergence: if top 10% have nearly identical fitness, stop early
        let top_n = (params.population_size / 10).max(2);
        let top_std = fitness[..top_n]
            .iter()
            .map(|(_, f)| (f - best_sharpe).powi(2))
            .sum::<f64>()
            / top_n as f64;
        if gen > 30 && top_std.sqrt() < 1e-6 {
            break;
        }

        // Elitism: keep best individuals
        let mut new_population: Vec<Vec<f64>> = fitness[..params.elite_count]
            .iter()
            .map(|(w, _)| w.clone())
            .collect();

        // Fill rest with selection + crossover + mutation
        while new_population.len() < params.population_size {
            let p1_idx = tournament_select(&fitness, 3, &mut rng);
            let p2_idx = tournament_select(&fitness, 3, &mut rng);

            let (mut child1, mut child2) = if rng.gen::<f64>() < params.crossover_prob {
                sbx_crossover(&fitness[p1_idx].0, &fitness[p2_idx].0, 2.0, &mut rng)
            } else {
                (fitness[p1_idx].0.clone(), fitness[p2_idx].0.clone())
            };

            if rng.gen::<f64>() < params.mutation_prob {
                mutate(&mut child1, params.mutation_scale, &mut rng);
            }
            if rng.gen::<f64>() < params.mutation_prob {
                mutate(&mut child2, params.mutation_scale, &mut rng);
            }

            enforce_constraints(&mut child1, min_stock, max_single);
            enforce_constraints(&mut child2, min_stock, max_single);

            new_population.push(child1);
            if new_population.len() < params.population_size {
                new_population.push(child2);
            }
        }

        population = new_population;
    }

    best_weights.map(|w| MvoWeights {
        weights: Array1::from_vec(w),
        sharpe: best_sharpe,
        rho: 0.0, // GA doesn't use shrinkage
    })
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
    let noise_avg: f64 = lambda.iter().filter(|&&lv| lv <= mp_upper).sum::<f64>()
        / lambda.iter().filter(|&&lv| lv <= mp_upper).count().max(1) as f64;

    // Replace noise eigenvalues with their average, keep signal
    let shrunk_lambda: Vec<f64> = lambda
        .iter()
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

    (1.0 - rho) * &sample_cov + rho * &target
}

/// Compute EWMA (exponentially weighted) covariance matrix.
/// Recent observations have higher weight: w_t = (1-λ) * λ^(T-1-t) / (1-λ^T)
/// `lambda`: decay factor, typically 0.94 (RiskMetrics) or 0.97
pub fn ewma_covariance(returns: &Array2<f64>, lambda: f64) -> Array2<f64> {
    let n_assets = returns.ncols();
    let n_periods = returns.nrows();
    let mut cov = Array2::zeros((n_assets, n_assets));
    if n_periods < 2 {
        return cov;
    }
    // lambda<=0 时权重退化为 [0,..,0,1]，归一化平方和=1 使偏差修正项 1/(1-Σw̃²)
    // 除零产出 inf/NaN；lambda>=1 无衰减意义。回退等权样本协方差。
    // 注意 0.0..1.0 的 Range 含 0.0，必须显式排除 lambda<=0。
    if lambda <= 0.0 || lambda >= 1.0 {
        return sample_covariance(returns);
    }
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
    // 归一化权重平方和（用于有限样本 bias correction）
    let w_sq_sum: f64 = weights.iter().map(|w| (w / w_sum).powi(2)).sum();
    for (row, &wt) in centered.rows().into_iter().zip(weights.iter()) {
        let w = wt / w_sum;
        for i in 0..n_assets {
            for j in 0..n_assets {
                cov[(i, j)] += w * row[i] * row[j];
            }
        }
    }
    // Bias correction: scale = 1 / (1 - Σw̃²)，w̃ 为归一化权重
    let scale = 1.0 / (1.0 - w_sq_sum);
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
    let result = grid_search_n_asset(
        custom_mu,
        &cov,
        min_stock,
        MAX_SINGLE,
        GridObjective::MinVariance {
            target: return_target,
        },
        grid_step,
    );
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho,
    })
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
        custom_mu,
        &cov,
        min_stock,
        MAX_SINGLE,
        GridObjective::MinVariance {
            target: return_target,
        },
        grid_step,
    );
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho,
    })
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
        custom_mu,
        &cov,
        min_stock,
        MAX_SINGLE,
        GridObjective::MinVariance {
            target: return_target,
        },
        grid_step,
    );
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho,
    })
}

/// MVO allocation using Genetic Algorithm + custom mu + LW covariance.
/// Uses MinVariance objective with return target (matching Grid Search behavior)
/// to avoid the over-aggressive portfolios of pure Sharpe maximization.
/// GA scales O(population × generations) regardless of asset count,
/// while Grid Search is O(grid_step^(N-1)).
pub fn mvo_allocate_ga(
    monthly_returns: &Array2<f64>,
    custom_mu: &Array1<f64>,
    min_stock: f64,
    return_target: f64,
    _grid_step: f64,
) -> Option<MvoWeights> {
    mvo_allocate_ga_with_max_single(
        monthly_returns,
        custom_mu,
        min_stock,
        return_target,
        _grid_step,
        MAX_SINGLE,
    )
}

/// GA MVO with configurable max_single constraint.
/// Allows adaptive max_single: higher (e.g. 80%) in bull markets for more concentration.
pub fn mvo_allocate_ga_with_max_single(
    monthly_returns: &Array2<f64>,
    custom_mu: &Array1<f64>,
    min_stock: f64,
    return_target: f64,
    _grid_step: f64,
    max_single: f64,
) -> Option<MvoWeights> {
    let cov = ledoit_wolf_shrinkage(monthly_returns);
    ga_optimize_min_variance(custom_mu, &cov, min_stock, max_single, return_target)
}

/// GA MVO with MaxSharpe objective + configurable max_single（最大化 Sharpe，非最小方差）。
/// 用于干净 ETF 数据下的目标函数对比实验。
pub fn mvo_allocate_ga_maxsharpe_with_max_single(
    monthly_returns: &Array2<f64>,
    custom_mu: &Array1<f64>,
    min_stock: f64,
    max_single: f64,
) -> Option<MvoWeights> {
    let cov = ledoit_wolf_shrinkage(monthly_returns);
    ga_optimize(custom_mu, &cov, min_stock, max_single)
}

/// GA MVO with nonlinear shrinkage covariance.
pub fn mvo_allocate_ga_nl(
    monthly_returns: &Array2<f64>,
    custom_mu: &Array1<f64>,
    min_stock: f64,
    _return_target: f64,
    _grid_step: f64,
) -> Option<MvoWeights> {
    let cov = nonlinear_shrinkage(monthly_returns);
    ga_optimize(custom_mu, &cov, min_stock, MAX_SINGLE)
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
            _ => Err(format!(
                "Unknown cov method: {} (use linear/nonlinear/rmt)",
                s
            )),
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
        CovMethod::LinearLW => mvo_allocate_with_custom_mu(
            monthly_returns,
            custom_mu,
            min_stock,
            return_target,
            grid_step,
        ),
        CovMethod::Nonlinear => mvo_allocate_with_custom_mu_nl(
            monthly_returns,
            custom_mu,
            min_stock,
            return_target,
            grid_step,
        ),
        CovMethod::RMT => mvo_allocate_with_custom_mu_rmt(
            monthly_returns,
            custom_mu,
            min_stock,
            return_target,
            grid_step,
        ),
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
    let result = grid_search_n_asset(
        &mu,
        &cov,
        min_stock,
        MAX_SINGLE,
        GridObjective::MinVariance {
            target: return_target,
        },
        grid_step,
    );
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho,
    })
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
    grid_search_n_asset_ext(
        mu_annual, cov, None, min_stock, max_single, objective, grid_step,
    )
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
        return GridSearchResult {
            weights: None,
            best_sharpe: f64::NEG_INFINITY,
        };
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
    // TODO(DDD Step 2): 16 参数是网格搜索建模债，应参数对象化（GridSearchContext +
    // 最优解收集器）；P0 恢复 strict 门禁阶段显式豁免，避免波及递归调用方。
    #[allow(clippy::too_many_arguments)]
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
            if w_last > max_single + 0.001 {
                return;
            }
            current[level] = w_last;

            let weights = Array1::from_vec(current.to_vec());
            let w_sum = weights.sum();
            if w_sum <= 0.0 {
                return;
            }
            let w = &weights / w_sum;

            let port_mu = w.dot(mu_annual) - RISK_FREE;
            let port_var = w.dot(&cov.dot(&w));
            if port_var <= 0.0 {
                return;
            }
            // cov 为月度口径，×12 年化对齐 mu（此前月度 σ 使 sharpe 虚大 √12 倍，
            // 排序不受影响——单调缩放，仅绝对值失真）。
            let sharpe = port_mu / (port_var * 12.0).sqrt();

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

        let min_val = if level == 0 {
            min_stock.min(max_single)
        } else {
            0.0
        };
        let max_val = remaining.min(max_single);

        for &sv in step_values {
            if sv < min_val - 0.001 || sv > max_val + 0.001 {
                continue;
            }
            current[level] = sv;
            search_level(
                level + 1,
                n_assets,
                remaining - sv,
                min_stock,
                max_single,
                step_values,
                current,
                mu_annual,
                cov,
                semi_cov,
                best_sharpe,
                best_sortino,
                fallback_weights,
                best_var,
                target_weights,
                objective,
            );
        }
    }

    search_level(
        0,
        n_assets,
        1.0,
        min_stock,
        max_single,
        &step_values,
        &mut current,
        mu_annual,
        cov,
        semi_cov,
        &mut best_sharpe,
        &mut best_sortino,
        &mut fallback_weights,
        &mut best_var,
        &mut target_weights,
        &objective,
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

    let result = grid_search_5asset(
        mu_annual,
        cov,
        min_stock,
        MAX_SINGLE,
        GridObjective::MaxSharpe,
    );
    result.weights.map(|w| MvoWeights {
        weights: w,
        sharpe: result.best_sharpe,
        rho: 0.0,
    })
}

/// Full MVO pipeline: given monthly returns, compute optimal weights.
pub fn mvo_allocate(monthly_returns: &Array2<f64>, min_stock: f64) -> Option<MvoWeights> {
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

    let result = grid_search_5asset(
        &mu,
        &cov,
        min_stock,
        MAX_SINGLE,
        GridObjective::MinVariance {
            target: return_target,
        },
    );
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
        &mu,
        &cov,
        Some(&semi_cov),
        min_stock,
        MAX_SINGLE,
        GridObjective::MaxSortino {
            target: sortino_target,
        },
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

    let result = grid_search_n_asset(
        &mu,
        &cov,
        min_stock,
        MAX_SINGLE,
        GridObjective::MinVariance {
            target: return_target,
        },
        grid_step,
    );
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
pub fn monthly_returns_from_daily(daily_prices: &[(String, Vec<f64>)]) -> Array2<f64> {
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
        let returns = arr2(&[[0.01, 0.02], [-0.01, 0.01], [0.02, -0.01]]);
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
        assert!(
            (w.sum() - 1.0).abs() < 0.02,
            "weights don't sum to 1: {}",
            w.sum()
        );
    }

    // ─── 协方差估计器 ───────────────────────────────────────────

    #[test]
    fn sample_covariance_single_period_falls_back_to_identity() {
        let returns = arr2(&[[0.01, 0.02]]);
        let cov = sample_covariance(&returns);
        assert_eq!(cov[(0, 0)], 1.0);
        assert_eq!(cov[(1, 1)], 1.0);
        assert_eq!(cov[(0, 1)], 0.0);
    }

    #[test]
    fn sample_covariance_matches_manual_computation() {
        // 2 期 2 资产：均值各 0，n-1=1 → cov = 逐元素外积和
        // cov00 = 0.01²+0.01² = 0.0002；cov11 = 0.02²+0.02² = 0.0008；
        // cov01 = 0.01·(-0.02)+(-0.01)·0.02 = -0.0004
        let returns = arr2(&[[0.01, -0.02], [-0.01, 0.02]]);
        let cov = sample_covariance(&returns);
        assert!((cov[(0, 0)] - 0.0002).abs() < 1e-12);
        assert!((cov[(1, 1)] - 0.0008).abs() < 1e-12);
        assert!((cov[(0, 1)] - (-0.0004)).abs() < 1e-12);
        // 对称
        assert_eq!(cov[(0, 1)], cov[(1, 0)]);
    }

    #[test]
    fn sym_eigen_decomp_diagonal_matrix_returns_sorted_eigenvalues() {
        let m = arr2(&[[3.0, 0.0], [0.0, 5.0]]);
        let d = sym_eigen_decomp(&m);
        assert!((d.eigenvalues[0] - 5.0).abs() < 1e-9);
        assert!((d.eigenvalues[1] - 3.0).abs() < 1e-9);
    }

    #[test]
    fn sym_eigen_decomp_known_2x2() {
        // [[2,1],[1,2]] 特征值 3 和 1
        let m = arr2(&[[2.0, 1.0], [1.0, 2.0]]);
        let d = sym_eigen_decomp(&m);
        assert!((d.eigenvalues[0] - 3.0).abs() < 1e-9);
        assert!((d.eigenvalues[1] - 1.0).abs() < 1e-9);
        // 重构 A = V·diag(λ)·V^T
        let mut recon = Array2::zeros((2, 2));
        for i in 0..2 {
            for j in 0..2 {
                let mut s = 0.0;
                for k in 0..2 {
                    s += d.eigenvectors[(i, k)] * d.eigenvalues[k] * d.eigenvectors[(j, k)];
                }
                recon[(i, j)] = s;
            }
        }
        assert!((recon[(0, 0)] - 2.0).abs() < 1e-9);
        assert!((recon[(0, 1)] - 1.0).abs() < 1e-9);
        assert!((recon[(1, 1)] - 2.0).abs() < 1e-9);
    }

    #[test]
    fn epanechnikov_kernel_boundaries() {
        assert!((epanechnikov_kernel(0.0) - 0.75).abs() < 1e-12);
        assert!((epanechnikov_kernel(1.0) - 0.0).abs() < 1e-12);
        assert_eq!(epanechnikov_kernel(-1.5), 0.0);
        assert_eq!(epanechnikov_kernel(2.0), 0.0);
    }

    #[test]
    fn silverman_bandwidth_identical_values_uses_epsilon_floor() {
        let h = silverman_bandwidth(&[1.0; 10]);
        assert!(h > 0.0 && h < 1e-8);
    }

    #[test]
    fn nonlinear_shrinkage_small_sample_falls_back_to_lw() {
        // n_periods(2) < n_assets(2)+2 → 回退线性 LW
        let returns = arr2(&[[0.01, 0.02], [-0.01, 0.01]]);
        let nl = nonlinear_shrinkage(&returns);
        let lw = ledoit_wolf_shrinkage(&returns);
        for i in 0..2 {
            for j in 0..2 {
                assert!((nl[(i, j)] - lw[(i, j)]).abs() < 1e-12);
            }
        }
    }

    #[test]
    fn nonlinear_shrinkage_output_symmetric_positive_diagonal() {
        // 24 期 × 3 资产（满足 n >= N+2）
        let returns = arr2(&[
            [0.01, 0.02, 0.005],
            [-0.01, 0.01, 0.003],
            [0.02, -0.01, 0.004],
            [0.005, 0.015, 0.002],
            [-0.005, 0.005, 0.003],
            [0.015, 0.01, 0.004],
            [0.01, -0.005, 0.002],
            [-0.02, 0.02, 0.001],
            [0.03, 0.005, 0.005],
            [0.01, 0.015, 0.003],
            [0.012, -0.008, 0.006],
            [-0.015, 0.018, 0.002],
            [0.025, 0.002, 0.007],
            [-0.008, -0.012, 0.001],
            [0.018, 0.011, 0.004],
            [0.004, 0.022, 0.005],
            [-0.011, 0.008, 0.002],
            [0.022, -0.002, 0.003],
            [0.008, 0.012, 0.006],
            [-0.018, 0.016, 0.001],
            [0.028, 0.006, 0.008],
            [0.006, 0.018, 0.004],
            [-0.006, -0.004, 0.002],
            [0.016, 0.009, 0.005],
        ]);
        let cov = nonlinear_shrinkage(&returns);
        // 对称
        for i in 0..3 {
            for j in 0..3 {
                assert!((cov[(i, j)] - cov[(j, i)]).abs() < 1e-9);
            }
        }
        // 对角为正（方差）
        for i in 0..3 {
            assert!(cov[(i, i)] > 0.0, "diag[{}] = {}", i, cov[(i, i)]);
        }
    }

    #[test]
    fn rmt_filtered_small_sample_falls_back_to_lw() {
        let returns = arr2(&[[0.01, 0.02], [-0.01, 0.01]]);
        let rmt = rmt_filtered_covariance(&returns);
        let lw = ledoit_wolf_shrinkage(&returns);
        for i in 0..2 {
            for j in 0..2 {
                assert!((rmt[(i, j)] - lw[(i, j)]).abs() < 1e-12);
            }
        }
    }

    #[test]
    fn rmt_filtered_reduces_noise_dispersion() {
        // 一个高波动"信号"资产 + 多个低波动"噪声"资产：
        // 过滤后噪声特征值离散度应不高于原始样本谱
        let mut rows = Vec::new();
        for t in 0..60 {
            let signal = if t % 2 == 0 { 0.08 } else { -0.08 };
            let noise_a = if t % 3 == 0 { 0.004 } else { -0.003 };
            let noise_b = if t % 4 == 0 { -0.005 } else { 0.0035 };
            rows.push([signal, noise_a, noise_b]);
        }
        let data = Array2::from(rows);
        let sample = sample_covariance(&data);
        let filtered = rmt_filtered_covariance(&data);
        let disp = |m: &Array2<f64>| {
            let d = sym_eigen_decomp(m);
            d.eigenvalues[0] - d.eigenvalues[d.eigenvalues.len() - 1]
        };
        assert!(
            disp(&filtered) <= disp(&sample) + 1e-12,
            "RMT 应压缩噪声谱离散度"
        );
    }

    #[test]
    fn ledoit_wolf_full_shrinkage_when_assets_ge_periods() {
        // N(3) >= T(3) → rho=1 → 纯对角目标
        let returns = arr2(&[
            [0.01, 0.02, 0.005],
            [-0.01, 0.01, 0.003],
            [0.02, -0.01, 0.004],
        ]);
        let cov = ledoit_wolf_shrinkage(&returns);
        assert!((cov[(0, 1)]).abs() < 1e-12, "非对角应为 0");
        assert!((cov[(1, 2)]).abs() < 1e-12, "非对角应为 0");
        let sample = sample_covariance(&returns);
        for i in 0..3 {
            assert!((cov[(i, i)] - sample[(i, i)]).abs() < 1e-12);
        }
    }

    #[test]
    fn ewma_covariance_lambda_near_one_approximates_sample() {
        let returns = arr2(&[[0.01, 0.02], [-0.01, 0.01], [0.02, -0.01], [0.005, 0.015]]);
        let ewma = ewma_covariance(&returns, 0.999);
        let sample = sample_covariance(&returns);
        for i in 0..2 {
            for j in 0..2 {
                assert!(
                    (ewma[(i, j)] - sample[(i, j)]).abs() < 5e-4,
                    "[{}][{}] ewma={} sample={}",
                    i,
                    j,
                    ewma[(i, j)],
                    sample[(i, j)]
                );
            }
        }
    }

    #[test]
    fn ewma_covariance_recency_weighting() {
        // 60 期：前 59 期零波动、末期暴涨 30%。λ=0.94 半衰期约 11 期，
        // 末期有效权重约 6%（等权仅 1/60）→ EWMA 方差应数倍于样本方差。
        // 短窗口（T<10）下 EWMA 权重近乎均匀，recency 效应体现不出来。
        let mut rows = vec![[0.0]; 60];
        rows[59] = [0.30];
        let returns = Array2::from(rows);
        let ewma = ewma_covariance(&returns, 0.94);
        let sample = sample_covariance(&returns);
        assert!(
            ewma[(0, 0)] > sample[(0, 0)] * 2.0,
            "ewma={} sample={}",
            ewma[(0, 0)],
            sample[(0, 0)]
        );
    }

    #[test]
    fn ewma_covariance_invalid_lambda_falls_back_to_sample() {
        let returns = arr2(&[[0.01, 0.02], [-0.01, 0.01]]);
        for lambda in [0.0, -0.5, 1.0, 1.5] {
            let cov = ewma_covariance(&returns, lambda);
            let sample = sample_covariance(&returns);
            assert!(
                (cov[(0, 0)] - sample[(0, 0)]).abs() < 1e-12,
                "lambda={} 应回退样本协方差",
                lambda
            );
        }
    }

    #[test]
    fn downside_semi_covariance_only_negative_comovements() {
        // 资产 A、B 完全负相关：centered 后永不同时为负 → 半协方差非对角为 0
        let returns = arr2(&[[0.01, -0.01], [-0.01, 0.01], [0.02, -0.02], [-0.02, 0.02]]);
        let sc = downside_semi_covariance(&returns);
        assert!(sc[(0, 1)].abs() < 1e-12);
        // 对角仍为正（各自有负偏离期）
        assert!(sc[(0, 0)] > 0.0);
        assert!(sc[(1, 1)] > 0.0);
    }

    #[test]
    fn downside_semi_covariance_short_input_returns_zero() {
        let sc = downside_semi_covariance(&arr2(&[[0.01, 0.02]]));
        assert_eq!(sc[(0, 0)], 0.0);
    }

    // ─── GA 基础组件 ───────────────────────────────────────────

    #[test]
    fn random_simplex_sums_to_one_non_negative() {
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..50 {
            let w = random_simplex(5, &mut rng);
            assert!((w.iter().sum::<f64>() - 1.0).abs() < 1e-12);
            assert!(w.iter().all(|&x| x >= 0.0));
        }
    }

    #[test]
    fn random_feasible_weights_infeasible_constraints_fallback_not_hang() {
        // min_stock > max_single：不可行约束集，历史上此函数无限循环
        let mut rng = StdRng::seed_from_u64(7);
        let w = random_feasible_weights(3, 0.8, 0.5, &mut rng);
        assert_eq!(w.len(), 3);
        // max_single*n < 1 同样不可行
        let w2 = random_feasible_weights(3, 0.0, 0.2, &mut rng);
        assert_eq!(w2.len(), 3);
    }

    #[test]
    fn enforce_constraints_caps_max_single_waterfilling() {
        // 归一化后单资产超 cap 的经典用例：[0.80, 0.05] → normalize → [0.94, 0.06]
        let mut w = vec![0.80, 0.05, 0.05, 0.05, 0.05];
        enforce_constraints(&mut w, 0.0, 0.30);
        assert!(
            w.iter().all(|&x| x <= 0.30 + 1e-9),
            "max_single 被突破: {:?}",
            w
        );
    }

    #[test]
    fn enforce_constraints_min_stock_floor() {
        let mut w = vec![0.10, 0.45, 0.45];
        enforce_constraints(&mut w, 0.30, 0.75);
        assert!(w[0] >= 0.30 - 1e-9, "w[0]={}", w[0]);
        assert!(w.iter().all(|&x| x >= 0.0));
    }

    #[test]
    fn enforce_constraints_all_zero_falls_back_equal_weight() {
        let mut w = vec![0.0, 0.0, 0.0, 0.0];
        enforce_constraints(&mut w, 0.0, 0.75);
        for &x in &w {
            assert!((x - 0.25).abs() < 1e-9);
        }
    }

    #[test]
    fn sbx_crossover_preserves_gene_pool_mean() {
        let mut rng = StdRng::seed_from_u64(3);
        let p1 = vec![0.2, 0.5, 0.1, 0.2];
        let p2 = vec![0.4, 0.1, 0.3, 0.2];
        let (c1, c2) = sbx_crossover(&p1, &p2, 2.0, &mut rng);
        // SBX 对称性：child1+child2 = parent1+parent2（逐基因）
        for i in 0..4 {
            assert!(
                ((c1[i] + c2[i]) - (p1[i] + p2[i])).abs() < 1e-9,
                "gene {} 均值被破坏",
                i
            );
        }
    }

    #[test]
    fn mutate_keeps_non_negative_and_sums_near_one() {
        let mut rng = StdRng::seed_from_u64(11);
        let mut w = vec![0.25; 4];
        for _ in 0..20 {
            mutate(&mut w, 0.05, &mut rng);
            assert!(w.iter().all(|&x| x >= 0.0));
            assert!((w.iter().sum::<f64>() - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn tournament_select_picks_best_of_sampled() {
        let mut rng = StdRng::seed_from_u64(5);
        let fitness = vec![(vec![1.0], 0.1), (vec![2.0], 0.9), (vec![3.0], 0.5)];
        // 锦标赛可重复抽样：选中者必为抽样内最优；20 次内几乎必然
        // 抽到过 0.9（全不中概率 (2/3)^60 ≈ 2e-11）
        let mut saw_best = false;
        for _ in 0..20 {
            let idx = tournament_select(&fitness, 3, &mut rng);
            let chosen = fitness[idx].1;
            assert!(chosen == 0.1 || chosen == 0.5 || chosen == 0.9);
            if chosen == 0.9 {
                saw_best = true;
            }
        }
        assert!(saw_best, "20 次锦标赛应至少一次选中全局最优");
    }

    // ─── GA 优化器 ─────────────────────────────────────────────

    #[test]
    fn ga_optimize_deterministic_and_respects_constraints() {
        let mu = Array1::from_vec(vec![0.20, 0.10, 0.05]);
        let cov = arr2(&[[0.04, 0.01, 0.0], [0.01, 0.02, 0.005], [0.0, 0.005, 0.01]]);
        let r1 = ga_optimize(&mu, &cov, 0.20, 0.60).expect("GA 应有解");
        let r2 = ga_optimize(&mu, &cov, 0.20, 0.60).expect("GA 应有解");
        // 确定性：同输入同输出（种子由输入派生）
        for i in 0..3 {
            assert!((r1.weights[i] - r2.weights[i]).abs() < 1e-12);
        }
        // 约束
        assert!(r1.weights[0] >= 0.20 - 1e-6);
        assert!(r1.weights.iter().all(|&x| x >= -1e-9));
        assert!(r1.weights.iter().all(|&x| x <= 0.60 + 1e-6));
        assert!((r1.weights.sum() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn ga_optimize_maxsharpe_uses_annualized_vol_scale() {
        // mu 年化、cov 月度对角：修复前 sharpe 虚大 √12。已知最优 w≈[1,0] 时
        // sharpe = (0.24-0.02)/sqrt(0.01*12) ≈ 0.635
        let mu = Array1::from_vec(vec![0.24, 0.0]);
        let cov = arr2(&[[0.01, 0.0], [0.0, 0.01]]);
        let r = ga_optimize(&mu, &cov, 0.0, 1.0).expect("GA 应有解");
        assert!(
            r.sharpe < 1.2,
            "年化口径修复后 sharpe 应≈0.635，实际 {}",
            r.sharpe
        );
        assert!(r.sharpe > 0.3, "sharpe={}", r.sharpe);
    }

    #[test]
    fn ga_optimize_min_variance_prefers_feasible_over_infeasible() {
        // 目标 8%：低收益资产组合无法达标（infeasible），高收益可行组合应胜出
        let mu = Array1::from_vec(vec![0.10, 0.02]);
        let cov = arr2(&[[0.02, 0.0], [0.0, 0.01]]);
        let r = ga_optimize_min_variance(&mu, &cov, 0.0, 0.90, 0.08).expect("GA 应有解");
        // 可行解 w·mu >= 0.08 → 资产 0 占比至少 75%
        let port_mu: f64 = r.weights.iter().zip(mu.iter()).map(|(w, m)| w * m).sum();
        assert!(port_mu >= 0.08 - 1e-6, "port_mu={}", port_mu);
        assert!((r.weights.sum() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn ga_optimize_single_asset_returns_none() {
        let mu = Array1::from_vec(vec![0.1]);
        let cov = arr2(&[[0.01]]);
        assert!(ga_optimize(&mu, &cov, 0.0, 1.0).is_none());
    }

    // ─── Grid Search 与入口路由 ────────────────────────────────

    #[test]
    fn cov_method_from_str_all_branches() {
        use std::str::FromStr;
        assert_eq!(CovMethod::from_str("linear").unwrap(), CovMethod::LinearLW);
        assert_eq!(CovMethod::from_str("LW").unwrap(), CovMethod::LinearLW);
        assert_eq!(
            CovMethod::from_str("ledoit_wolf").unwrap(),
            CovMethod::LinearLW
        );
        assert_eq!(CovMethod::from_str("nl").unwrap(), CovMethod::Nonlinear);
        assert_eq!(
            CovMethod::from_str("nonlinear").unwrap(),
            CovMethod::Nonlinear
        );
        assert_eq!(CovMethod::from_str("rmt").unwrap(), CovMethod::RMT);
        assert!(CovMethod::from_str("bogus").is_err());
    }

    fn sample_monthly_3asset() -> Array2<f64> {
        arr2(&[
            [0.02, 0.01, 0.005],
            [-0.01, 0.015, 0.003],
            [0.03, -0.008, 0.004],
            [0.005, 0.012, 0.002],
            [-0.004, 0.006, 0.003],
            [0.015, 0.01, 0.004],
            [0.01, -0.005, 0.002],
            [-0.02, 0.02, 0.001],
            [0.025, 0.005, 0.005],
            [0.012, 0.015, 0.003],
            [0.011, -0.008, 0.006],
            [-0.014, 0.018, 0.002],
            [0.022, 0.002, 0.007],
            [-0.008, -0.012, 0.001],
            [0.017, 0.011, 0.004],
        ])
    }

    #[test]
    fn mvo_allocate_with_target_family_produce_constrained_weights() {
        let data = sample_monthly_3asset();
        let min_stock = 0.20;

        let cases: Vec<Option<MvoWeights>> = vec![
            mvo_allocate_with_target(&data, min_stock, 0.02),
            mvo_allocate_with_target_n(&data, min_stock, 0.02, 0.10),
            mvo_allocate_sortino_n(&data, min_stock, 0.02, 0.10),
            mvo_allocate_ewma_n(&data, min_stock, 0.02, 0.10, 0.94),
        ];
        for (i, r) in cases.iter().enumerate() {
            let r = r.as_ref().unwrap_or_else(|| panic!("case {} 应有解", i));
            assert!(
                r.weights[0] >= min_stock - 0.02,
                "case {} w0={}",
                i,
                r.weights[0]
            );
            assert!(r.weights.iter().all(|&x| x >= -1e-9));
            assert!(r.weights.iter().all(|&x| x <= MAX_SINGLE + 1e-6));
            assert!((r.weights.sum() - 1.0).abs() < 0.02);
            assert!((0.0..=1.0).contains(&r.rho), "case {} rho={}", i, r.rho);
        }
    }

    #[test]
    fn mvo_allocate_with_target_unreachable_target_falls_back_to_maxsharpe() {
        let data = sample_monthly_3asset();
        // 年化 500% 目标不可达 → 应回退 max-Sharpe 权重（仍有解）
        let r = mvo_allocate_with_target(&data, 0.20, 5.0).expect("不可达 target 应回退");
        assert!((r.weights.sum() - 1.0).abs() < 0.02);
    }

    #[test]
    fn mvo_allocate_with_custom_mu_family_all_cov_methods() {
        let data = sample_monthly_3asset();
        let custom_mu = Array1::from_vec(vec![0.15, 0.08, 0.04]);

        let lw = mvo_allocate_with_custom_mu(&data, &custom_mu, 0.20, 0.02, 0.10);
        let nl = mvo_allocate_with_custom_mu_nl(&data, &custom_mu, 0.20, 0.02, 0.10);
        let rmt = mvo_allocate_with_custom_mu_rmt(&data, &custom_mu, 0.20, 0.02, 0.10);
        for (i, r) in [&lw, &nl, &rmt].iter().enumerate() {
            let r = r
                .as_ref()
                .unwrap_or_else(|| panic!("cov method {} 应有解", i));
            assert!(r.weights[0] >= 0.18, "method {} w0={}", i, r.weights[0]);
            assert!((r.weights.sum() - 1.0).abs() < 0.02);
        }

        // 路由一致性：with_cov_method 应与直接调用等价（全部三个分支）
        for (method, direct) in [
            (CovMethod::LinearLW, &lw),
            (CovMethod::Nonlinear, &nl),
            (CovMethod::RMT, &rmt),
        ] {
            let via_router =
                mvo_allocate_with_cov_method(&data, &custom_mu, 0.20, 0.02, 0.10, method)
                    .unwrap_or_else(|| panic!("router {:?} 应有解", method));
            let d = direct.as_ref().unwrap();
            for i in 0..3 {
                assert!(
                    (via_router.weights[i] - d.weights[i]).abs() < 1e-12,
                    "{:?} 路由不等价",
                    method
                );
            }
        }
    }

    #[test]
    fn ewma_covariance_single_period_returns_zero_matrix() {
        let cov = ewma_covariance(&arr2(&[[0.01, 0.02]]), 0.94);
        assert_eq!(cov[(0, 0)], 0.0);
        assert_eq!(cov[(1, 1)], 0.0);
    }

    #[test]
    fn ga_optimize_zero_covariance_falls_to_infeasible_ranking() {
        // 协方差全零 → MaxSharpe fitness 返回 NEG_INFINITY，GA 仍应返回
        // 结构合法的权重（不 panic、不 NaN）
        let mu = Array1::from_vec(vec![0.1, 0.05]);
        let cov = Array2::zeros((2, 2));
        let r = ga_optimize(&mu, &cov, 0.0, 1.0);
        if let Some(r) = r {
            assert!((r.weights.sum() - 1.0).abs() < 1e-6);
        }
    }

    #[test]
    fn enforce_constraints_large_min_stock_squeezes_others_to_zero() {
        // min_stock=0.9：其他资产被按比例压缩，可能触底 0（负值钳制分支）
        let mut w = vec![0.5, 0.25, 0.25];
        enforce_constraints(&mut w, 0.90, 0.95);
        assert!(w[0] >= 0.90 - 1e-9);
        assert!(w.iter().all(|&x| x >= 0.0));
    }

    #[test]
    fn random_feasible_weights_tight_max_single_resamples_or_falls_back() {
        // max_single=0.34（3 资产下界之上极紧）：多数采样会触发 continue 重采样；
        // 无论重采样命中或回退，返回结构必须合法且不挂起
        let mut rng = StdRng::seed_from_u64(19);
        let w = random_feasible_weights(3, 0.0, 0.34, &mut rng);
        assert_eq!(w.len(), 3);
        assert!((w.iter().sum::<f64>() - 1.0).abs() < 0.05);
    }

    #[test]
    fn mvo_allocate_ga_family_produce_weights() {
        let data = sample_monthly_3asset();
        let custom_mu = Array1::from_vec(vec![0.15, 0.08, 0.04]);

        let ga = mvo_allocate_ga(&data, &custom_mu, 0.20, 0.02, 0.10);
        let ga_ms = mvo_allocate_ga_with_max_single(&data, &custom_mu, 0.20, 0.02, 0.10, 0.60);
        let ga_sharpe = mvo_allocate_ga_maxsharpe_with_max_single(&data, &custom_mu, 0.20, 0.60);
        let ga_nl = mvo_allocate_ga_nl(&data, &custom_mu, 0.20, 0.02, 0.10);
        for (i, r) in [&ga, &ga_ms, &ga_sharpe, &ga_nl].iter().enumerate() {
            let r = r
                .as_ref()
                .unwrap_or_else(|| panic!("ga variant {} 应有解", i));
            assert!((r.weights.sum() - 1.0).abs() < 1e-6, "variant {}", i);
            assert!(
                r.weights[0] >= 0.20 - 1e-6,
                "variant {} w0={}",
                i,
                r.weights[0]
            );
        }
        // ga_ms 的 max_single=0.60 应被遵守
        let w = ga_ms.as_ref().unwrap();
        assert!(w.weights.iter().all(|&x| x <= 0.60 + 1e-6));
    }

    #[test]
    fn optimize_mvo_single_asset_returns_none() {
        let mu = Array1::from_vec(vec![0.1]);
        let cov = arr2(&[[0.01]]);
        assert!(optimize_mvo(&mu, &cov, 0.0).is_none());
    }

    // ─── 工具函数 ──────────────────────────────────────────────

    #[test]
    fn annualized_returns_simple_mean_times_twelve() {
        let returns = arr2(&[[0.01, 0.02], [0.03, 0.04]]);
        let mu = annualized_returns(&returns);
        assert!((mu[0] - 0.24).abs() < 1e-12);
        assert!((mu[1] - 0.36).abs() < 1e-12);
    }

    #[test]
    fn monthly_returns_from_daily_groups_by_month() {
        // 1 月两日 + 2 月两日 → 2 个月度收益（当月首→当月末口径）
        let daily = vec![
            ("2024-01-02".to_string(), vec![100.0, 200.0]),
            ("2024-01-31".to_string(), vec![110.0, 190.0]),
            ("2024-02-01".to_string(), vec![121.0, 195.0]),
            ("2024-02-28".to_string(), vec![127.05, 205.0]),
        ];
        let m = monthly_returns_from_daily(&daily);
        assert_eq!(m.nrows(), 2);
        // 1 月：110/100-1=10%，190/200-1=-5%
        assert!((m[(0, 0)] - 0.10).abs() < 1e-12);
        assert!((m[(0, 1)] - (-0.05)).abs() < 1e-12);
        // 2 月：127.05/121-1=5%，205/195-1≈5.13%
        assert!((m[(1, 0)] - 0.05).abs() < 1e-12);
        assert!((m[(1, 1)] - (205.0 / 195.0 - 1.0)).abs() < 1e-12);
    }

    #[test]
    fn monthly_returns_from_daily_degenerate_inputs() {
        assert_eq!(monthly_returns_from_daily(&[]).nrows(), 0);
        let single = vec![("2024-01-02".to_string(), vec![100.0])];
        assert_eq!(monthly_returns_from_daily(&single).nrows(), 0);
        // 价格为 0 的资产该月收益记 0
        let daily = vec![
            ("2024-01-02".to_string(), vec![100.0, 0.0]),
            ("2024-01-31".to_string(), vec![110.0, 0.0]),
        ];
        let m = monthly_returns_from_daily(&daily);
        assert!((m[(0, 1)] - 0.0).abs() < 1e-12);
    }
}
