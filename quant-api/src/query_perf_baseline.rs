//! Query-side performance baseline for backtest list, summary and equity curve.

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct QueryPerfBaselineConfig {
    pub database_url: String,
    pub task_id: String,
    pub iterations: usize,
    pub page_size: i64,
}

impl Default for QueryPerfBaselineConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://gaocheng@localhost/quant".to_string(),
            task_id: "perf-db-release-e7152c4b-a06a-4d79-8ad8-28a64db5d5f5".to_string(),
            iterations: 20,
            page_size: 50,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPerfBaselineReport {
    pub scenario: String,
    pub task_id: String,
    pub iterations: usize,
    pub page_size: i64,
    pub list_avg_ms: u128,
    pub list_p95_ms: u128,
    pub list_max_ms: u128,
    pub summary_avg_ms: u128,
    pub summary_p95_ms: u128,
    pub summary_max_ms: u128,
    pub equity_curve_avg_ms: u128,
    pub equity_curve_p95_ms: u128,
    pub equity_curve_max_ms: u128,
    pub list_rows: usize,
    pub equity_points: usize,
}

pub fn parse_query_perf_args<I, S>(args: I) -> Result<QueryPerfBaselineConfig, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut config = QueryPerfBaselineConfig::default();
    let mut iter = args.into_iter();
    let _program = iter.next();

    while let Some(arg) = iter.next() {
        let arg = arg.as_ref();
        match arg {
            "--database-url" => config.database_url = parse_next_string(&mut iter, arg)?,
            "--task-id" => config.task_id = parse_next_string(&mut iter, arg)?,
            "--iterations" => config.iterations = parse_next_usize(&mut iter, arg)?,
            "--page-size" => config.page_size = parse_next_i64(&mut iter, arg)?,
            "--help" | "-h" => return Err(query_perf_usage()),
            unknown => return Err(format!("unknown argument `{}`\n{}", unknown, query_perf_usage())),
        }
    }

    config.iterations = config.iterations.max(1);
    config.page_size = config.page_size.clamp(1, 200);
    Ok(config)
}

pub async fn run_query_perf_baseline(
    config: QueryPerfBaselineConfig,
) -> Result<QueryPerfBaselineReport, sqlx::Error> {
    let pool = PgPool::connect(&config.database_url).await?;
    let mut list_samples = Vec::with_capacity(config.iterations);
    let mut summary_samples = Vec::with_capacity(config.iterations);
    let mut curve_samples = Vec::with_capacity(config.iterations);
    let mut list_rows = 0usize;
    let mut equity_points = 0usize;

    for _ in 0..config.iterations {
        let started = Instant::now();
        let rows: Vec<(String, String)> = sqlx::query_as(
            r#"SELECT t.task_id, t.status
               FROM backtest_task t
               LEFT JOIN backtest_result r ON r.task_id = t.task_id
               ORDER BY t.created_at DESC
               LIMIT $1 OFFSET 0"#,
        )
        .bind(config.page_size)
        .fetch_all(&pool)
        .await?;
        list_samples.push(started.elapsed().as_millis());
        list_rows = rows.len();

        let started = Instant::now();
        let _summary: Option<(String, String)> = sqlx::query_as(
            r#"SELECT t.task_id, t.status
               FROM backtest_task t
               JOIN backtest_result r ON r.task_id = t.task_id
               WHERE t.task_id = $1"#,
        )
        .bind(&config.task_id)
        .fetch_optional(&pool)
        .await?;
        summary_samples.push(started.elapsed().as_millis());

        let started = Instant::now();
        let points: Vec<(chrono::NaiveDate, rust_decimal::Decimal)> = sqlx::query_as(
            r#"SELECT trade_date, portfolio_value
               FROM backtest_equity_curve
               WHERE task_id = $1
               ORDER BY trade_date"#,
        )
        .bind(&config.task_id)
        .fetch_all(&pool)
        .await?;
        curve_samples.push(started.elapsed().as_millis());
        equity_points = points.len();
    }

    let list_stats = sample_stats(&list_samples);
    let summary_stats = sample_stats(&summary_samples);
    let curve_stats = sample_stats(&curve_samples);

    Ok(QueryPerfBaselineReport {
        scenario: "phase2_query_perf_smoke".to_string(),
        task_id: config.task_id,
        iterations: config.iterations,
        page_size: config.page_size,
        list_avg_ms: list_stats.avg_ms,
        list_p95_ms: list_stats.p95_ms,
        list_max_ms: list_stats.max_ms,
        summary_avg_ms: summary_stats.avg_ms,
        summary_p95_ms: summary_stats.p95_ms,
        summary_max_ms: summary_stats.max_ms,
        equity_curve_avg_ms: curve_stats.avg_ms,
        equity_curve_p95_ms: curve_stats.p95_ms,
        equity_curve_max_ms: curve_stats.max_ms,
        list_rows,
        equity_points,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SampleStats {
    avg_ms: u128,
    p95_ms: u128,
    max_ms: u128,
}

fn sample_stats(samples: &[u128]) -> SampleStats {
    if samples.is_empty() {
        return SampleStats {
            avg_ms: 0,
            p95_ms: 0,
            max_ms: 0,
        };
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let total: u128 = sorted.iter().sum();
    let p95_index = ((sorted.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);

    SampleStats {
        avg_ms: total / sorted.len() as u128,
        p95_ms: sorted[p95_index],
        max_ms: *sorted.last().expect("non-empty samples has max"),
    }
}

fn parse_next_string<I, S>(iter: &mut I, flag: &str) -> Result<String, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    iter.next()
        .map(|value| value.as_ref().to_string())
        .ok_or_else(|| format!("missing value for `{}`\n{}", flag, query_perf_usage()))
}

fn parse_next_usize<I, S>(iter: &mut I, flag: &str) -> Result<usize, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let value = parse_next_string(iter, flag)?;
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid numeric value `{}` for `{}`", value, flag))
}

fn parse_next_i64<I, S>(iter: &mut I, flag: &str) -> Result<i64, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let value = parse_next_string(iter, flag)?;
    value
        .parse::<i64>()
        .map_err(|_| format!("invalid numeric value `{}` for `{}`", value, flag))
}

pub fn query_perf_usage() -> String {
    [
        "Usage: query_perf_baseline [--database-url URL] [--task-id TASK] [--iterations N] [--page-size N]",
        "",
        "Defaults: --database-url postgres://gaocheng@localhost/quant --iterations 20 --page-size 50",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_perf_args_override_defaults() {
        let config = parse_query_perf_args([
            "query_perf_baseline",
            "--database-url",
            "postgres://gaocheng@localhost/quant",
            "--task-id",
            "perf-db-task",
            "--iterations",
            "7",
            "--page-size",
            "5000",
        ])
        .expect("valid arguments should parse");

        assert_eq!(config.database_url, "postgres://gaocheng@localhost/quant");
        assert_eq!(config.task_id, "perf-db-task");
        assert_eq!(config.iterations, 7);
        assert_eq!(config.page_size, 200);
    }

    #[test]
    fn sample_stats_reports_average_p95_and_max() {
        let stats = sample_stats(&[1, 2, 3, 4, 100]);

        assert_eq!(
            stats,
            SampleStats {
                avg_ms: 22,
                p95_ms: 100,
                max_ms: 100,
            }
        );
    }
}
