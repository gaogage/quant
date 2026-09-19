//! quant-factor 原生量价因子夜间增量（2026-09-16 治本）
//!
//! 背景: mom_5d/vol_20d/turn_20d/mom_20d/rsi_14d/amp_5d/bb_pos_20d 这 7 个
//! quant-factor 原生 batch 因子不在 phase7 回填体系(32 路由)内, 长期无夜间任务,
//! 历史 2026-05-12 / 07-15 / 09-05 三次断供全靠手动 #[ignore] 测试补数
//! (crud.rs stale_pv_recompute / pv_std_backfill_2605), 补完即停。
//!
//! 本任务每晚 23:00(DB CRON: scheduled_task_config `native_pv_increment`)分批
//! 增量计算, 口径与历史手动补数完全一致: 同 batch_compute_factors + chunk 内
//! 截面 ZScore(chunk_size=200, 500 股/批), 保证因子值序列连续。
//! 计算/落库窗口: 最近截面 7 个自然日(容错漏跑自动补), bar 预载回溯 40 天
//! (20d 滚动窗口预热余量)。幂等 upsert, 重复跑安全。

use crate::AppState;
use axum::extract::State;
use quant_factor::batch::{batch_compute_factors, BatchConfig};
use quant_factor::types::{DailyBar, StandardizeMethod};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{error, info};

/// 夜间增量覆盖的 7 因子(full PIT 76 白名单中的 quant-factor 原生量价族)。
/// 增删因子只需改此表——batch_compute_factors 按名字前缀路由公式。
pub const NATIVE_PV_FACTORS: &[&str] = &[
    "mom_5d",
    "vol_20d",
    "turn_20d",
    "mom_20d",
    "rsi_14d",
    "amp_5d",
    "bb_pos_20d",
];

/// 分批股票数(内存受控: 单批 bar 预载约 500 股 × 40 天, 与 pv_std_backfill_2605 同参)。
const BATCH_SYMBOLS: usize = 500;

/// 定时任务入口(scheduler.rs "native_pv_increment" 分支调用), 也可经
/// POST /api/v1/quant/factors/native-pv-increment/background 手动触发。
pub async fn run_native_pv_increment(db: &PgPool) {
    match native_pv_increment_inner(db).await {
        Ok(n) => info!("[native_pv] 夜间增量完成: 落库 {} 行", n),
        Err(e) => {
            error!("[native_pv] 夜间增量失败: {}", e);
            crate::routes::shared::send_dingtalk_alert(
                db,
                &format!("⛔ [原生量价因子] 夜间增量失败(7因子截面停更风险): {}", e),
            )
            .await;
        }
    }
}

/// POST /api/v1/quant/factors/native-pv-increment/background
/// 手动触发入口(无参数): data_sync_task 记录状态, 后台异步执行。
pub async fn native_pv_increment_background(
    State(state): State<Arc<AppState>>,
) -> impl axum::response::IntoResponse {
    use axum::Json;
    let uuid_tail = uuid::Uuid::new_v4().simple().to_string();
    let task_id = format!(
        "fs-{}-{}",
        chrono::Local::now().format("%Y%m%d-%H%M%S%3f"),
        &uuid_tail[..6]
    );
    let _ = sqlx::query(
        "INSERT INTO data_sync_task (task_id, task_type, source, status, total_count, progress, last_heartbeat_at, started_at)
         VALUES ($1, 'native_pv_increment', 'factor', 'running', 0, 0, now(), now())",
    )
    .bind(&task_id)
    .execute(&state.db)
    .await;

    let db = state.db.clone();
    let tid = task_id.clone();
    tokio::spawn(async move {
        let result = native_pv_increment_inner(&db).await;
        let (status, n_rows, err_msg) = match result {
            Ok(n) => ("completed", n as i64, None),
            Err(ref e) => ("failed", 0i64, Some(e.clone())),
        };
        let _ = sqlx::query(
            "UPDATE data_sync_task SET status=$2, error_message=$3, total_count=$4,
             success_count=$4, progress=100, last_heartbeat_at=now(), completed_at=now()
             WHERE task_id=$1",
        )
        .bind(&tid)
        .bind(status)
        .bind(&err_msg)
        .bind(n_rows)
        .execute(&db)
        .await;
        if status == "completed" {
            info!("[native_pv] 手动增量完成 task={} 落库 {} 行", tid, n_rows);
        } else {
            error!(
                "[native_pv] 手动增量失败 task={} {}",
                tid,
                err_msg.unwrap_or_default()
            );
        }
    });

    Json(serde_json::json!({"code": 0, "data": {"task_id": task_id, "status": "running"}}))
}

async fn native_pv_increment_inner(db: &PgPool) -> Result<usize, String> {
    // 1. 窗口: bar 预载起点 = 最新截面日 - 40 天(20d 因子回溯余量);
    //    增量落库起点 = 最新截面日 - 7 天(约 5 个交易日, 容错漏跑补齐)。
    let (latest,): (chrono::NaiveDate,) = sqlx::query_as(
        "SELECT MAX(trade_date) FROM market_stock_daily_bar_adj WHERE trade_date <= CURRENT_DATE",
    )
    .fetch_one(db)
    .await
    .map_err(|e| format!("latest trade date: {}", e))?;
    let preload_start = latest - chrono::Duration::days(40);
    let increment_start = latest - chrono::Duration::days(7);

    // 活跃股票池: 近 7 天有行情的(剔除退市/长停)
    let symbols: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT symbol FROM market_stock_daily_bar WHERE trade_date >= $1 ORDER BY 1",
    )
    .bind(latest - chrono::Duration::days(7))
    .fetch_all(db)
    .await
    .map_err(|e| format!("symbols: {}", e))?;
    if symbols.is_empty() {
        return Err("活跃股票池为空".into());
    }
    info!(
        "[native_pv] 开始: {} 因子 × {} 股, 截面 {} (增量自 {}), 预载自 {}",
        NATIVE_PV_FACTORS.len(),
        symbols.len(),
        latest,
        increment_start,
        preload_start
    );

    let mut total_saved = 0usize;
    for (bi, chunk_syms) in symbols.chunks(BATCH_SYMBOLS).enumerate() {
        let chunk_syms: Vec<String> = chunk_syms.to_vec();

        // 2. 本批预载(复权价视图, 与历史补数同源) + 全量返回 loader
        let bars = load_bars(db, &chunk_syms, preload_start, latest).await?;
        let loader: Arc<
            dyn Fn(
                    &[String],
                    chrono::NaiveDate,
                    chrono::NaiveDate,
                ) -> Result<HashMap<String, Vec<DailyBar>>, String>
                + Send
                + Sync,
        > = {
            let bars = bars.clone();
            Arc::new(move |_syms, _s, _e| Ok(bars.clone()))
        };

        // 3. saver 只收集(batch standardize 已把输出名变为 {factor}_std, 直接用传入名
        //    ——再 format 加后缀会产生 _std_std 垃圾行, 2026-09-16 实测教训)
        let collected: Arc<Mutex<Vec<(String, Vec<(String, chrono::NaiveDate, f64, bool)>)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let saver: Arc<
            dyn Fn(&str, &str, &[(String, chrono::NaiveDate, f64, bool)]) -> Result<usize, String>
                + Send
                + Sync,
        > = {
            let collected = collected.clone();
            Arc::new(move |name, _ver, vals| {
                collected
                    .lock()
                    .unwrap()
                    .push((name.to_string(), vals.to_vec()));
                Ok(vals.len())
            })
        };

        for factor in NATIVE_PV_FACTORS {
            let cfg = BatchConfig {
                factor: factor.to_string(),
                version: "1.0.0".into(),
                symbols: chunk_syms.clone(),
                start_date: preload_start,
                end_date: latest,
                standardize: Some(StandardizeMethod::ZScore),
                chunk_size: 200,
                progress: None,
            };
            let r = batch_compute_factors(cfg, loader.clone(), saver.clone()).await;
            if !r.errors.is_empty() {
                return Err(format!("{}: {:?}", factor, r.errors));
            }
        }

        // 4. 本批落库(仅增量段, 幂等 upsert)——先把收集结果 move 出 Mutex,
        //    避免 MutexGuard 跨 await 导致 future 非 Send。
        let collected_vec = std::mem::take(&mut *collected.lock().unwrap());
        let mut batch_saved = 0usize;
        for (code, vals) in collected_vec.iter() {
            let recent: Vec<&(String, chrono::NaiveDate, f64, bool)> = vals
                .iter()
                .filter(|(_, d, _, _)| *d >= increment_start)
                .collect();
            batch_saved += save_factor_values(db, code, &recent).await?;
        }
        total_saved += batch_saved;
        info!(
            "[native_pv] 批 {}/{}: 落库 {} 行 (累计 {})",
            bi + 1,
            symbols.len().div_ceil(BATCH_SYMBOLS),
            batch_saved,
            total_saved
        );
    }
    Ok(total_saved)
}

/// 预载一批股票的日 bar(复权价视图; change_pct/amount 置空——7 因子仅用 OHLCV+pre_close,
/// turn_20d 为 volume 均值比代理, 不依赖成交额)。与 crud.rs 历史补数的 load_bars_static 同构。
async fn load_bars(
    db: &PgPool,
    syms: &[String],
    s: chrono::NaiveDate,
    e: chrono::NaiveDate,
) -> Result<HashMap<String, Vec<DailyBar>>, String> {
    use rust_decimal::Decimal;
    let rows: Vec<(String, chrono::NaiveDate, Decimal, Decimal, Decimal, Decimal, Decimal, Option<Decimal>)> =
        sqlx::query_as(
            "SELECT symbol, trade_date, open, high, low, close, volume, pre_close \
             FROM market_stock_daily_bar_adj WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3 \
             ORDER BY symbol, trade_date",
        )
        .bind(syms)
        .bind(s)
        .bind(e)
        .fetch_all(db)
        .await
        .map_err(|e| format!("load bars: {}", e))?;
    let mut m: HashMap<String, Vec<DailyBar>> = HashMap::new();
    for (sym, d, o, h, l, c, v, pc) in rows {
        m.entry(sym.clone()).or_default().push(DailyBar {
            symbol: sym,
            trade_date: d,
            open: o,
            high: h,
            low: l,
            close: c,
            volume: v,
            pre_close: pc,
            change_pct: None,
            amount: Decimal::ZERO,
        });
    }
    Ok(m)
}

/// 幂等落库(与 factor_value 唯一键对齐, 重复跑安全)。
async fn save_factor_values(
    db: &PgPool,
    code: &str,
    vals: &[&(String, chrono::NaiveDate, f64, bool)],
) -> Result<usize, String> {
    let mut n = 0usize;
    for (sym, d, v, valid) in vals {
        if !valid {
            continue;
        }
        let r = sqlx::query(
            "INSERT INTO factor_value (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
             VALUES ($1, '1.0.0', $2, $3, $4, $4, $3)
             ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
               raw_value = EXCLUDED.raw_value, normalized_value = EXCLUDED.normalized_value, available_at = EXCLUDED.available_at",
        )
        .bind(code)
        .bind(sym)
        .bind(d)
        .bind(rust_decimal::Decimal::from_f64_retain(*v).unwrap_or_default())
        .execute(db)
        .await
        .map_err(|e| format!("save {} {}: {}", code, sym, e))?;
        n += r.rows_affected() as usize;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 本地验证入口: set -a; source ../.env; source ../.env.quant; set +a;
    /// cargo test --release -p quant-api native_pv_increment_once -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "数据补数/外部 API 工具型(写业务表),手动触发"]
    async fn native_pv_increment_once() {
        let db = sqlx::PgPool::connect(
            &std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into()),
        )
        .await
        .expect("db");
        let n = native_pv_increment_inner(&db).await.expect("increment");
        println!("[native_pv-test] saved {} rows", n);
    }
}
