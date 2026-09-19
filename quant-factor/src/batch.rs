//! Batch factor computation for large symbol universes.
//!
//! Progress is tracked via a simple callback so that API endpoints
//! can report live status without coupling to a specific database schema.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDate;

use crate::factors::price_volume::*;
use crate::standardize::standardize;
use crate::types::*;

/// Progress callback signature: (completed_chunks, total_chunks, symbols_processed, last_symbol)
pub type ProgressFn = Arc<dyn Fn(usize, usize, usize, String) + Send + Sync>;

/// Configuration for batch computation
pub struct BatchConfig {
    pub factor: String,
    pub version: String,
    pub symbols: Vec<String>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub standardize: Option<StandardizeMethod>,
    pub chunk_size: usize,
    pub progress: Option<ProgressFn>,
}

/// Result of batch computation
pub struct BatchResult {
    pub factor_name: String,
    pub version: String,
    pub total_values: usize,
    pub inserted: usize,
    pub errors: Vec<String>,
    pub standardized: bool,
}

/// Bar 加载回调：按 symbol 列表与日期区间加载日线。
pub type BarLoaderFn = Arc<
    dyn Fn(&[String], NaiveDate, NaiveDate) -> Result<HashMap<String, Vec<DailyBar>>, String>
        + Send
        + Sync,
>;

/// 分块持久化回调：落库一批因子值并返回写入行数。
pub type SaveChunkFn = Arc<
    dyn Fn(&str, &str, &[(String, NaiveDate, f64, bool)]) -> Result<usize, String> + Send + Sync,
>;

/// Compute and persist factor values for a list of symbols in chunks.
///
/// The `save_fn` callback is called per chunk to persist values.
pub async fn batch_compute_factors(
    config: BatchConfig,
    bar_loader: BarLoaderFn,
    save_fn: SaveChunkFn,
) -> BatchResult {
    let mut total_values = 0usize;
    let mut inserted = 0usize;
    let mut errors: Vec<String> = Vec::new();

    let (factor_type, period) = parse_factor(&config.factor);
    let n_chunks = config.symbols.len().div_ceil(config.chunk_size);

    for (chunk_idx, chunk) in config.symbols.chunks(config.chunk_size).enumerate() {
        let syms: Vec<String> = chunk.to_vec();

        // Report progress
        if let Some(ref progress) = config.progress {
            let last = syms.last().cloned().unwrap_or_default();
            progress(chunk_idx + 1, n_chunks, config.symbols.len(), last);
        }

        // Load bars
        let bars = match bar_loader(&syms, config.start_date, config.end_date) {
            Ok(b) => b,
            Err(e) => {
                errors.push(format!("chunk {} load failed: {}", chunk_idx, e));
                continue;
            }
        };

        if bars.is_empty() {
            continue;
        }

        let input = FactorInput {
            bars,
            trade_dates: vec![],
        };

        // Compute factor
        let mut output = match compute_price_volume_factor(factor_type, period, &input) {
            Some(output) => output,
            None => {
                errors.push(format!("Unknown factor: {}", config.factor));
                break;
            }
        };

        // Standardize if requested
        let is_std = config.standardize.is_some();
        if let Some(method) = config.standardize {
            output = standardize(&output, method);
        }

        total_values += output.values.len();

        // Build save rows: (symbol, date, value, is_standardized)
        let rows: Vec<(String, NaiveDate, f64, bool)> = output
            .values
            .iter()
            .map(|fv| (fv.symbol.clone(), fv.date, fv.value, is_std))
            .collect();

        match save_fn(&output.name, &config.version, &rows) {
            Ok(n) => inserted += n,
            Err(e) => errors.push(format!("chunk {} save failed: {}", chunk_idx, e)),
        }
    }

    BatchResult {
        factor_name: format!(
            "{}_{}d{}",
            factor_type,
            period,
            if config.standardize.is_some() {
                "_std"
            } else {
                ""
            }
        ),
        version: config.version,
        total_values,
        inserted,
        errors,
        standardized: config.standardize.is_some(),
    }
}

fn parse_factor(name: &str) -> (&'static str, usize) {
    if let Some(rest) = name.strip_prefix("mom_") {
        ("momentum", rest.trim_end_matches('d').parse().unwrap_or(20))
    } else if let Some(rest) = name.strip_prefix("vol_") {
        (
            "volatility",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("downvol_") {
        (
            "downside_volatility",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("rev_") {
        ("reversal", rest.trim_end_matches('d').parse().unwrap_or(5))
    } else if let Some(rest) = name.strip_prefix("turn_") {
        ("turnover", rest.trim_end_matches('d').parse().unwrap_or(20))
    } else if let Some(rest) = name.strip_prefix("amihud_") {
        (
            "amihud_illiquidity",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("amt_intensity_") {
        (
            "amount_intensity",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("rsi_") {
        ("rsi", rest.trim_end_matches('d').parse().unwrap_or(14))
    } else if let Some(rest) = name.strip_prefix("bb_pos_") {
        (
            "bb_position",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("atr_") {
        ("atr", rest.trim_end_matches('d').parse().unwrap_or(14))
    } else if let Some(rest) = name.strip_prefix("amp_") {
        (
            "amplitude",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("vp_corr_") {
        (
            "vol_price_corr",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else if let Some(rest) = name.strip_prefix("skew_") {
        ("skewness", rest.trim_end_matches('d').parse().unwrap_or(20))
    } else if let Some(rest) = name.strip_prefix("maxdd_") {
        (
            "max_drawdown",
            rest.trim_end_matches('d').parse().unwrap_or(20),
        )
    } else {
        ("momentum", 20) // default fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_factor_recognizes_all_prefixes() {
        let cases = [
            ("mom_60d", "momentum", 60),
            ("mom_20", "momentum", 20),
            ("vol_20d", "volatility", 20),
            ("downvol_30d", "downside_volatility", 30),
            ("rev_5d", "reversal", 5),
            ("turn_20d", "turnover", 20),
            ("amihud_20d", "amihud_illiquidity", 20),
            ("amt_intensity_10d", "amount_intensity", 10),
            ("rsi_14d", "rsi", 14),
            ("bb_pos_20d", "bb_position", 20),
            ("atr_14d", "atr", 14),
            ("amp_20d", "amplitude", 20),
            ("vp_corr_20d", "vol_price_corr", 20),
            ("skew_60d", "skewness", 60),
            ("maxdd_20d", "max_drawdown", 20),
        ];
        for (name, expect_kind, expect_window) in cases {
            let (kind, window) = parse_factor(name);
            assert_eq!(kind, expect_kind, "解析 {}", name);
            assert_eq!(window, expect_window, "窗口 {}", name);
        }
    }

    #[test]
    fn parse_factor_invalid_window_falls_back_to_default() {
        // 非数字窗口 → 各前缀默认值
        let (kind, window) = parse_factor("mom_xxd");
        assert_eq!(kind, "momentum");
        assert_eq!(window, 20);
        let (kind, window) = parse_factor("rev_badd");
        assert_eq!(kind, "reversal");
        assert_eq!(window, 5);
    }

    #[test]
    fn parse_factor_unknown_name_falls_back_to_momentum_20() {
        let (kind, window) = parse_factor("nonexistent_factor");
        assert_eq!(kind, "momentum");
        assert_eq!(window, 20);
    }

    #[test]
    fn parse_factor_suffix_ordering_matters() {
        // downvol_ 必须先于 vol_ 匹配（都含 vol 前缀子串），否则 downvol 会被
        // 误解析为 volatility——锁定当前 if-else 顺序
        let (kind, _) = parse_factor("downvol_20d");
        assert_eq!(kind, "downside_volatility");
        // amt_intensity_ 与 turn_/mom_ 无冲突，但同样锁定
        let (kind, _) = parse_factor("amt_intensity_5d");
        assert_eq!(kind, "amount_intensity");
    }

    // ─── batch_compute_factors 主体（回调注入, 内存集成测试）──────────
    //
    // 函数本身不直接触碰数据库（行情加载/落库均为回调），
    // 用内存假回调覆盖全部分支：进度、加载失败、空行情、标准化、落库失败。

    use rust_decimal::Decimal;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// 构造单 symbol 的 n 根等差递增收盘 bar（步长 step 控制动量强度）
    fn make_daily_bars(symbol: &str, n: usize, step: f64) -> Vec<DailyBar> {
        let base = NaiveDate::from_ymd_opt(2025, 3, 3).unwrap();
        (0..n)
            .map(|i| {
                let close = 10.0 + i as f64 * step;
                DailyBar {
                    symbol: symbol.to_string(),
                    trade_date: base + chrono::Duration::days(i as i64),
                    open: Decimal::from_f64_retain(close - 0.1).unwrap(),
                    high: Decimal::from_f64_retain(close + 0.2).unwrap(),
                    low: Decimal::from_f64_retain(close - 0.2).unwrap(),
                    close: Decimal::from_f64_retain(close).unwrap(),
                    pre_close: None,
                    change_pct: None,
                    volume: Decimal::from_f64_retain(1000.0 + i as f64).unwrap(),
                    amount: Decimal::from_f64_retain(close * (1000.0 + i as f64)).unwrap(),
                }
            })
            .collect()
    }

    /// 按请求 symbol 列表过滤返回行情的加载器（与真实 DB 加载语义一致）
    fn loader_with(bars: HashMap<String, Vec<DailyBar>>) -> BarLoaderFn {
        Arc::new(move |syms, _, _| {
            let mut m = HashMap::new();
            for s in syms {
                if let Some(b) = bars.get(s) {
                    m.insert(s.clone(), b.clone());
                }
            }
            Ok(m)
        })
    }

    /// 收集落库行的保存器：记录 (factor_name, version, symbol, date, value, is_std)
    type SavedRow = (String, String, String, NaiveDate, f64, bool);

    fn collecting_saver(rows: Arc<Mutex<Vec<SavedRow>>>) -> SaveChunkFn {
        Arc::new(move |name, ver, chunk| {
            let mut guard = rows.lock().unwrap();
            for (sym, date, value, is_std) in chunk {
                guard.push((
                    name.to_string(),
                    ver.to_string(),
                    sym.clone(),
                    *date,
                    *value,
                    *is_std,
                ));
            }
            Ok(chunk.len())
        })
    }

    fn base_config(symbols: Vec<String>, chunk_size: usize) -> BatchConfig {
        BatchConfig {
            factor: "mom_3d".to_string(),
            version: "1.0.0".to_string(),
            symbols,
            start_date: NaiveDate::from_ymd_opt(2025, 3, 3).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 3, 12).unwrap(),
            standardize: None,
            chunk_size,
            progress: None,
        }
    }

    /// 主流程：2 标的 × 8 根 bar → mom_3d 每标的 5 个输出点，
    /// 行情加载、计算、落库全链路贯通，首点动量手算核对
    #[tokio::test]
    async fn batch_compute_factors_computes_and_saves_momentum() {
        let mut bars = HashMap::new();
        bars.insert("ZZZT1.SH".to_string(), make_daily_bars("ZZZT1.SH", 8, 0.5));
        bars.insert("ZZZT2.SH".to_string(), make_daily_bars("ZZZT2.SH", 8, 0.5));
        let saved = Arc::new(Mutex::new(Vec::new()));

        let result = batch_compute_factors(
            base_config(vec!["ZZZT1.SH".into(), "ZZZT2.SH".into()], 10),
            loader_with(bars),
            collecting_saver(saved.clone()),
        )
        .await;

        assert_eq!(result.factor_name, "momentum_3d");
        assert_eq!(result.version, "1.0.0");
        assert!(result.errors.is_empty(), "不应有错误: {:?}", result.errors);
        assert!(!result.standardized);
        assert_eq!(result.total_values, 10, "2 标的 × 5 个窗口输出");
        assert_eq!(result.inserted, 10);

        let guard = saved.lock().unwrap();
        assert_eq!(guard.len(), 10);
        assert!(guard
            .iter()
            .all(|(name, ver, _, _, _, flag)| name == "mom_3d" && ver == "1.0.0" && !*flag));
        // 首个输出点手算：close[3]=11.5, close[0]=10.0 → (11.5-10)/10 = 0.15
        let first = guard
            .iter()
            .find(|(_, _, sym, _, _, _)| sym == "ZZZT1.SH")
            .expect("应含 ZZZT1.SH 的输出");
        assert!(
            (first.4 - 0.15).abs() < 1e-9,
            "首个动量值应 0.15，实际 {}",
            first.4
        );
    }

    /// 标准化分支：3 标的不同动量 → ZScore 后同日截面均值 ≈ 0，因子名带 _std 后缀
    #[tokio::test]
    async fn batch_compute_factors_applies_zscore_standardization() {
        let mut bars = HashMap::new();
        bars.insert("ZZZA.SH".to_string(), make_daily_bars("ZZZA.SH", 8, 0.1));
        bars.insert("ZZZB.SH".to_string(), make_daily_bars("ZZZB.SH", 8, 0.5));
        bars.insert("ZZZZ.SH".to_string(), make_daily_bars("ZZZZ.SH", 8, 1.0));
        let saved = Arc::new(Mutex::new(Vec::new()));

        let mut config = base_config(
            vec!["ZZZA.SH".into(), "ZZZB.SH".into(), "ZZZZ.SH".into()],
            10,
        );
        config.standardize = Some(StandardizeMethod::ZScore);

        let result =
            batch_compute_factors(config, loader_with(bars), collecting_saver(saved.clone())).await;

        assert_eq!(result.factor_name, "momentum_3d_std");
        assert!(result.standardized);
        assert_eq!(result.total_values, 15);
        assert_eq!(result.inserted, 15);

        // 按日期分组：标准化后每个 3 值截面均值应 ≈ 0
        let guard = saved.lock().unwrap();
        assert!(
            guard.iter().all(|row| row.5),
            "标准化行应标记 is_standardized"
        );
        let mut by_date: BTreeMap<NaiveDate, Vec<f64>> = BTreeMap::new();
        for (_, _, _, date, v, _) in guard.iter() {
            by_date.entry(*date).or_default().push(*v);
        }
        for (date, vals) in by_date {
            assert_eq!(vals.len(), 3, "{} 截面应 3 标的", date);
            let mean = vals.iter().sum::<f64>() / 3.0;
            assert!(
                mean.abs() < 1e-9,
                "{} 截面标准化后均值应 ≈0，实际 {}",
                date,
                mean
            );
        }
    }

    /// 行情加载失败：错误入列且不中断后续 chunk
    #[tokio::test]
    async fn batch_compute_factors_records_loader_error_and_continues_next_chunk() {
        let good_bars: HashMap<String, Vec<DailyBar>> = {
            let mut m = HashMap::new();
            m.insert("ZZZOK.SH".to_string(), make_daily_bars("ZZZOK.SH", 8, 0.5));
            m
        };
        let loader: BarLoaderFn = Arc::new(move |syms, _, _| {
            if syms.first().map(|s| s.as_str()) == Some("ZZZBAD.SH") {
                Err("db down".to_string())
            } else {
                Ok(good_bars.clone())
            }
        });

        let result = batch_compute_factors(
            base_config(vec!["ZZZBAD.SH".into(), "ZZZOK.SH".into()], 1),
            loader,
            Arc::new(|_, _, rows| Ok(rows.len())),
        )
        .await;

        assert_eq!(result.errors.len(), 1, "仅 chunk 0 失败");
        assert!(
            result.errors[0].contains("chunk 0 load failed: db down"),
            "错误应含 chunk 序号与原因，实际 {:?}",
            result.errors[0]
        );
        assert_eq!(result.inserted, 5, "第二个 chunk 正常产出 5 个值");
    }

    /// 空行情：正常跳过该 chunk（不算错误）
    #[tokio::test]
    async fn batch_compute_factors_skips_chunk_returning_no_bars() {
        let loader: BarLoaderFn = Arc::new(|_, _, _| Ok(HashMap::new()));
        let saved = Arc::new(Mutex::new(Vec::new()));

        let result = batch_compute_factors(
            base_config(vec!["ZZZNONE.SH".into()], 10),
            loader,
            collecting_saver(saved.clone()),
        )
        .await;

        assert!(result.errors.is_empty(), "空行情是正常跳过，不是错误");
        assert_eq!(result.total_values, 0);
        assert_eq!(result.inserted, 0);
        assert!(saved.lock().unwrap().is_empty());
    }

    /// 落库失败：计算照常进行，错误入列，inserted 计数为 0
    #[tokio::test]
    async fn batch_compute_factors_records_save_error() {
        let mut bars = HashMap::new();
        bars.insert("ZZZT1.SH".to_string(), make_daily_bars("ZZZT1.SH", 8, 0.5));
        let saver: SaveChunkFn = Arc::new(|_, _, _| Err("insert conflict".to_string()));

        let result = batch_compute_factors(
            base_config(vec!["ZZZT1.SH".into()], 10),
            loader_with(bars),
            saver,
        )
        .await;

        assert_eq!(result.total_values, 5, "计算不受落库失败影响");
        assert_eq!(result.inserted, 0);
        assert_eq!(result.errors.len(), 1);
        assert!(
            result.errors[0].contains("chunk 0 save failed: insert conflict"),
            "错误应含 save 失败原因，实际 {:?}",
            result.errors[0]
        );
    }

    /// 进度回调：chunk_size=2 × 5 标的 → 3 次回调，
    /// 每次携带 (已完成 chunk 数, 总 chunk 数, 总标的数, 本 chunk 末尾标的)
    #[tokio::test]
    async fn batch_compute_factors_reports_progress_per_chunk() {
        let mut bars = HashMap::new();
        let mut symbols = Vec::new();
        for i in 1..=5 {
            let sym = format!("ZZZS{}.SH", i);
            bars.insert(sym.clone(), make_daily_bars(&sym, 8, 0.5));
            symbols.push(sym);
        }

        let calls = Arc::new(Mutex::new(Vec::<(usize, usize, usize, String)>::new()));
        let calls_clone = calls.clone();
        let progress: ProgressFn = Arc::new(move |done, total, n, last| {
            calls_clone.lock().unwrap().push((done, total, n, last));
        });

        let mut config = base_config(symbols, 2);
        config.progress = Some(progress);

        let result = batch_compute_factors(
            config,
            loader_with(bars),
            Arc::new(|_, _, rows| Ok(rows.len())),
        )
        .await;
        assert_eq!(result.inserted, 25, "5 标的 × 5 输出点");

        let guard = calls.lock().unwrap();
        assert_eq!(guard.len(), 3, "3 个 chunk 各回调一次");
        assert_eq!(
            guard[0],
            (1, 3, 5, "ZZZS2.SH".to_string()),
            "首 chunk 完成后应报 (1, 3, 5, 末尾标的)"
        );
        assert_eq!(guard[1], (2, 3, 5, "ZZZS4.SH".to_string()));
        assert_eq!(
            guard[2],
            (3, 3, 5, "ZZZS5.SH".to_string()),
            "最后一个 chunk 只有 1 个标的，末尾即该标的"
        );
    }
}
