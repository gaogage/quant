//! EOD 日终同步模块（DDD R10c/Step 6c-5：从 scheduler 上帝模块迁出）。
//!
//! 原属 scheduler.rs 的 `sync_eod_data`，职责是 22:00 盘后全量行情同步：
//! 事件数据（停牌/涨跌停）→ 当日日线/ETF/指数 → daily_basic/moneyflow/block_trade →
//! 复权因子+兜底 → composite 曲线刷新 → ML 预测覆盖检查 → 数据质量校验。
//!
//! 关键设计：P0 EOD 隔离——daily_basic 等慢点用 timeout 包裹，超时不阻断复权因子兜底
//! （关键路径），避免 adj 视图退化为 raw 价导致回测崩坏。
//!
//! 依赖：quant_data::sync::* + scheduler::{4 helper} + sync::market_data +
//! equity_curve_sync + data_quality（单向依赖，无循环）。

use chrono::NaiveDate;
use quant_data::tushare::client::TushareClient;
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::routes::scheduler::{
    ensure_prediction_coverage, load_active_etf_symbols_union, load_first_active_strategy_config,
    sync_limit_with_retry,
};

/// 22:00 日终数据同步：事件优先 → 当日行情 → 复权兜底 → composite → 日报 → 复权全量 → ML → 质量检查。
pub async fn sync_eod_data(
    db: &PgPool,
    tushare: &TushareClient,
    date: NaiveDate,
    is_trade: bool,
) -> Result<(), String> {
    let date_str = date.format("%Y%m%d").to_string();
    // etf_symbols 取所有 active 复合策略的并集（不再硬编码 v19，覆盖多策略 ETF）。
    let etf_symbols = load_active_etf_symbols_union(db).await;
    // ensure_prediction_coverage 的 ML 训练逻辑仍需单策略 sc（取第一个 active 复合策略为代表）。
    let sc = load_first_active_strategy_config(db).await;
    let all_stocks: Vec<String> = sqlx::query_scalar(
        "SELECT symbol FROM market_stock WHERE list_status = 'L' ORDER BY symbol",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    // ── 事件数据优先同步：即使后续 heavy EOD 任务失败，也不能让交易门禁缺停牌/涨跌停。──
    if let Err(e) = quant_data::sync::sync_suspension(db, tushare, &date_str).await {
        warn!("[scheduler] EOD 停牌数据同步失败: {}", e);
    }
    let limit_ok = sync_limit_with_retry(db, tushare, &date_str).await;
    if !limit_ok {
        warn!("[scheduler] ⚠ 涨跌停数据同步失败 (已重试)");
    }

    // ── 当日日线 + ETF日线（收盘后通常已可获取）──
    // P0 EOD 隔离:日线/ETF/指数/daily_basic/moneyflow/block_trade 均为"非关键路径",
    // 用 timeout 包裹防卡死。daily_basic 是已知慢点(历史曾 3 小时卡死 running 永不完成,
    // 导致后续复权因子同步+兜底未执行,adj 视图退化为 raw 价,回测曲线崩坏)。
    // 超时则 warn 并继续,确保复权因子+兜底(关键路径)不被前序步骤短路。
    const EOD_STEP_TIMEOUT_SECS: u64 = 1800; // 30 分钟

    // 注意:timeout 结果含 Box<dyn StdError>(非 Send),须在独立块内消费,
    // 避免变量跨越后续 await 点导致整个 spawn future 非 Send。
    {
        let daily_timeout = tokio::time::timeout(
            tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS),
            quant_data::sync::sync_daily_bars(
                db,
                tushare,
                &all_stocks,
                &date_str,
                &date_str,
                &format!("dv-eod-{}", date_str),
            ),
        )
        .await;
        match daily_timeout {
            Ok(r) => {
                if let Err(e) = r {
                    warn!("[scheduler] EOD 日线同步失败: {}", e);
                }
            }
            Err(_) => warn!("[scheduler] ⚠ EOD 日线同步超时({}秒),跳过", EOD_STEP_TIMEOUT_SECS),
        }
    }
    let _ = quant_data::sync::sync_fund_daily(
        db,
        tushare,
        &etf_symbols,
        &date_str,
        &date_str,
        &format!("etf-eod-{}", date_str),
    )
    .await;
    // ── 个股两融明细每日增量（2026-09-08 接入，margin_rq* 因子数据源）──
    // 充值 token 权限接口；双 token fallback 后主 client 失败自动切 ALT。
    // 同步失败不阻塞 EOD 主链路（因子次日 T+1 保鲜窗口兜底）。
    // 2026-09-16 修复: margin_detail 是 T+1 数据源(数据日次日才发布), 原来只拉
    // trade_date=今日每晚必空、静默 0 行断供 9 天(源表停 09-07)。改为 [T-1, T]
    // 区间: 昨日为主(T+1 已发布), 今日兜底(防未来改 T+0 发布)。
        {
            let empty_syms: Vec<String> = vec![];
            let prev_str = (chrono::Local::now().date_naive() - chrono::Duration::days(1))
                .format("%Y%m%d")
                .to_string();
            match quant_data::sync::sync_margin_detail(
                db,
                tushare,
                &empty_syms,
                &prev_str,
                &date_str,
                &format!("margin-detail-eod-{}", date_str),
            )
            .await
            {
                Ok(n) if n > 0 => info!("[scheduler] EOD 两融明细同步 {} 行 ({})", n, date_str),
                Ok(_) => {}
                Err(e) => warn!("[scheduler] EOD 两融明细同步失败 {}: {}", date_str, e),
            }
        }
        // ── 两融因子每日增量物化（margin_rq*，生效日口径，与生产白名单 74 因子配套）──
        // 增量只算近 5 个数据日（20 日窗口因子由 SQL 窗口自动取足前置数据）。
        // 失败仅告警：因子缺数当季贡献为零，不会污染既有信号（白名单缺数告警会提示）。
        {
            let factor_sql = r#"
            WITH win AS (
              SELECT MAX(trade_date) - 7 AS min_d, MAX(trade_date) AS max_d FROM market_stock_margin_detail
            ),
            ratio AS (
              SELECT m.symbol, m.available_at AS eff_date,
                AVG(m.rqye / NULLIF(m.rzye, 0)) OVER (
                  PARTITION BY m.symbol ORDER BY m.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS rq_ratio
              FROM market_stock_margin_detail m CROSS JOIN win
              WHERE m.rqye IS NOT NULL AND m.rzye IS NOT NULL AND m.rzye > 0 AND m.available_at IS NOT NULL
                AND m.trade_date >= win.min_d - 40 AND m.trade_date <= win.max_d
            ),
            ins AS (
              INSERT INTO factor_value (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
              SELECT 'margin_rq_ratio_20d_std', '1.0.0', symbol, eff_date, rq_ratio,
                     percent_rank() OVER (PARTITION BY eff_date ORDER BY rq_ratio), eff_date
              FROM ratio WHERE rq_ratio IS NOT NULL AND eff_date >= (SELECT min_d FROM win)
              ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
                raw_value = EXCLUDED.raw_value, normalized_value = EXCLUDED.normalized_value, available_at = EXCLUDED.available_at
              RETURNING 1
            )
            SELECT COUNT(*) FROM ins"#;
            match sqlx::query_scalar::<_, i64>(factor_sql).fetch_one(db).await {
                Ok(n) if n > 0 => info!("[scheduler] 两融因子 rq_ratio 增量物化 {} 行", n),
                Ok(_) => {}
                Err(e) => warn!("[scheduler] 两融因子增量物化失败: {}", e),
            }
        }
        {
            let factor_sql = r#"
            WITH win AS (
              SELECT MAX(trade_date) - 7 AS min_d, MAX(trade_date) AS max_d FROM market_stock_margin_detail
            ),
            raw AS (
              SELECT m.symbol, m.available_at AS eff_date,
                m.rqye / NULLIF(lag20.rqye, 0) - 1 AS chg
              FROM market_stock_margin_detail m CROSS JOIN win
              JOIN LATERAL (
                SELECT rqye FROM market_stock_margin_detail x
                WHERE x.symbol = m.symbol AND x.trade_date < m.trade_date
                ORDER BY x.trade_date DESC OFFSET 19 LIMIT 1
              ) lag20 ON true
              WHERE m.rqye IS NOT NULL AND m.rqye > 0 AND m.available_at IS NOT NULL
                AND m.trade_date >= win.min_d AND m.trade_date <= win.max_d
            ),
            ins AS (
              INSERT INTO factor_value (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
              SELECT 'margin_rqye_chg_20d_std', '1.0.0', symbol, eff_date, chg,
                     percent_rank() OVER (PARTITION BY eff_date ORDER BY chg), eff_date
              FROM raw WHERE chg IS NOT NULL
              ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
                raw_value = EXCLUDED.raw_value, normalized_value = EXCLUDED.normalized_value, available_at = EXCLUDED.available_at
              RETURNING 1
            )
            SELECT COUNT(*) FROM ins"#;
            match sqlx::query_scalar::<_, i64>(factor_sql).fetch_one(db).await {
                Ok(n) if n > 0 => info!("[scheduler] 两融因子 rqye_chg 增量物化 {} 行", n),
                Ok(_) => {}
                Err(e) => warn!("[scheduler] 两融因子 rqye_chg 增量物化失败: {}", e),
            }
        }
        // ── 东财主力资金流每日增量（2026-09-09 接入，mfdc_* 因子数据源，76 白名单配套）──
        // moneyflow_dc 按日全市场 6000 行；双 token fallback 兜权限。生效日 = 数据日+1。
        {
            // raw 拉取：当日 moneyflow_dc 全市场（6000 行/次，reqwest 直调充值 token，
            // 主 token 无该接口权限；失败仅告警不阻塞 EOD）
            if let Ok(tok) = std::env::var("TUSHARE_TOKEN_ALT") {
                if !tok.trim().is_empty() {
                    let body = serde_json::json!({
                        "api_name": "moneyflow_dc", "token": tok.trim(),
                        "params": {"trade_date": date_str},
                        "fields": "ts_code,trade_date,net_amount,net_amount_rate,buy_elg_amount,buy_elg_amount_rate,buy_lg_amount,buy_lg_amount_rate,buy_md_amount,buy_sm_amount"
                    });
                    match reqwest::Client::new()
                        .post("http://api.tushare.pro")
                        .json(&body)
                        .timeout(std::time::Duration::from_secs(60))
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            if let Ok(parsed) = resp.json::<serde_json::Value>().await {
                                if parsed["code"].as_i64() == Some(0) {
                                    if let Some(items) = parsed["data"]["items"].as_array() {
                                        let mut n = 0usize;
                                        for it in items {
                                            let g = |k: &str| it.get(k).and_then(|v| v.as_f64());
                                            let s = |k: &str| it.get(k).and_then(|v| v.as_str());
                                            let (Some(tc), Some(td)) = (s("ts_code"), s("trade_date")) else { continue };
                                            let r = sqlx::query(
                                                "INSERT INTO market_stock_moneyflow_dc_raw
                                                 (ts_code, trade_date, net_amount, net_amount_rate, buy_elg_amount, buy_elg_amount_rate,
                                                  buy_lg_amount, buy_lg_amount_rate, buy_md_amount, buy_sm_amount, available_at)
                                                 VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8, $9, $10, $2::date + 1)
                                                 ON CONFLICT DO NOTHING",
                                            )
                                            .bind(tc).bind(td)
                                            .bind(g("net_amount")).bind(g("net_amount_rate"))
                                            .bind(g("buy_elg_amount")).bind(g("buy_elg_amount_rate"))
                                            .bind(g("buy_lg_amount")).bind(g("buy_lg_amount_rate"))
                                            .bind(g("buy_md_amount")).bind(g("buy_sm_amount"))
                                            .execute(db).await;
                                            if r.map(|x| x.rows_affected()).unwrap_or(0) > 0 { n += 1; }
                                        }
                                        if n > 0 { info!("[scheduler] EOD 东财资金流同步 {} 行 ({})", n, date_str); }
                                    }
                                } else {
                                    warn!("[scheduler] EOD 东财资金流拉取失败 code={:?}", parsed["code"]);
                                }
                            }
                        }
                        Err(e) => warn!("[scheduler] EOD 东财资金流请求失败: {}", e),
                    }
                }
            }
            // 因子增量物化保鲜
            let factor_sql = r#"
            WITH win AS (
              SELECT MAX(trade_date) - 30 AS min_d, MAX(trade_date) AS max_d FROM market_stock_moneyflow_dc_raw
            ),
            smoothed AS (
              SELECT m.ts_code, m.available_at AS eff_date,
                AVG(m.net_amount_rate) OVER (PARTITION BY m.ts_code ORDER BY m.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma20
              FROM market_stock_moneyflow_dc_raw m CROSS JOIN win
              WHERE m.net_amount_rate IS NOT NULL AND m.available_at IS NOT NULL
                AND m.trade_date >= win.min_d
            ),
            ins AS (
              INSERT INTO factor_value (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
              SELECT 'mfdc_net_rate_20d_std', '1.0.0', ts_code, eff_date, sma20,
                     percent_rank() OVER (PARTITION BY eff_date ORDER BY sma20), eff_date
              FROM smoothed WHERE sma20 IS NOT NULL AND eff_date >= (SELECT min_d FROM win)
              ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
                raw_value = EXCLUDED.raw_value, normalized_value = EXCLUDED.normalized_value, available_at = EXCLUDED.available_at
              RETURNING 1
            )
            SELECT COUNT(*) FROM ins"#;
            match sqlx::query_scalar::<_, i64>(factor_sql).fetch_one(db).await {
                Ok(n) if n > 0 => info!("[scheduler] 主力资金流因子 net_rate 增量物化 {} 行", n),
                Ok(_) => {}
                Err(e) => warn!("[scheduler] 主力资金流因子物化失败: {}", e),
            }
            let factor_sql2 = r#"
            WITH win AS (
              SELECT MAX(trade_date) - 30 AS min_d, MAX(trade_date) AS max_d FROM market_stock_moneyflow_dc_raw
            ),
            smoothed AS (
              SELECT m.ts_code, m.available_at AS eff_date,
                AVG(m.buy_elg_amount_rate) OVER (PARTITION BY m.ts_code ORDER BY m.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma20
              FROM market_stock_moneyflow_dc_raw m CROSS JOIN win
              WHERE m.buy_elg_amount_rate IS NOT NULL AND m.available_at IS NOT NULL
                AND m.trade_date >= win.min_d
            ),
            ins AS (
              INSERT INTO factor_value (factor_code, factor_version, symbol, trade_date, raw_value, normalized_value, available_at)
              SELECT 'mfdc_elg_rate_20d_std', '1.0.0', ts_code, eff_date, sma20,
                     percent_rank() OVER (PARTITION BY eff_date ORDER BY sma20), eff_date
              FROM smoothed WHERE sma20 IS NOT NULL AND eff_date >= (SELECT min_d FROM win)
              ON CONFLICT (factor_code, factor_version, symbol, trade_date) DO UPDATE SET
                raw_value = EXCLUDED.raw_value, normalized_value = EXCLUDED.normalized_value, available_at = EXCLUDED.available_at
              RETURNING 1
            )
            SELECT COUNT(*) FROM ins"#;
            match sqlx::query_scalar::<_, i64>(factor_sql2).fetch_one(db).await {
                Ok(n) if n > 0 => info!("[scheduler] 主力资金流因子 elg_rate 增量物化 {} 行", n),
                Ok(_) => {}
                Err(e) => warn!("[scheduler] 主力资金流因子 elg 物化失败: {}", e),
            }
        }
    // ── 业绩预告增量同步（forecast 族因子数据源，2026-09-05 接入）──
    // forecast 接口要求 ann_date 或 ts_code 至少一个参数，按日增量拉当日公告。
    // 用充值 token（现行 token 无此接口权限）。同步失败不阻塞 EOD 主链路。
    if let Ok(tok) = std::env::var("TUSHARE_TOKEN_ALT") {
        if !tok.trim().is_empty() {
            let mut fc_cfg = quant_data::tushare::client::TushareConfig::default();
            fc_cfg.token = tok.trim().to_string();
            fc_cfg.rate_limit_per_minute = 60;
            // 凭证配对铁律: 充值 token(ALT)必须走官方域名——TUSHARE_API_URL 是私有
            // 直连端点(专属 token), default 读到的主 URL 与 ALT 不匹配会 40101
            // (2026-09-15 事故: 逐股全失败一周的根因)。fallback_base_url 也清空——
            // 该 client 的主备 token 相同(都是 ALT), 无 fallback 意义。
            fc_cfg.base_url = std::env::var("TUSHARE_API_URL_ALT")
                .unwrap_or_else(|_| "http://api.tushare.pro".to_string());
            fc_cfg.fallback_token = None;
            fc_cfg.fallback_base_url = None;
            match quant_data::tushare::client::TushareClient::new(fc_cfg) {
                Ok(fc_client) => {
                    // 2026-09-11 后台化: 原串行 await 阻塞主链 2 小时(22:00→00:00),
                    // 导致后续 index/adj_factor 跨 00:00——违反本机 00:00-08:30 关机约束。
                    // forecast(业绩预告)是 forecast_* 因子数据源,非当日信号硬依赖
                    // (当晚缺则次日 9 点档补,影响=该 3 因子晚一天),spawn 后台不阻塞。
                    let db2 = db.clone();
                    let ds = date_str.clone();
                    tokio::spawn(async move {
                        // 2026-09-15: 逐股(7210次×60/min=2h)改 ann_date 全市场单次拉取;
                        // 备用端点教训见 sync_forecast_by_day 文档注释
                        match quant_data::sync::sync_forecast_by_day(
                            &db2, &fc_client, &ds, &format!("fc-eod-{}", ds),
                        ).await {
                            Ok(n) => info!("[EOD] 业绩预告增量(后台,按日): {} 条", n),
                            Err(e) => warn!("[EOD] 业绩预告增量失败(后台,次日9点兜底): {}", e),
                        }
                    });
                }
                Err(e) => warn!("[EOD] forecast client 初始化失败(不阻塞): {}", e),
            }
        }
    }
    // fund_daily 当日缺失时不做运行时兜底(运行时零 Python 依赖铁律,见 quant/AGENTS.md):
    // Tushare fund_daily 就绪率不稳定属数据源现实。缺失时日报当日收益显示 --
    // (snapshot.daily_return 置 NULL),次日 9:00 T+1 补齐后补盯市并补发昨日绩效。
    let index_codes = vec!["000300.SH".to_string()];
    // CSI300 是重放门禁与基准对比的必依赖——失败不能静默（9/07 EOD 静默断更一日，
    // 次日重放被门禁拦截才发现）。失败即告警，次日 9:00 T+1 路径会补齐。
    // 错误先转 String：Box<dyn StdError> 非 Send，不能跨 await 存活于 spawn 的 future。
    let idx_err: Option<String> = match quant_data::sync::sync_index_daily(
        db,
        tushare,
        &index_codes,
        &date_str,
        &date_str,
        &format!("idx-eod-{}", date_str),
    )
    .await
    {
        Ok(_) => None,
        Err(e) => Some(e.to_string()),
    };
    if let Some(err) = idx_err {
        warn!("[EOD] 指数日线同步失败 {}: {}", date_str, err);
        crate::routes::shared::send_quality_alert(
            db,
            &[format!("EOD 指数日线同步失败 {}（次日 T+1 会补齐，若仍缺需手工 sync/index-daily）: {}", date_str, err)],
        )
        .await;
    }

    // 日线基础指标(批量拉取：传空走 trade_date 全市场路径，1 次 API 调用几秒完成)
    {
        let basic_timeout = tokio::time::timeout(
            tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS),
            quant_data::sync::sync_daily_basic(
                db,
                tushare,
                &[],
                &date_str,
                &date_str,
                &format!("dv-basic-eod-{}", date_str),
            ),
        )
        .await;
        match basic_timeout {
            Ok(r) => {
                if let Err(e) = r {
                    warn!("[scheduler] EOD daily_basic 同步失败: {}", e);
                }
            }
            Err(_) => warn!(
                "[scheduler] ⚠ EOD daily_basic 同步超时({}秒),跳过 - 复权因子兜底仍将执行",
                EOD_STEP_TIMEOUT_SECS
            ),
        }
    }

    // 资金流(market_stock_moneyflow): v24 mf_* 因子依赖。
    // 批量拉取：传空走 trade_date 全市场路径，1 次 API 调用几秒完成。
    let mf_n = {
        let mf_dv = format!("mf-eod-{}", date_str);
        let mf_fut = quant_data::sync::sync_moneyflow(
            db,
            tushare,
            &[],
            &date_str,
            &date_str,
            &mf_dv,
        );
        match tokio::time::timeout(tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS), mf_fut)
            .await
        {
            Ok(r) => r.unwrap_or(0),
            Err(_) => {
                warn!("[scheduler] ⚠ EOD 资金流同步超时,跳过");
                0
            }
        }
    };
    if mf_n > 0 {
        info!("[scheduler] EOD 资金流同步: {} 条", mf_n);
    }

    // 大宗交易(market_stock_block_trade): v24 block_trade_inst 因子依赖。
    let bt_n = {
        let bt_dv = format!("bt-eod-{}", date_str);
        let bt_fut = quant_data::sync::sync_block_trade(
            db,
            tushare,
            &bt_dv,
            &date_str,
            &date_str,
        );
        match tokio::time::timeout(tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS), bt_fut)
            .await
        {
            Ok(r) => r.unwrap_or(0),
            Err(_) => {
                warn!("[scheduler] ⚠ EOD 大宗交易同步超时,跳过");
                0
            }
        }
    };
    if bt_n > 0 {
        info!("[scheduler] EOD 大宗交易同步: {} 条", bt_n);
    }

    // ── 复权因子兜底（快，纯 DB 前向填充）+ composite 合成 + 日报推送 ──
    // 关键优化：先 backfill（LATERAL 前值填充，几秒完成）→ composite 合成 → 立即推日报，
    // 让日报在 22:00 后几分钟内送达（不等 sync_adj_factor 逐只拉 7210 只 API，那要 2+ 小时）。
    // 数学正确性：非除权日 adj_factor 恒等于前一交易日值（复权因子仅在除权除息日跳变），
    // backfill 前值填充 == 真实值。除权日当日 sync_adj_factor 会拿到新值，但日报已推——
    // 除权日偏离会略偏，但除权日稀少，且 sync_adj_factor 后台跑完会用真实值覆盖 backfill 值。
    let dv_adj_id = format!("dv-adj-eod-{}", date_str);
    crate::routes::sync::market_data::backfill_adj_factor_for_date(db, date, &dv_adj_id).await;

    // P1: 刷新 composite 回测曲线(偏离监控对标用)。复权兜底后执行,
    // 确保合成所用 ETF 复权价已补全。单策略失败不阻断其他。
    for sid in crate::routes::equity_curve_sync::collect_active_strategies(db).await {
        if let Err(e) =
            crate::routes::equity_curve_sync::sync_composite_equity_curve(db, &sid).await
        {
            warn!("[scheduler] composite 曲线合成失败 {}: {}", sid, e);
        }
    }

    info!(
        "[scheduler] 22:00 EOD 同步 (事件+当日日线+ETF+指数+基础指标+复权兜底) ({})",
        date_str
    );

    // ── EOD 日终盯市:用当日收盘价重估所有 active 模拟账户 ──
    // 14:45 调仓时当日 bar 未入库,mark_to_market 只能取昨收,NAV/daily_return 滞后一天;
    // 此处日线已同步(当日 close 已入库),re-mark + NAV 重算后再推日报,
    // 使日报 daily_return = 当日日终净资产 vs 昨日日终净资产的真实当日涨跌。
    // 14:45 写的 snapshot 会被 push_daily_performance_report 内的 upsert 覆盖为收盘口径。
    if is_trade {
        let accounts: Vec<String> = sqlx::query_scalar(
            "SELECT paper_account_id FROM paper_account \
             WHERE status = 'active' AND account_type = 'simulated'",
        )
        .fetch_all(db)
        .await
        .unwrap_or_default();
        for aid in &accounts {
            if let Err(e) = crate::routes::rebalance::mark_to_market(
                db,
                aid,
                date,
                crate::routes::rebalance::PriceSource::EodClose,
            )
            .await
            {
                warn!("[scheduler] EOD re-mark 失败 {}: {}", aid, e);
                continue;
            }
            if let Err(e) = crate::routes::trading::update_current_nav(db, aid).await {
                warn!("[scheduler] EOD NAV 重算失败 {}: {}", aid, e);
            }
        }
        // 盯市完整性门禁（2026-09-08）：逐账户校验当日 NAV 快照已写入。
        // 背景：unlev 账户 09-04 起快照静默断链 3 个交易日无任何告警（日报路径
        // 单账户失败被吞），绩效曲线出现空洞。宁可告警不可静默。
        let missing_snapshots: Vec<String> = sqlx::query_scalar(
            "SELECT a.paper_account_id FROM paper_account a
             WHERE a.status='active' AND a.account_type='simulated'
               AND NOT EXISTS (SELECT 1 FROM paper_nav_snapshot s
                               WHERE s.paper_account_id=a.paper_account_id AND s.snapshot_date=$1)",
        )
        .bind(date)
        .fetch_all(db)
        .await
        .unwrap_or_default();
        if !missing_snapshots.is_empty() {
            error!(
                accounts = ?missing_snapshots,
                "⚠️ EOD 盯市完整性门禁：{} 个活跃账户当日 NAV 快照缺失（宁可报错不可静默断链）",
                missing_snapshots.len()
            );
            let msgs: Vec<String> = missing_snapshots
                .iter()
                .map(|a| format!("EOD 盯市后账户 {} 在 {} 无 NAV 快照——绩效断链，需排查 mark_to_market/日报路径", a, date))
                .collect();
            crate::routes::shared::send_quality_alert(db, &msgs).await;
        }
        info!(
            "[scheduler] EOD 日终盯市完成({} 账户, 当日收盘价口径)",
            accounts.len()
        );
    }

    // 日报提前推送：composite 合成后立即推，不等 sync_adj_factor 全量同步。
    // 日报只需 NAV 快照（上方 EOD re-mark 已刷新为当日收盘口径）+ composite 曲线（已合成），
    // 不依赖复权因子全量完成。
    if is_trade {
        info!("[scheduler] EOD composite 就绪，提前推送绩效日报...");
        if let Err(e) = crate::routes::report::push_daily_performance_report(db, date).await {
            warn!("[scheduler] 实盘绩效日报生成失败: {}", e);
        }
    }

    // ── 复权因子全量同步（批量拉取：传空走 trade_date 全市场路径，1 次 API 调用几秒完成）
    // + ML + 数据质量。这些是后台收尾任务，不阻塞日报。sync_adj_factor 会用真实值覆盖 backfill 的前值填充。
    let adj_n = quant_data::sync::sync_adj_factor(
        db,
        tushare,
        &[],
        &date_str,
        &date_str,
        &format!("dv-adj-eod-{}", date_str),
    )
    .await
    .unwrap_or(0);
    if adj_n > 0 {
        info!("[scheduler] EOD 复权因子同步: {} 条", adj_n);
    }
    // backfill 再次兜底（sync_adj_factor 部分失败时补全剩余）
    crate::routes::sync::market_data::backfill_adj_factor_for_date(db, date, &dv_adj_id).await;

    // ── ML预测数据检查+补齐 ──
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    let pred_ok = match sc.as_ref() {
        Some(sc) => ensure_prediction_coverage(db, tushare, date, sc).await,
        None => {
            warn!("[scheduler] ⚠ 无 active 策略，跳过 ML 预测覆盖检查");
            false
        }
    };
    if !pred_ok && sc.is_some() {
        warn!("[scheduler] ⚠ ML预测数据补齐失败, v16将降级为纯因子选股");
    }

    // 数据完整性检查
    crate::routes::data_quality::run_data_quality_check(db).await;

    // 夜间信号预备链已迁移为独立定时任务 nightly_signal_prep(22:10 触发,
    // 见 scheduled_task_config)——原 EOD 尾部 spawn 依赖主链完成时点,主链被
    // forecast 拖到 00:00 后预备链才启动,违反关机约束。bar 22:01 入库即满足
    // 预备链全部前置,22:10 独立触发可提前 ~2 小时完成。
    Ok(())
}

