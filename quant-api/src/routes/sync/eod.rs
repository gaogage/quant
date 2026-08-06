//! EOD 日终同步模块（DDD R10c/Step 6c-5：从 scheduler 上帝模块迁出）。
//!
//! 原属 scheduler.rs 的 `sync_eod_data`，职责是 20:00 盘后全量行情同步：
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
use tracing::{info, warn};

use crate::routes::scheduler::{
    ensure_prediction_coverage, load_active_etf_symbols_union, load_first_active_strategy_config,
    sync_limit_with_retry,
};

/// 20:00 日终数据同步：事件优先 → 当日行情 → 复权兜底 → composite → 日报 → 复权全量 → ML → 质量检查。
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
    let index_codes = vec!["000300.SH".to_string()];
    let _ = quant_data::sync::sync_index_daily(
        db,
        tushare,
        &index_codes,
        &date_str,
        &date_str,
        &format!("idx-eod-{}", date_str),
    )
    .await;

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
    // 让日报在 20:00 后几分钟内送达（不等 sync_adj_factor 逐只拉 7210 只 API，那要 2+ 小时）。
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
        "[scheduler] 20:00 EOD 同步 (事件+当日日线+ETF+指数+基础指标+复权兜底) ({})",
        date_str
    );

    // 日报提前推送：composite 合成后立即推，不等 sync_adj_factor 全量同步。
    // 日报只需 NAV 快照（14:40 已写）+ composite 曲线（已合成），不依赖复权因子全量完成。
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

    Ok(())
}
