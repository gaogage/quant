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

/// 20:00 日终数据同步：事件优先 → 当日行情 → 复权兜底 → ML 预测 → 质量检查。
pub async fn sync_eod_data(
    db: &PgPool,
    tushare: &TushareClient,
    date: NaiveDate,
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

    // 日线基础指标(已知慢点,超时隔离)
    {
        let basic_timeout = tokio::time::timeout(
            tokio::time::Duration::from_secs(EOD_STEP_TIMEOUT_SECS),
            quant_data::sync::sync_daily_basic(
                db,
                tushare,
                &all_stocks,
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
    // 不在原 EOD 列表内,缺失会导致 moneyflow backfill completed 但无产出(静默降级)。
    let mf_n = {
        let mf_dv = format!("mf-eod-{}", date_str);
        let mf_fut = quant_data::sync::sync_moneyflow(
            db,
            tushare,
            &all_stocks,
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

    // ── 复权因子 + 兜底:关键路径,必须执行,不受前序步骤失败/超时影响 ──
    // (这是 P0 修复核心:7/20-7/21 daily_basic 卡死导致此处未执行,adj 视图退化,回测崩坏)
    let adj_n = quant_data::sync::sync_adj_factor(
        db,
        tushare,
        &all_stocks,
        &date_str,
        &date_str,
        &format!("dv-adj-eod-{}", date_str),
    )
    .await
    .unwrap_or(0);
    if adj_n > 0 {
        info!("[scheduler] EOD 复权因子同步: {} 条", adj_n);
    }

    // 复权因子完整性兜底:adj_factor 表是「每日全量快照」设计(正常≈bar行数,实测5192≈5190)。
    // 但 sync_adj_factor 按 symbol 逐只拉 Tushare(8并发,5201只),限流 200/分钟下部分超时会致
    // 当日只成功一部分(如 7/10 仅 1196/5189)。视图 market_stock_daily_bar_adj 用 LEFT JOIN +
    // COALESCE(adj_factor,1.0),缺失股票复权价退化为 raw 价,与前后日断层(10倍级),回测当日
    // 收益率/涨跌停全错乱。故 EOD 必须校验覆盖率并自动补全,而非仅告警。
    //
    // 补全原理:非除权日 adj_factor 恒等于前一交易日值(复权因子仅在除权除息日跳变)。
    // 对当日有 bar 但缺 adj_factor 的股票,用最近前一交易日的 adj_factor 前向填充 INSERT。
    // 这是数学正确的兜底——除权日当日 Tushare 必返回新值(不会缺),缺失的必是非除权日。
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
        "[scheduler] 20:00 EOD 同步 (事件+当日日线+ETF+指数+基础指标+复权) ({})",
        date_str
    );

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
