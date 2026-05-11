//! 回测运行器 — 数据加载、引擎驱动、结果持久化

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};
use uuid::Uuid;

use super::engine::{BacktestConfig, BacktestEngine, BacktestMode, BacktestOutput, MarketDay, StrategySignal};

// ─── Runner ───────────────────────────────────────────────────────

pub struct BacktestRunner {
    pool: PgPool,
}

impl BacktestRunner {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 执行回测
    pub async fn run(
        &self,
        task_id: &str,
        config: BacktestConfig,
        signals: &HashMap<NaiveDate, StrategySignal>,
    ) -> Result<BacktestOutput, Box<dyn std::error::Error>> {
        info!(task_id, "回测开始: {} -> {}", config.start_date, config.end_date);

        // 1. 创建任务记录
        self.create_task(task_id, &config).await?;

        // 2. 加载交易日历
        let trading_days = self.load_trading_days(config.start_date, config.end_date).await?;
        info!(task_id, days = trading_days.len(), "交易日已加载");

        // 3. 加载基准数据
        let benchmark_data = self.load_benchmark_data(&config.benchmark, config.start_date, config.end_date).await?;
        info!(task_id, bm_points = benchmark_data.len(), "基准数据已加载");

        // 4. 加载所需股票日线
        let all_symbols: Vec<String> = signals.values()
            .flat_map(|s| s.target_weights.keys().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if all_symbols.is_empty() {
            warn!(task_id, "无股票数据");
            return Ok(BacktestOutput {
                config, metrics: Default::default(),
                equity_curve: vec![], benchmark_curve: vec![],
                trades: vec![], daily_positions: vec![],
                exposures: vec![], attributions: vec![], violations: vec![],
                reproducibility_hash: None,
            });
        }

        let daily_data = self.load_daily_bars(&all_symbols, config.start_date, config.end_date).await?;
        info!(task_id, symbols = all_symbols.len(), dates = daily_data.len(), "日线已加载");
        
        // DEBUG: check first day
        if let Some(first) = trading_days.first() {
            if let Some(day_data) = daily_data.get(first) {
                info!(task_id, date = %first, stocks_in_day = day_data.len(), "首日数据");
                for (sym, (c, pc)) in day_data.iter().take(3) {
                    info!(task_id, symbol = %sym, close = %c, pre_close = %pc, "首日行情");
                }
            } else {
                warn!(task_id, date = %first, "首日无行情数据!");
            }
        }

        // 5. 运行回测引擎
        let mut engine = BacktestEngine::new(config.clone());

        for day in &trading_days {
            let market = self.build_market_day(*day, &daily_data, &benchmark_data);
            let signal = signals.get(day);
            engine.process_day(&market, signal);
        }

        // 6. 计算指标 + 持久化
        let output = engine.finalize();
        self.persist_results(task_id, &output).await?;

        info!(task_id,
            total_return = %output.metrics.total_return_pct,
            sharpe = %output.metrics.sharpe_ratio,
            trades = output.trades.len(),
            "回测完成"
        );

        Ok(output)
    }

    // ─── Data loading ──────────────────────────────────────────

    async fn load_trading_days(&self, start: NaiveDate, end: NaiveDate) -> Result<Vec<NaiveDate>, sqlx::Error> {
        let rows: Vec<(NaiveDate,)> = sqlx::query_as(
            "SELECT trade_date FROM market_trade_calendar
             WHERE exchange = 'SSE' AND is_open = true
             AND trade_date >= $1 AND trade_date <= $2
             ORDER BY trade_date",
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(d,)| d).collect())
    }

    async fn load_benchmark_data(
        &self,
        benchmark: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<HashMap<NaiveDate, (Decimal, Decimal)>, sqlx::Error> {
        let rows: Vec<(NaiveDate, Decimal, Option<Decimal>)> = sqlx::query_as(
            "SELECT trade_date, close, pre_close
             FROM market_stock_daily_bar
             WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date",
        )
        .bind(benchmark)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter()
            .map(|(d, c, pc)| (d, (c, pc.unwrap_or(c))))
            .collect())
    }

    async fn load_daily_bars(
        &self,
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<HashMap<NaiveDate, HashMap<String, (Decimal, Decimal)>>, sqlx::Error> {
        let rows: Vec<(NaiveDate, String, Decimal, Option<Decimal>)> = sqlx::query_as(
            "SELECT trade_date, symbol, close, pre_close
             FROM market_stock_daily_bar
             WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date, symbol",
        )
        .bind(symbols)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        let mut result: HashMap<NaiveDate, HashMap<String, (Decimal, Decimal)>> = HashMap::new();
        for (d, sym, c, pc) in rows {
            result.entry(d).or_default().insert(sym, (c, pc.unwrap_or(c)));
        }
        Ok(result)
    }

    // ─── Market day builder ────────────────────────────────────

    fn build_market_day(
        &self,
        date: NaiveDate,
        daily_data: &HashMap<NaiveDate, HashMap<String, (Decimal, Decimal)>>,
        benchmark_data: &HashMap<NaiveDate, (Decimal, Decimal)>,
    ) -> MarketDay {
        let day_data = daily_data.get(&date);
        let bm = benchmark_data.get(&date).copied().unwrap_or((Decimal::zero(), Decimal::zero()));

        let mut close = HashMap::new();
        let mut pre_close = HashMap::new();
        let mut suspended = HashSet::new();
        let mut up_limit = HashMap::new();
        let mut down_limit = HashMap::new();

        if let Some(data) = day_data {
            for (sym, (c, pc)) in data {
                close.insert(sym.clone(), *c);
                pre_close.insert(sym.clone(), *pc);
                // 10% 涨跌停（简化版，实际需按板块区分）
                if !pc.is_zero() {
                    up_limit.insert(sym.clone(), *pc * Decimal::new(11, 1)); // 1.1x
                    down_limit.insert(sym.clone(), *pc * Decimal::new(9, 1));  // 0.9x
                }
            }
        }

        // 标记停牌：前一天有数据但今天没有
        let prev_day = date.pred_opt().unwrap_or(date);
        if let Some(prev_data) = daily_data.get(&prev_day) {
            for sym in prev_data.keys() {
                if !close.contains_key(sym) {
                    suspended.insert(sym.clone());
                }
            }
        }

        MarketDay {
            date,
            close,
            pre_close,
            suspended,
            up_limit,
            down_limit,
            benchmark_close: bm.0,
            benchmark_pre_close: bm.1,
        }
    }

    // ─── Persistence ──────────────────────────────────────────

    async fn create_task(&self, task_id: &str, config: &BacktestConfig) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO backtest_task (task_id, strategy_version_id, data_version_id,
               benchmark_symbol, symbols, start_date, end_date, initial_capital,
               rebalance_frequency, cost_model, slippage_model, execution_rules,
               parameters, status, mode)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'daily', '{}', '{}', '{}', '{}', 'running', $9)"#,
        )
        .bind(task_id)
        .bind("phase2-v1")  // strategy_version_id
        .bind("phase2-v1")  // data_version_id (placeholder)
        .bind(&config.benchmark)
        .bind(&Vec::<String>::new())  // symbols
        .bind(config.start_date)
        .bind(config.end_date)
        .bind(config.initial_capital)
        .bind(match config.mode {
            BacktestMode::Fast => "fast",
            BacktestMode::Standard => "standard",
            BacktestMode::Audit => "audit",
        })
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_results(&self, task_id: &str, output: &BacktestOutput) -> Result<(), sqlx::Error> {
        let result_id = format!("result-{}", Uuid::new_v4());

        // Backtest result
        sqlx::query(
            r#"INSERT INTO backtest_result (result_id, task_id,
               total_return, annualized_return, benchmark_return, excess_return,
               annualized_excess_return, sharpe_ratio, sortino_ratio,
               information_ratio, max_drawdown, relative_max_drawdown,
               turnover, total_trades, win_rate, metrics,
               calmar_ratio, annualized_volatility, reproducibility_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"#,
        )
        .bind(&result_id)
        .bind(task_id)
        .bind(output.metrics.total_return)
        .bind(output.metrics.annual_return_pct)
        .bind(output.metrics.benchmark_return_pct)
        .bind(output.metrics.excess_return_pct)
        .bind(output.metrics.excess_return_pct)
        .bind(output.metrics.sharpe_ratio)
        .bind(output.metrics.sortino_ratio)
        .bind(output.metrics.information_ratio)
        .bind(output.metrics.max_drawdown_pct)
        .bind(output.metrics.max_drawdown_pct)
        .bind(Decimal::zero()) // turnover placeholder
        .bind(output.metrics.num_trades as i32)
        .bind(output.metrics.win_rate_pct)
        .bind(serde_json::Value::Null)
        .bind(output.metrics.calmar_ratio)
        .bind(output.metrics.annualized_volatility)
        .bind(output.reproducibility_hash.as_deref().unwrap_or(""))
        .execute(&self.pool)
        .await?;

        // Equity curve
        for (date, val) in &output.equity_curve {
            sqlx::query(
                "INSERT INTO backtest_equity_curve (task_id, trade_date, portfolio_value, cash)
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(task_id)
            .bind(date)
            .bind(val)
            .bind(Decimal::zero()) // cash detail not tracked in current version
            .execute(&self.pool)
            .await?;
        }

        // Trades
        for trade in &output.trades {
            let trade_id = format!("tr-{}", Uuid::new_v4());
            sqlx::query(
                r#"INSERT INTO backtest_trade (trade_id, task_id, symbol, trade_time, side,
                   quantity, price, amount, commission, tax, slippage,
                   signal_type, signal_strength, fill_status, event_reason,
                   target_weight, executed_weight)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'filled', '', $14, $15)"#,
            )
            .bind(&trade_id)
            .bind(task_id)
            .bind(&trade.symbol)
            .bind(trade.trade_date.and_hms_opt(15, 0, 0))
            .bind(match trade.side {
                super::portfolio::TradeSide::Buy => "buy",
                super::portfolio::TradeSide::Sell => "sell",
            })
            .bind(trade.quantity)
            .bind(trade.price)
            .bind(trade.amount)
            .bind(trade.commission)
            .bind(trade.tax)
            .bind(trade.slippage)
            .bind(trade.signal_type.as_deref())
            .bind(None::<Decimal>)
            .bind(None::<Decimal>)
            .bind(None::<Decimal>)
            .execute(&self.pool)
            .await?;
        }

        // Positions
        for pos in &output.daily_positions {
            sqlx::query(
                r#"INSERT INTO backtest_position (task_id, symbol, position_date,
                   quantity, available_quantity, avg_cost, close_price,
                   market_value, weight, unrealized_pnl, target_weight)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (task_id, symbol, position_date) DO NOTHING"#,
            )
            .bind(task_id)
            .bind(&pos.symbol)
            .bind(pos.date)
            .bind(pos.quantity)
            .bind(pos.available_quantity)
            .bind(pos.avg_cost)
            .bind(pos.close_price)
            .bind(pos.market_value)
            .bind(pos.weight)
            .bind(pos.unrealized_pnl)
            .bind(pos.target_weight)
            .execute(&self.pool)
            .await?;
        }

        // Update task status
        sqlx::query(
            "UPDATE backtest_task SET status = 'completed', completed_at = now(), progress = 100 WHERE task_id = $1",
        )
        .bind(task_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
