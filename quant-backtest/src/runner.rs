//! 回测运行器 — 数据加载、引擎驱动、结果持久化

use chrono::NaiveDate;
use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;
use serde_json::json;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};
use uuid::Uuid;

use super::engine::{
    BacktestConfig, BacktestEngine, BacktestMode, BacktestOutput, MarketDay, StrategySignal,
};

pub fn schedule_signals_for_execution(
    trading_days: &[NaiveDate],
    signals: &HashMap<NaiveDate, StrategySignal>,
) -> HashMap<NaiveDate, StrategySignal> {
    let mut scheduled = HashMap::new();
    for pair in trading_days.windows(2) {
        let signal_day = pair[0];
        let execution_day = pair[1];
        if let Some(signal) = signals.get(&signal_day) {
            scheduled.insert(execution_day, signal.clone());
        }
    }
    scheduled
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTaskInsert {
    pub task_id: String,
    pub strategy_version_id: String,
    pub data_version_id: String,
    pub prediction_set_id: Option<String>,
    pub benchmark_symbol: String,
    pub symbols: Vec<String>,
    pub rebalance_frequency: String,
    pub mode: String,
}

#[derive(Debug, Clone, Default)]
struct TradingProfile {
    exchange: Option<String>,
    market: Option<String>,
    is_st: bool,
}

impl BacktestTaskInsert {
    pub fn from_config(task_id: &str, config: &BacktestConfig) -> Self {
        Self {
            task_id: task_id.to_string(),
            strategy_version_id: config.strategy_version_id.clone(),
            data_version_id: config.data_version_id.clone(),
            prediction_set_id: config.prediction_set_id.clone(),
            benchmark_symbol: config.benchmark.clone(),
            symbols: config.symbols.clone(),
            rebalance_frequency: config.rebalance_frequency.clone(),
            mode: match config.mode {
                BacktestMode::Fast => "fast".into(),
                BacktestMode::Standard => "standard".into(),
                BacktestMode::Audit => "audit".into(),
            },
        }
    }
}

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
        info!(
            task_id,
            "回测开始: {} -> {}", config.start_date, config.end_date
        );

        // 1. 创建任务记录
        self.create_task(task_id, &config).await?;

        // 2. 加载交易日历
        let trading_days = self
            .load_trading_days(config.start_date, config.end_date)
            .await?;
        info!(task_id, days = trading_days.len(), "交易日已加载");

        // 3. 加载基准数据
        let benchmark_data = self
            .load_benchmark_data(&config.benchmark, config.start_date, config.end_date)
            .await?;
        info!(task_id, bm_points = benchmark_data.len(), "基准数据已加载");

        // 4. 加载所需股票日线
        let all_symbols: Vec<String> = signals
            .values()
            .flat_map(|s| s.target_weights.keys().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if all_symbols.is_empty() {
            warn!(task_id, "无股票数据");
            return Ok(BacktestOutput {
                config,
                metrics: Default::default(),
                equity_curve: vec![],
                benchmark_curve: vec![],
                trades: vec![],
                daily_positions: vec![],
                targets: vec![],
                exposures: vec![],
                attributions: vec![],
                violations: vec![],
                reproducibility_hash: None,
            });
        }

        let daily_data = self
            .load_daily_bars(&all_symbols, config.start_date, config.end_date)
            .await?;
        let trading_profiles = self.load_trading_profiles(&all_symbols).await?;
        info!(
            task_id,
            symbols = all_symbols.len(),
            dates = daily_data.len(),
            "日线已加载"
        );

        // DEBUG: check first day
        if let Some(first) = trading_days.first() {
            if let Some(day_data) = daily_data.get(first) {
                info!(task_id, date = %first, stocks_in_day = day_data.len(), "首日数据");
                for (sym, (o, c, pc, amount)) in day_data.iter().take(3) {
                    info!(task_id, symbol = %sym, open = %o, close = %c, pre_close = %pc, amount = %amount, "首日行情");
                }
            } else {
                warn!(task_id, date = %first, "首日无行情数据!");
            }
        }

        // 5. 运行回测引擎
        let mut engine = BacktestEngine::new(config.clone());
        let execution_signals = schedule_signals_for_execution(&trading_days, signals);

        for (idx, day) in trading_days.iter().enumerate() {
            let prev_day = idx
                .checked_sub(1)
                .and_then(|i| trading_days.get(i).copied());
            let market = self.build_market_day(
                *day,
                prev_day,
                &daily_data,
                &benchmark_data,
                &trading_profiles,
            );
            let signal = execution_signals.get(day);
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

    async fn load_trading_days(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<NaiveDate>, sqlx::Error> {
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
             FROM market_index_daily_bar
             WHERE symbol = $1 AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date",
        )
        .bind(benchmark)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(d, c, pc)| (d, (c, pc.unwrap_or(c))))
            .collect())
    }

    async fn load_daily_bars(
        &self,
        symbols: &[String],
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<
        HashMap<NaiveDate, HashMap<String, (Decimal, Decimal, Decimal, Decimal)>>,
        sqlx::Error,
    > {
        let rows: Vec<(
            NaiveDate,
            String,
            Option<Decimal>,
            Decimal,
            Option<Decimal>,
            Option<Decimal>,
        )> = sqlx::query_as(
            "SELECT trade_date, symbol, open, close, pre_close, amount
             FROM market_stock_daily_bar
             WHERE symbol = ANY($1) AND trade_date >= $2 AND trade_date <= $3
             ORDER BY trade_date, symbol",
        )
        .bind(symbols)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await?;

        let mut result: HashMap<NaiveDate, HashMap<String, (Decimal, Decimal, Decimal, Decimal)>> =
            HashMap::new();
        for (d, sym, o, c, pc, amount) in rows {
            result.entry(d).or_default().insert(
                sym,
                (
                    o.unwrap_or(c),
                    c,
                    pc.unwrap_or(c),
                    amount.unwrap_or_default(),
                ),
            );
        }
        Ok(result)
    }

    async fn load_trading_profiles(
        &self,
        symbols: &[String],
    ) -> Result<HashMap<String, TradingProfile>, sqlx::Error> {
        let rows: Vec<(String, Option<String>, Option<String>, Option<bool>)> = sqlx::query_as(
            "SELECT symbol, exchange, market, is_st FROM market_stock WHERE symbol = ANY($1)",
        )
        .bind(symbols)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(symbol, exchange, market, is_st)| {
                (
                    symbol,
                    TradingProfile {
                        exchange,
                        market,
                        is_st: is_st.unwrap_or(false),
                    },
                )
            })
            .collect())
    }

    // ─── Market day builder ────────────────────────────────────

    fn build_market_day(
        &self,
        date: NaiveDate,
        prev_trading_day: Option<NaiveDate>,
        daily_data: &HashMap<NaiveDate, HashMap<String, (Decimal, Decimal, Decimal, Decimal)>>,
        benchmark_data: &HashMap<NaiveDate, (Decimal, Decimal)>,
        trading_profiles: &HashMap<String, TradingProfile>,
    ) -> MarketDay {
        let day_data = daily_data.get(&date);
        let bm = benchmark_data
            .get(&date)
            .copied()
            .unwrap_or((Decimal::zero(), Decimal::zero()));

        let mut open = HashMap::new();
        let mut close = HashMap::new();
        let mut pre_close = HashMap::new();
        let mut amount = HashMap::new();
        let mut suspended = HashSet::new();
        let mut up_limit = HashMap::new();
        let mut down_limit = HashMap::new();

        if let Some(data) = day_data {
            for (sym, (o, c, pc, amt)) in data {
                open.insert(sym.clone(), *o);
                close.insert(sym.clone(), *c);
                pre_close.insert(sym.clone(), *pc);
                amount.insert(sym.clone(), *amt);
                if !pc.is_zero() {
                    let limit_rate = Self::limit_rate_for(sym, trading_profiles.get(sym));
                    up_limit.insert(sym.clone(), *pc * (Decimal::ONE + limit_rate));
                    down_limit.insert(sym.clone(), *pc * (Decimal::ONE - limit_rate));
                }
            }
        }

        // 标记停牌：上一交易日有数据但今天没有，避免自然日前一天穿过节假日。
        if let Some(prev_data) = prev_trading_day.and_then(|prev| daily_data.get(&prev)) {
            for sym in prev_data.keys() {
                if !close.contains_key(sym) {
                    suspended.insert(sym.clone());
                }
            }
        }

        MarketDay {
            date,
            open,
            close,
            pre_close,
            amount,
            suspended,
            up_limit,
            down_limit,
            benchmark_close: bm.0,
            benchmark_pre_close: bm.1,
        }
    }

    fn limit_rate_for(symbol: &str, profile: Option<&TradingProfile>) -> Decimal {
        if profile.is_some_and(|p| p.is_st) {
            return Decimal::new(5, 2);
        }

        let market = profile
            .and_then(|p| p.market.as_deref())
            .unwrap_or_default();
        let exchange = profile
            .and_then(|p| p.exchange.as_deref())
            .unwrap_or_default();
        let is_growth_or_bse = market.contains("创业")
            || market.contains("科创")
            || market.contains("北交")
            || exchange.eq_ignore_ascii_case("BSE")
            || symbol.starts_with("300")
            || symbol.starts_with("301")
            || symbol.starts_with("688")
            || symbol.starts_with("8")
            || symbol.starts_with("4")
            || symbol.starts_with("920");

        if is_growth_or_bse {
            Decimal::new(20, 2)
        } else {
            Decimal::new(10, 2)
        }
    }

    // ─── Persistence ──────────────────────────────────────────

    async fn create_task(&self, task_id: &str, config: &BacktestConfig) -> Result<(), sqlx::Error> {
        let insert = BacktestTaskInsert::from_config(task_id, config);
        let cost_model = json!({
            "commission_rate": config.fee_config.commission_rate.to_string(),
            "min_commission": config.fee_config.min_commission.to_string(),
            "tax_rate": config.fee_config.tax_rate.to_string(),
            "cost_multiplier": config.fee_config.cost_multiplier.to_string()
        });
        let slippage_model = json!({
            "slippage_bps": config.fee_config.slippage_bps.to_string(),
            "impact_cost_coefficient": config.fee_config.impact_cost_coefficient.to_string()
        });
        let execution_rules = json!({
            "execution_timing": config.execution_timing,
            "execution_price": config.execution_price,
            "max_participation_rate": config.max_participation_rate.map(|v| v.to_string())
        });
        let parameters = json!({
            "research_dataset_id": config.research_dataset_id.as_deref(),
            "feature_set_version_id": config.feature_set_version_id.as_deref(),
            "prediction_set_id": config.prediction_set_id.as_deref(),
            "portfolio_policy_id": config.portfolio_policy_id.as_deref()
        });
        sqlx::query(
            r#"INSERT INTO backtest_task (task_id, strategy_version_id, data_version_id,
               prediction_set_id, benchmark_symbol, symbols, start_date, end_date, initial_capital,
               rebalance_frequency, cost_model, slippage_model, execution_rules,
               parameters, status, mode, progress, last_heartbeat_at, heartbeat_timeout_seconds, started_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'running', $15, 0, now(), 600, now())"#,
        )
        .bind(&insert.task_id)
        .bind(&insert.strategy_version_id)
        .bind(&insert.data_version_id)
        .bind(&insert.prediction_set_id)
        .bind(&insert.benchmark_symbol)
        .bind(&insert.symbols)
        .bind(config.start_date)
        .bind(config.end_date)
        .bind(config.initial_capital)
        .bind(&insert.rebalance_frequency)
        .bind(cost_model)
        .bind(slippage_model)
        .bind(execution_rules)
        .bind(parameters)
        .bind(&insert.mode)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_results(
        &self,
        task_id: &str,
        output: &BacktestOutput,
    ) -> Result<(), sqlx::Error> {
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
        .bind(output.metrics.turnover)
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
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'filled', $14, $15, $16)"#,
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
            .bind(trade.event_reason.as_deref())
            .bind(trade.target_weight)
            .bind(trade.executed_weight)
            .execute(&self.pool)
            .await?;
        }

        // Portfolio targets
        for target in &output.targets {
            sqlx::query(
                r#"INSERT INTO portfolio_target (task_id, trade_date, symbol, target_weight, target_quantity, reason)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (task_id, trade_date, symbol) DO UPDATE SET
                     target_weight = EXCLUDED.target_weight,
                     target_quantity = EXCLUDED.target_quantity,
                     reason = EXCLUDED.reason"#,
            )
            .bind(task_id)
            .bind(target.trade_date)
            .bind(&target.symbol)
            .bind(target.target_weight)
            .bind(target.target_quantity)
            .bind(target.reason.as_deref())
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

        // Portfolio exposures
        for exposure in &output.exposures {
            sqlx::query(
                r#"INSERT INTO portfolio_exposure
                   (task_id, trade_date, exposure_type, exposure_name, net_exposure, gross_exposure)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (task_id, trade_date, exposure_type, exposure_name) DO UPDATE SET
                     net_exposure = EXCLUDED.net_exposure,
                     gross_exposure = EXCLUDED.gross_exposure"#,
            )
            .bind(task_id)
            .bind(exposure.trade_date)
            .bind(&exposure.exposure_type)
            .bind(&exposure.exposure_name)
            .bind(exposure.net_exposure)
            .bind(exposure.gross_exposure)
            .execute(&self.pool)
            .await?;
        }

        // Portfolio attributions
        for attribution in &output.attributions {
            sqlx::query(
                r#"INSERT INTO portfolio_attribution
                   (task_id, trade_date, attribution_type, attribution_name, contribution)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (task_id, trade_date, attribution_type, attribution_name) DO UPDATE SET
                     contribution = EXCLUDED.contribution"#,
            )
            .bind(task_id)
            .bind(attribution.trade_date)
            .bind(&attribution.attribution_type)
            .bind(&attribution.attribution_name)
            .bind(attribution.contribution)
            .execute(&self.pool)
            .await?;
        }

        // Constraint violations
        for violation in &output.violations {
            let violation_id = format!("pv-{}", Uuid::new_v4());
            sqlx::query(
                r#"INSERT INTO portfolio_constraint_violation
                   (violation_id, task_id, trade_date, constraint_name, limit_value, actual_value, severity)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
            )
            .bind(&violation_id)
            .bind(task_id)
            .bind(violation.trade_date)
            .bind(&violation.constraint_name)
            .bind(violation.limit_value)
            .bind(violation.actual_value)
            .bind(&violation.severity)
            .execute(&self.pool)
            .await?;
        }

        // Update task status
        sqlx::query(
            "UPDATE backtest_task SET status = 'completed', completed_at = now(), progress = 100, last_completed_date = $2, last_heartbeat_at = now() WHERE task_id = $1",
        )
        .bind(task_id)
        .bind(output.equity_curve.last().map(|(date, _)| *date))
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_rate_uses_st_growth_board_and_default_rules() {
        let st = TradingProfile {
            exchange: Some("SSE".into()),
            market: Some("主板".into()),
            is_st: true,
        };
        let growth = TradingProfile {
            exchange: Some("SZSE".into()),
            market: Some("创业板".into()),
            is_st: false,
        };
        let main = TradingProfile {
            exchange: Some("SSE".into()),
            market: Some("主板".into()),
            is_st: false,
        };

        assert_eq!(
            BacktestRunner::limit_rate_for("600000.SH", Some(&st)),
            Decimal::new(5, 2)
        );
        assert_eq!(
            BacktestRunner::limit_rate_for("300001.SZ", Some(&growth)),
            Decimal::new(20, 2)
        );
        assert_eq!(
            BacktestRunner::limit_rate_for("600000.SH", Some(&main)),
            Decimal::new(10, 2)
        );
        assert_eq!(
            BacktestRunner::limit_rate_for("688001.SH", None),
            Decimal::new(20, 2)
        );
    }
}
