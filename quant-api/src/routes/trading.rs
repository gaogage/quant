//! 交易系统模块 — 计划交易、实际交易、融资交易
//!
//! 核心公式：净资产(current_nav) = 持仓市值 + cash - margin_amount
//!
//! 交易类型：
//! - buy/sell: 计划买卖资产
//! - borrow: 融资借款（借钱买入），cash↑ margin_amount↑
//! - repay:  融资归还（还钱），cash↓ margin_amount↓

use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use crate::routes::shared::{PaperAccountRepository, PgPaperAccountRepo};

fn short_id() -> String {
    Uuid::new_v4()
        .to_string()
        .split('-')
        .next()
        .unwrap_or("x")
        .to_string()
}

/// 模拟盘执行费率(执行器配置——本质是券商渠道属性, 2026-09-19 定版)。
/// 绩效自含全成本要求: 模拟成交必须扣佣金/印花税(此前恒 0 使绩效虚高)。
/// - 佣金: 成交额 × rate, 单笔最低 min(双边)
/// - 印花税: 成交额 × rate, 卖出单边
///   可配置: env 覆盖(PAPER_FEE_*), 当前单渠道; 未来多券商/渠道时扩展为
///   账户级配置(paper_account 列或渠道表), 费率结构与计算口径不变。
pub struct ExecutionFeeSchedule {
    pub commission_rate: Decimal, // 万 2.5
    pub commission_min: Decimal,  // 5 元/笔
    pub stamp_tax_rate: Decimal,  // 万 2.5, 卖出单边
}

impl Default for ExecutionFeeSchedule {
    fn default() -> Self {
        Self {
            commission_rate: Decimal::new(25, 5), // 0.00025 (万2.5)
            commission_min: Decimal::from(5),
            stamp_tax_rate: Decimal::new(25, 5), // 0.00025 (万2.5), 卖出单边
        }
    }
}

impl ExecutionFeeSchedule {
    /// 任务80 C6: DB 配置层——优先级 env PAPER_FEE_* > app_config(executor.*) > 代码默认。
    /// app_config 键缺失/脏值静默回落（费率有代码默认兜底，非"宁可报错"场景）。
    pub async fn from_config(db: &PgPool) -> Self {
        let mut s = Self::default();
        if let Some(v) = crate::routes::shared::app_config_f64(db, "executor.commission_rate").await
        {
            if let Some(d) = Decimal::from_f64_retain(v) {
                s.commission_rate = d;
            }
        }
        if let Some(v) = crate::routes::shared::app_config_f64(db, "executor.min_commission").await
        {
            if let Some(d) = Decimal::from_f64_retain(v) {
                s.commission_min = d;
            }
        }
        if let Some(v) = crate::routes::shared::app_config_f64(db, "executor.stamp_tax_rate").await
        {
            if let Some(d) = Decimal::from_f64_retain(v) {
                s.stamp_tax_rate = d;
            }
        }
        // env 覆盖层（部署级，最高）
        if let Ok(v) = std::env::var("PAPER_FEE_COMMISSION_RATE") {
            if let Some(d) = v.parse().ok().and_then(Decimal::from_f64_retain) {
                s.commission_rate = d;
            }
        }
        if let Ok(v) = std::env::var("PAPER_FEE_COMMISSION_MIN") {
            if let Some(d) = v.parse().ok().and_then(Decimal::from_f64_retain) {
                s.commission_min = d;
            }
        }
        if let Ok(v) = std::env::var("PAPER_FEE_STAMP_TAX_RATE") {
            if let Some(d) = v.parse().ok().and_then(Decimal::from_f64_retain) {
                s.stamp_tax_rate = d;
            }
        }
        s
    }

    /// 双边佣金: max(成交额×rate, 最低佣金)
    pub fn commission(&self, amount: Decimal) -> Decimal {
        (amount * self.commission_rate).max(self.commission_min)
    }

    /// 印花税: 卖出单边, 买入为 0
    pub fn stamp_tax(&self, side: &str, amount: Decimal) -> Decimal {
        if side == "sell" {
            amount * self.stamp_tax_rate
        } else {
            Decimal::ZERO
        }
    }
}

/// 业务时间戳：trade_date == 今天时为真实成交时刻 now()；
/// 历史重放日期时为该日 00:00。均取部署环境本地时区(容器 TZ,如 Asia/Shanghai),
/// 不写死时区偏移——换时区部署自动跟随。
/// 此前直接绑 NaiveDate 到 timestamptz 列被按连接会话时区解释,fill_time 恒显示 08:00,
/// 实盘审计无法还原 14:45 调仓的真实时刻。
fn biz_timestamp(d: chrono::NaiveDate) -> chrono::DateTime<chrono::Local> {
    use chrono::TimeZone;
    if d == chrono::Local::now().date_naive() {
        chrono::Local::now()
    } else {
        chrono::Local
            .from_local_datetime(&d.and_hms_opt(0, 0, 0).expect("valid midnight"))
            .single()
            .expect("unambiguous midnight")
    }
}

// ── 计划交易 ──────────────────────────────────────────

pub struct PlannedTrade {
    pub account_id: String,
    pub symbol: String,
    pub side: String, // buy / sell
    pub target_quantity: Decimal,
    pub target_price: Decimal,
    pub price_upper_limit: Option<Decimal>,
    pub price_lower_limit: Option<Decimal>,
    pub slippage_pct: f64,
    pub target_value: Decimal,
    pub reason: Option<String>,
    pub strategy_version_id: Option<String>,
    /// 业务交易日(历史重放用)。None 时用 now()——正常实盘调仓不传。
    pub trade_date: Option<chrono::NaiveDate>,
}

// ── 实际交易 ──────────────────────────────────────────

#[allow(dead_code)]
pub struct ActualTrade {
    pub planned_order_id: String,
    pub fill_price: Decimal,
    pub fill_quantity: Decimal,
    pub fill_amount: Decimal,
    pub commission: Decimal,
    pub tax: Decimal,
    pub slippage: Decimal,
    pub fill_status: String, // filled / partial
}

// ── 创建计划交易 ──────────────────────────────────────

pub async fn create_planned_trade(db: &PgPool, trade: &PlannedTrade) -> Result<String, String> {
    let order_id = format!("po-{}", short_id());
    // trade_date 显式写 created_at(历史重放用业务日期)；None 时用 DB 默认 now()
    if let Some(d) = trade.trade_date {
        sqlx::query(
            "INSERT INTO paper_order (order_id, paper_account_id, strategy_version_id,
             symbol, side, order_type, quantity, limit_price, status, reason,
             target_price, price_upper_limit, price_lower_limit, slippage_pct, target_value, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,'market',$6,$7,'pending',$8,$9,$10,$11,$12,$13,$14,$14)",
        )
        .bind(&order_id)
        .bind(&trade.account_id)
        .bind(trade.strategy_version_id.as_deref())
        .bind(&trade.symbol)
        .bind(&trade.side)
        .bind(trade.target_quantity)
        .bind(trade.target_price)
        .bind(trade.reason.as_deref())
        .bind(trade.target_price)
        .bind(trade.price_upper_limit)
        .bind(trade.price_lower_limit)
        .bind(Decimal::from_f64_retain(trade.slippage_pct).unwrap_or(Decimal::ZERO))
        .bind(trade.target_value)
        .bind(biz_timestamp(d))
        .execute(db)
        .await
        .map_err(|e| format!("create_planned_trade: {}", e))?;
    } else {
        sqlx::query(
            "INSERT INTO paper_order (order_id, paper_account_id, strategy_version_id,
             symbol, side, order_type, quantity, limit_price, status, reason,
             target_price, price_upper_limit, price_lower_limit, slippage_pct, target_value)
             VALUES ($1,$2,$3,$4,$5,'market',$6,$7,'pending',$8,$9,$10,$11,$12,$13)",
        )
        .bind(&order_id)
        .bind(&trade.account_id)
        .bind(trade.strategy_version_id.as_deref())
        .bind(&trade.symbol)
        .bind(&trade.side)
        .bind(trade.target_quantity)
        .bind(trade.target_price)
        .bind(trade.reason.as_deref())
        .bind(trade.target_price)
        .bind(trade.price_upper_limit)
        .bind(trade.price_lower_limit)
        .bind(Decimal::from_f64_retain(trade.slippage_pct).unwrap_or(Decimal::ZERO))
        .bind(trade.target_value)
        .execute(db)
        .await
        .map_err(|e| format!("create_planned_trade: {}", e))?;
    }

    Ok(order_id)
}

// ── 执行实际交易 ──────────────────────────────────────

pub async fn execute_actual_trade(
    db: &PgPool,
    account_id: &str,
    order_id: &str,
    fill: &ActualTrade,
) -> Result<String, String> {
    let fill_id = format!("pf-{}", short_id());
    // fill_time 从 paper_order.created_at 取(历史重放时 order.created_at 是业务日期)
    sqlx::query(
        "INSERT INTO paper_fill (fill_id, order_id, paper_account_id, symbol,
         fill_time, side, quantity, price, amount, commission, tax, slippage,
         planned_order_id, fill_status)
         SELECT $1, $2, $3, symbol, o.created_at, side, $4, $5, $6, $7, $8, $9, $10, $11
         FROM paper_order o WHERE order_id = $2",
    )
    .bind(&fill_id)
    .bind(order_id)
    .bind(account_id)
    .bind(fill.fill_quantity)
    .bind(fill.fill_price)
    .bind(fill.fill_amount)
    .bind(fill.commission)
    .bind(fill.tax)
    .bind(fill.slippage)
    .bind(order_id) // planned_order_id = order_id
    .bind(&fill.fill_status)
    .execute(db)
    .await
    .map_err(|e| format!("execute_actual_trade: {}", e))?;

    // 更新计划交易状态
    sqlx::query("UPDATE paper_order SET status = $1 WHERE order_id = $2")
        .bind(&fill.fill_status)
        .bind(order_id)
        .execute(db)
        .await
        .map_err(|e| format!("update order status: {}", e))?;

    Ok(fill_id)
}

// ── 模拟执行（计划=实际） ─────────────────────────────

pub async fn execute_simulated_trade(
    db: &PgPool,
    trade: &PlannedTrade,
) -> Result<(String, String), String> {
    let order_id = create_planned_trade(db, trade).await?;
    // 实际成交价 = 计划价 × (1 ± slippage_pct);买入抬高成本,卖出反向
    let slip = Decimal::from_f64_retain(trade.slippage_pct).unwrap_or(Decimal::ZERO);
    let multiplier = match trade.side.as_str() {
        "sell" => Decimal::ONE - slip,
        _ => Decimal::ONE + slip, // buy
    };
    let fill_price = trade.target_price * multiplier;
    let fill_quantity = trade.target_quantity;
    let fill_amount = fill_price * fill_quantity;
    let slippage = (fill_price - trade.target_price) * fill_quantity;
    // 佣金/印花税(2026-09-19 绩效自含全成本): 费率为执行器配置
    // (ExecutionFeeSchedule, env 可覆盖)。fill 行记录计算全额(审计口径),
    // 下方现金扣减按同额独立成笔。
    let fee = ExecutionFeeSchedule::from_config(db).await;
    let commission = fee.commission(fill_amount);
    let tax = fee.stamp_tax(&trade.side, fill_amount);
    let fill = ActualTrade {
        planned_order_id: order_id.clone(),
        fill_price,
        fill_quantity,
        fill_amount,
        commission,
        tax,
        slippage,
        fill_status: "filled".to_string(),
    };
    let fill_id = execute_actual_trade(db, &trade.account_id, &order_id, &fill).await?;
    // 费用现金扣减(独立于成交扣款笔): LEAST 截断到现金余额——无杠杆账户满仓
    // 时不因 5 元佣金意外产生 margin(严格不融资), 截断差额每笔 < 5 元可忽略。
    let fee_total = commission + tax;
    if fee_total > Decimal::ZERO {
        if let Err(e) = PgPaperAccountRepo::new(db)
            .debit_fee_capped(&trade.account_id, fee_total)
            .await
        {
            // 费用扣减失败不回滚成交(成交已落库); 留痕供对账
            tracing::warn!(
                "[trading] 费用现金扣减失败 {} {} (commission={} tax={}): {}",
                trade.account_id,
                order_id,
                commission,
                tax,
                e
            );
        }
    }
    Ok((order_id, fill_id))
}

// ── 融资借款 ──────────────────────────────────────────

#[allow(dead_code)]
pub async fn execute_margin_borrow(
    db: &PgPool,
    account_id: &str,
    amount: Decimal,
    reason: &str,
) -> Result<String, String> {
    if amount <= Decimal::ZERO {
        return Err("融资借款金额必须大于0".into());
    }
    let trade_id = format!("mt-{}", short_id());
    sqlx::query(
        "INSERT INTO paper_margin_trade (margin_trade_id, paper_account_id, side, amount, reason)
         VALUES ($1, $2, 'borrow', $3, $4)",
    )
    .bind(&trade_id)
    .bind(account_id)
    .bind(amount)
    .bind(reason)
    .execute(db)
    .await
    .map_err(|e| format!("margin_borrow: {}", e))?;

    // 更新账户：现金增加，融资金额增加
    // R5b 批次4:收敛为 AccountRepository::apply_margin_borrow(SQL 逐字搬移)。
    PgPaperAccountRepo::new(db)
        .apply_margin_borrow(account_id, amount)
        .await
        .map_err(|e| format!("margin_borrow update: {}", e))?;

    Ok(trade_id)
}

// ── 融资归还 ──────────────────────────────────────────

pub async fn execute_margin_repay(
    db: &PgPool,
    account_id: &str,
    amount: Decimal,
    reason: &str,
) -> Result<String, String> {
    if amount <= Decimal::ZERO {
        return Err("融资归还金额必须大于0".into());
    }
    // 检查当前 margin_amount 是否足够
    let row: Option<(Option<rust_decimal::Decimal>,)> = sqlx::query_as(
        "SELECT COALESCE(margin_amount,0) FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("query: {}", e))?;

    let current_margin = row.and_then(|(m,)| m).unwrap_or(Decimal::ZERO);
    if amount > current_margin {
        return Err(format!(
            "融资归还金额({})超过当前融资金额({})",
            amount, current_margin
        ));
    }

    let trade_id = format!("mt-{}", short_id());
    sqlx::query(
        "INSERT INTO paper_margin_trade (margin_trade_id, paper_account_id, side, amount, reason)
         VALUES ($1, $2, 'repay', $3, $4)",
    )
    .bind(&trade_id)
    .bind(account_id)
    .bind(amount)
    .bind(reason)
    .execute(db)
    .await
    .map_err(|e| format!("margin_repay: {}", e))?;

    // 更新账户：现金减少，融资金额减少
    // R5b 批次4:收敛为 AccountRepository::apply_margin_repay(SQL 逐字搬移)。
    PgPaperAccountRepo::new(db)
        .apply_margin_repay(account_id, amount)
        .await
        .map_err(|e| format!("margin_repay update: {}", e))?;

    Ok(trade_id)
}

// ── 自动归还融资 ──────────────────────────────────────

/// 如果 cash > reserve_amount，归还超出部分（不超过 margin_amount）
pub async fn try_auto_repay(db: &PgPool, account_id: &str) -> Result<Option<String>, String> {
    let row: Option<(
        Option<rust_decimal::Decimal>,
        Option<rust_decimal::Decimal>,
        Option<rust_decimal::Decimal>,
    )> = sqlx::query_as(
        "SELECT COALESCE(cash,0), COALESCE(margin_amount,0), COALESCE(reserve_amount,0)
         FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    let (cash, margin, reserve) = match row {
        Some((c, m, r)) => (
            c.unwrap_or(Decimal::ZERO),
            m.unwrap_or(Decimal::ZERO),
            r.unwrap_or(Decimal::ZERO),
        ),
        None => return Ok(None),
    };

    let excess = cash - reserve;
    if excess <= Decimal::ZERO || margin <= Decimal::ZERO {
        return Ok(None);
    }

    let repay_amount = if excess < margin { excess } else { margin };
    execute_margin_repay(db, account_id, repay_amount, "自动归还多余融资")
        .await
        .map(Some)
}

// ── 更新净资产 ────────────────────────────────────────

/// 更新 paper_account.current_nav = 持仓市值合计 + cash - margin,返回算出的 NAV。
///
/// 改返回 `Result<f64>`(P1-B 性能优化):mvo_simulate 循环内可直接拿 NAV,
/// 省去紧随其后的 `SELECT current_nav` 一次 DB 往返(~N 次,N=模拟天数)。
pub async fn update_current_nav(db: &PgPool, account_id: &str) -> Result<f64, String> {
    // P0-3 修复(2026-07-20): 原实现只更新 current_nav，peak_nav/max_drawdown_pct 在实盘调仓
    // 路径中从不更新（只有 paper.rs 旧回放路径维护）。现改为单条原子 UPDATE 同步维护三者：
    //   peak_nav = GREATEST(旧peak, 新nav)            -- 单调递增的水位线
    //   max_drawdown_pct = GREATEST(旧max_dd, 当前回撤) -- 当前回撤=(新peak-新nav)/新peak
    // SET 表达式中的 peak_nav/max_drawdown_pct 引用 UPDATE 前的旧值（PostgreSQL 语义），
    // calc.nav 由 paper_position 聚合 + cash - margin 计算得出（均为本语句未修改的列，安全）。
    //
    // NAV 日度合理性检查（2026-09-07 加，价格源不一致 BUG 防御）：
    // 单日 NAV 变化 > 20% 时告警（分散组合+1.5x 杠杆下单日物理极限 ~10-15%，
    // >20% 几乎必然是价格源错配/计算错误，如 2026-07-13 的 raw→adjusted 切换）。
    let prev_nav: f64 = sqlx::query_scalar(
        "SELECT COALESCE(current_nav, 0)::double precision FROM paper_account WHERE paper_account_id = $1",
    )
    .bind(account_id)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    let nav: f64 = sqlx::query_scalar(
        "UPDATE paper_account SET
         current_nav = calc.nav,
         peak_nav = GREATEST(COALESCE(peak_nav, calc.nav), calc.nav),
         max_drawdown_pct = GREATEST(
             COALESCE(max_drawdown_pct, 0),
             CASE WHEN GREATEST(COALESCE(peak_nav, calc.nav), calc.nav) > 0
                  THEN GREATEST(0,
                      (GREATEST(COALESCE(peak_nav, calc.nav), calc.nav) - calc.nav)
                      / GREATEST(COALESCE(peak_nav, calc.nav), calc.nav))
                  ELSE 0 END
         )
         FROM (
             SELECT (SELECT COALESCE(SUM(market_value),0) FROM paper_position WHERE paper_account_id = $1)
                  + COALESCE(cash,0) - COALESCE(margin_amount,0) AS nav
             FROM paper_account WHERE paper_account_id = $1
         ) calc
         WHERE paper_account_id = $1
         RETURNING current_nav::double precision",
    )
    .bind(account_id)
    .fetch_one(db)
    .await
    .map_err(|e| format!("update_nav: {}", e))?;

    // NAV 合理性告警：>20% 单日变化
    if prev_nav > 0.0 && nav > 0.0 {
        let change_pct = (nav / prev_nav - 1.0).abs() * 100.0;
        if change_pct > 20.0 {
            tracing::error!(
                account = account_id,
                prev_nav = prev_nav,
                new_nav = nav,
                change_pct = change_pct,
                "⚠️ NAV 单日变化 >20%——疑似价格源错配或计算错误（分散组合+杠杆的物理极限 ~15%）"
            );
        }
    }
    Ok(nav)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn fee_schedule_commission_min_and_rate() {
        let f = super::ExecutionFeeSchedule::default();
        // 大额: 按费率 万2.5 (100 万 → 250 元)
        assert_eq!(f.commission(Decimal::from(1_000_000)), Decimal::from(250));
        // 小额: 触发最低 5 元 (1 万 → 2.5 元 < 5 → 收 5)
        assert_eq!(f.commission(Decimal::from(10_000)), Decimal::from(5));
        // 边界: 2 万 × 万2.5 = 5 元 恰好等于最低
        assert_eq!(f.commission(Decimal::from(20_000)), Decimal::from(5));
    }

    #[test]
    fn fee_schedule_stamp_tax_sell_only() {
        let f = super::ExecutionFeeSchedule::default();
        // 印花税卖出单边 万2.5; 买入为 0
        assert_eq!(
            f.stamp_tax("sell", Decimal::from(1_000_000)),
            Decimal::from(250)
        );
        assert_eq!(f.stamp_tax("buy", Decimal::from(1_000_000)), Decimal::ZERO);
    }

    #[test]
    fn slippage_buy_inflates_fill_price() {
        // 买入:实际价 = 计划价 × (1 + slippage_pct)
        let target_price = Decimal::new(10000, 2); // 100.00
        let slip = Decimal::new(2, 3); // 0.002 精确构造(from_f64_retain 会引入浮点噪声)
        let fill_price = target_price * (Decimal::ONE + slip);
        assert_eq!(fill_price, Decimal::new(10020, 2)); // 100.20
        let qty = Decimal::new(100, 0);
        let slippage = (fill_price - target_price) * qty;
        assert_eq!(slippage, Decimal::new(2000, 2)); // 0.20×100 = 20.00
    }

    #[test]
    fn slippage_sell_deflates_fill_price() {
        // 卖出:实际价 = 计划价 × (1 - slippage_pct)
        let target_price = Decimal::new(10000, 2);
        let slip = Decimal::new(2, 3); // 0.002
        let fill_price = target_price * (Decimal::ONE - slip);
        assert_eq!(fill_price, Decimal::new(9980, 2)); // 99.80
    }

    #[test]
    fn short_id_yields_first_uuid_segment() {
        let id = short_id();
        assert!(!id.is_empty(), "short_id 非空");
        assert!(!id.contains('-'), "只取 UUID 第一段: {id}");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "应为 hex 片段: {id}"
        );
        // 连续生成不重复
        assert_ne!(short_id(), short_id());
    }

    #[test]
    fn biz_timestamp_uses_midnight_for_history_and_now_for_today() {
        let historical = chrono::NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let ts = biz_timestamp(historical);
        assert_eq!(ts.timestamp_subsec_nanos(), 0, "历史日期应为该日 00:00 整");
        assert_eq!(ts.date_naive(), historical);

        let today = chrono::Local::now().date_naive();
        let now_ts = biz_timestamp(today);
        let now = chrono::Local::now();
        let drift = (now_ts - now).num_seconds().abs();
        assert!(drift <= 5, "今天应取真实时刻 now(): 偏差 {drift}s");
    }
}

// ─── 第二批补充测试（DB 路径，真实本机 PG，zzz 键自造自清理）─────────────

#[cfg(test)]
mod db_second_batch {
    use super::*;
    use rust_decimal::Decimal;

    async fn test_db() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        sqlx::PgPool::connect(&url).await.expect("test db connect")
    }

    /// 造 zzz 模拟账户：cash=100000 / margin=0 / reserve=5000，先清残留保证幂等。
    async fn create_zzz_account(db: &sqlx::PgPool, account_id: &str) {
        cleanup_account(db, account_id).await;
        sqlx::query(
            "INSERT INTO paper_account
               (paper_account_id, name, initial_capital, cash, status,
                account_type, signal_source, margin_amount, reserve_amount)
             VALUES ($1, 'zzz 第二批交易测试', 100000, 100000, 'active',
                'simulated', 'factor', 0, 5000)",
        )
        .bind(account_id)
        .execute(db)
        .await
        .expect("insert zzz paper_account");
    }

    /// 精确清理 zzz 账户的全部子表行（FK CASCADE 兜底前手动清，顺序无关化）。
    async fn cleanup_account(db: &sqlx::PgPool, account_id: &str) {
        for sql in [
            "DELETE FROM paper_fill WHERE paper_account_id = $1",
            "DELETE FROM paper_order WHERE paper_account_id = $1",
            "DELETE FROM paper_margin_trade WHERE paper_account_id = $1",
            "DELETE FROM paper_position WHERE paper_account_id = $1",
        ] {
            let _ = sqlx::query(sql).bind(account_id).execute(db).await;
        }
        let _ = sqlx::query("DELETE FROM paper_account WHERE paper_account_id = $1")
            .bind(account_id)
            .execute(db)
            .await;
    }

    async fn account_balances(db: &sqlx::PgPool, account_id: &str) -> (Decimal, Decimal, Decimal) {
        sqlx::query_as(
            "SELECT COALESCE(cash,0), COALESCE(margin_amount,0), COALESCE(reserve_amount,0)
             FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_one(db)
        .await
        .expect("zzz account row")
    }

    #[tokio::test]
    async fn update_current_nav_tracks_peak_and_drawdown_for_zzz_account() {
        let db = test_db().await;
        let account_id = "zzz_test_api3_nav";
        create_zzz_account(&db, account_id).await;

        // 无持仓：nav = cash - margin = 100000；peak 初始化为 nav
        let nav = update_current_nav(&db, account_id)
            .await
            .expect("nav update");
        assert_eq!(nav, 100_000.0);
        let (peak, dd): (Option<Decimal>, Option<Decimal>) = sqlx::query_as(
            "SELECT peak_nav, max_drawdown_pct FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&db)
        .await
        .expect("zzz account");
        assert_eq!(peak, Some(Decimal::from(100_000)));
        assert_eq!(dd.unwrap_or(Decimal::ZERO), Decimal::ZERO, "无回撤");

        // cash 降至 90000：nav 回撤 10%，peak 保持
        sqlx::query("UPDATE paper_account SET cash = 90000 WHERE paper_account_id = $1")
            .bind(account_id)
            .execute(&db)
            .await
            .expect("adjust cash");
        let nav = update_current_nav(&db, account_id)
            .await
            .expect("nav update 2");
        assert_eq!(nav, 90_000.0);
        let (peak, dd): (Option<Decimal>, Option<Decimal>) = sqlx::query_as(
            "SELECT peak_nav, max_drawdown_pct FROM paper_account WHERE paper_account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&db)
        .await
        .expect("zzz account 2");
        assert_eq!(peak, Some(Decimal::from(100_000)), "peak 水位线单调保持");
        let dd = dd.unwrap_or(Decimal::ZERO);
        assert!(
            (dd - Decimal::new(1, 1)).abs() < Decimal::new(1, 4),
            "回撤应约 10%: {dd}"
        );

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn margin_borrow_and_repay_roundtrip_on_zzz_account() {
        let db = test_db().await;
        let account_id = "zzz_test_api3_margin";
        create_zzz_account(&db, account_id).await;

        // 金额校验拒绝分支
        let err = execute_margin_borrow(&db, account_id, Decimal::ZERO, "zzz")
            .await
            .expect_err("零额借款应拒绝");
        assert!(err.contains("大于0"), "拒绝信息应说明金额须为正: {err}");
        let err = execute_margin_repay(&db, account_id, Decimal::from(-5), "zzz")
            .await
            .expect_err("负额还款应拒绝");
        assert!(err.contains("大于0"), "拒绝信息应说明金额须为正: {err}");

        // 借款 1000：cash↑ margin↑
        let trade_id = execute_margin_borrow(&db, account_id, Decimal::from(1000), "zzz 借款")
            .await
            .expect("borrow");
        assert!(trade_id.starts_with("mt-"), "融资流水 id 前缀: {trade_id}");
        let (cash, margin, _) = account_balances(&db, account_id).await;
        assert_eq!(cash, Decimal::from(101_000));
        assert_eq!(margin, Decimal::from(1000));

        // 超额还款拒绝
        let err = execute_margin_repay(&db, account_id, Decimal::from(2000), "zzz")
            .await
            .expect_err("超额还款应拒绝");
        assert!(
            err.contains("超过当前融资金额"),
            "拒绝信息应说明超额: {err}"
        );

        // 正常还款 400
        let trade_id = execute_margin_repay(&db, account_id, Decimal::from(400), "zzz 还款")
            .await
            .expect("repay");
        assert!(trade_id.starts_with("mt-"));
        let (cash, margin, _) = account_balances(&db, account_id).await;
        assert_eq!(cash, Decimal::from(100_600));
        assert_eq!(margin, Decimal::from(600));

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn try_auto_repay_settles_excess_cash_above_reserve_on_zzz_account() {
        let db = test_db().await;
        let account_id = "zzz_test_api3_autorepay";
        create_zzz_account(&db, account_id).await;
        execute_margin_borrow(&db, account_id, Decimal::from(800), "zzz 借款")
            .await
            .expect("borrow");

        // cash=100800, reserve=5000 → excess=95800 > margin=800 → 全额归还 800
        let repaid = try_auto_repay(&db, account_id)
            .await
            .expect("auto repay")
            .expect("应触发自动归还");
        assert!(repaid.starts_with("mt-"), "自动归还产生还款流水: {repaid}");
        let (cash, margin, _) = account_balances(&db, account_id).await;
        assert_eq!(margin, Decimal::ZERO, "excess 超过 margin 时全额归还");
        assert_eq!(cash, Decimal::from(100_000), "归还 800 后现金回到本金");

        // margin=0 后再触发：无事可还
        let again = try_auto_repay(&db, account_id)
            .await
            .expect("auto repay again");
        assert!(again.is_none(), "无融资时不再产生流水");

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn execute_simulated_trade_fills_order_with_fees_on_zzz_account() {
        let db = test_db().await;
        let account_id = "zzz_test_api3_sim";
        create_zzz_account(&db, account_id).await;

        // 买 100 股 @10.00，滑点 0.2%
        let (order_id, fill_id) = execute_simulated_trade(
            &db,
            &PlannedTrade {
                account_id: account_id.to_string(),
                symbol: "510300.SH".to_string(),
                side: "buy".to_string(),
                target_quantity: Decimal::from(100),
                target_price: Decimal::new(1000, 2), // 10.00
                price_upper_limit: None,
                price_lower_limit: None,
                slippage_pct: 0.002,
                target_value: Decimal::from(1000),
                reason: Some("zzz 第二批模拟成交测试".to_string()),
                strategy_version_id: None,
                trade_date: None,
            },
        )
        .await
        .expect("simulated trade");
        assert!(order_id.starts_with("po-"));
        assert!(fill_id.starts_with("pf-"));

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM paper_order WHERE order_id = $1")
                .bind(&order_id)
                .fetch_one(&db)
                .await
                .expect("zzz order row");
        assert_eq!(status, "filled", "模拟成交后订单状态 filled");

        let (fill_price, quantity, commission, tax): (Decimal, Decimal, Decimal, Decimal) =
            sqlx::query_as(
                "SELECT price, quantity, commission, tax FROM paper_fill WHERE fill_id = $1",
            )
            .bind(&fill_id)
            .fetch_one(&db)
            .await
            .expect("zzz fill row");
        // 成交价 = 10.00 × (1 + 0.002) = 10.02
        assert_eq!(fill_price, Decimal::new(1002, 2), "买入滑点抬高成交价");
        assert_eq!(quantity, Decimal::from(100));
        // 佣金 = max(1002 × 万2.5, 5) = 5（触发最低佣金）；买入无印花税
        assert_eq!(commission, Decimal::from(5), "小额成交触发最低佣金 5 元");
        assert_eq!(tax, Decimal::ZERO, "买入免印花税");

        // 费用现金扣减独立成笔：cash 100000 - 5
        let (cash, _, _) = account_balances(&db, account_id).await;
        assert_eq!(cash, Decimal::from(99_995), "费用从现金扣除");

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn create_planned_trade_supports_business_date_backfill() {
        let db = test_db().await;
        let account_id = "zzz_test_api3_plan";
        create_zzz_account(&db, account_id).await;

        // 历史重放：显式 trade_date → created_at 为该业务日
        let order_id = create_planned_trade(
            &db,
            &PlannedTrade {
                account_id: account_id.to_string(),
                symbol: "518880.SH".to_string(),
                side: "buy".to_string(),
                target_quantity: Decimal::from(10),
                target_price: Decimal::new(7000, 2),
                price_upper_limit: None,
                price_lower_limit: None,
                slippage_pct: 0.0,
                target_value: Decimal::from(700),
                reason: None,
                strategy_version_id: None,
                trade_date: Some(chrono::NaiveDate::from_ymd_opt(2026, 6, 15).unwrap()),
            },
        )
        .await
        .expect("planned trade with date");
        // biz_timestamp 以本地时区（Asia/Shanghai）零点写入；读回须按同侧时区取日期
        // （裸 ::date 走连接会话时区 UTC 会少一天——report 空日测试同源坑）。
        let created_on: chrono::NaiveDate = sqlx::query_scalar(
            "SELECT (created_at AT TIME ZONE 'Asia/Shanghai')::date FROM paper_order WHERE order_id = $1",
        )
        .bind(&order_id)
        .fetch_one(&db)
        .await
        .expect("zzz order row");
        assert_eq!(
            created_on,
            chrono::NaiveDate::from_ymd_opt(2026, 6, 15).unwrap(),
            "历史重放订单应落业务日期"
        );

        // 常规调仓：无 trade_date → DB 默认 now()
        let order_id = create_planned_trade(
            &db,
            &PlannedTrade {
                account_id: account_id.to_string(),
                symbol: "518880.SH".to_string(),
                side: "sell".to_string(),
                target_quantity: Decimal::from(10),
                target_price: Decimal::new(7000, 2),
                price_upper_limit: None,
                price_lower_limit: None,
                slippage_pct: 0.0,
                target_value: Decimal::from(700),
                reason: None,
                strategy_version_id: None,
                trade_date: None,
            },
        )
        .await
        .expect("planned trade without date");
        let created_at: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT created_at FROM paper_order WHERE order_id = $1")
                .bind(&order_id)
                .fetch_one(&db)
                .await
                .expect("zzz order row");
        let age = chrono::Utc::now()
            .signed_duration_since(created_at)
            .num_seconds()
            .abs();
        assert!(
            age <= 60,
            "无 trade_date 时 created_at 取当前时刻: 偏差 {age}s"
        );

        cleanup_account(&db, account_id).await;
    }

    #[tokio::test]
    async fn try_auto_repay_skips_when_cash_within_reserve() {
        let db = test_db().await;
        let account_id = "zzz_test_api3_autorepay_hold";
        create_zzz_account(&db, account_id).await;
        execute_margin_borrow(&db, account_id, Decimal::from(300), "zzz 借款")
            .await
            .expect("borrow");

        // cash 压到 reserve 之下：100300 - 96000 = 4300 < 5000
        sqlx::query("UPDATE paper_account SET cash = 4300 WHERE paper_account_id = $1")
            .bind(account_id)
            .execute(&db)
            .await
            .expect("drain cash");
        let result = try_auto_repay(&db, account_id)
            .await
            .expect("auto repay skip");
        assert!(result.is_none(), "现金低于保留额不应自动还款");
        let (_, margin, _) = account_balances(&db, account_id).await;
        assert_eq!(margin, Decimal::from(300), "融资保持不动");

        // 不存在的账户同样返回 None（查询空 → Ok(None)）
        let result = try_auto_repay(&db, "zzz_test_api3_no_such_account")
            .await
            .expect("missing account ok");
        assert!(result.is_none());

        cleanup_account(&db, account_id).await;
    }
}
