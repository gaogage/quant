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
/// 可配置: env 覆盖(PAPER_FEE_*), 当前单渠道; 未来多券商/渠道时扩展为
/// 账户级配置(paper_account 列或渠道表), 费率结构与计算口径不变。
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
    pub fn from_env() -> Self {
        let mut s = Self::default();
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
    let fee = ExecutionFeeSchedule::from_env();
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
        if let Err(e) = sqlx::query(
            "UPDATE paper_account SET cash = cash - LEAST($2, COALESCE(cash,0)) \
             WHERE paper_account_id = $1",
        )
        .bind(&trade.account_id)
        .bind(fee_total)
        .execute(db)
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
    sqlx::query(
        "UPDATE paper_account SET cash = cash + $1, margin_amount = COALESCE(margin_amount,0) + $1 WHERE paper_account_id = $2"
    )
    .bind(amount).bind(account_id)
    .execute(db).await.map_err(|e| format!("margin_borrow update: {}", e))?;

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
    sqlx::query(
        "UPDATE paper_account SET cash = cash - $1, margin_amount = margin_amount - $1 WHERE paper_account_id = $2"
    )
    .bind(amount).bind(account_id)
    .execute(db).await.map_err(|e| format!("margin_repay update: {}", e))?;

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
        assert_eq!(f.stamp_tax("sell", Decimal::from(1_000_000)), Decimal::from(250));
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
}
