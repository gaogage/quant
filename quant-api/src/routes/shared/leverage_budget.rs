//! 杠杆与预算领域计算（R6 领域服务提取，2026-09-29）。
//!
//! 任务 85（杠杆预平衡）/任务 86（pro-rata 预算分配）两轮改造后，杠杆与预算的
//! 决策计算原本内联在 `rebalance_account` 路由函数中。本模块将其提取为纯函数：
//! - 无 DB 依赖，决策逻辑可直接单测（原行为测试是 SQL 口径验证型，绕着测）
//! - 路由层保留 DB 编排（配置读取/维保查询/强平动作），计算全部委托此处
//! - 数值路径与提取前逐字节一致（纯搬移，行为测试护航）
//!
//! 口径约定（与既有实现一致，勿漂移）：
//! - 实际杠杆 = (NAV + margin) / NAV
//! - 授信上限 credit_cap = NAV × (目标杠杆 - 1)
//! - 维保 = 总资产 / margin = 杠杆 L 对应 L/(L-1)

use rust_decimal::Decimal;

/// 维保门控三态判定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceAction {
    /// 正常放行
    Normal,
    /// 警戒区间：禁买不强平
    WarnBlockBuy,
    /// 平仓线以下：强平 + 禁买
    Liquidate,
}

/// 维保门控判定（纯函数，DB 查询与强平动作留在路由层）。
///
/// `maint` 为 Infinity（无融资账户）时正常放行。
pub fn resolve_maintenance_action(maint: f64, liq_thr: f64, warn_thr: f64) -> MaintenanceAction {
    if maint.is_infinite() {
        return MaintenanceAction::Normal;
    }
    if maint < liq_thr {
        MaintenanceAction::Liquidate
    } else if maint < warn_thr {
        MaintenanceAction::WarnBlockBuy
    } else {
        MaintenanceAction::Normal
    }
}

/// 杠杆预平衡还款额计算（任务 85 Step 5.5 核心）。
///
/// 实际杠杆 > 目标×(1+带宽) → 需还款 = margin - NAV×(目标-1)（下限 0）。
/// 带宽 ≤0（关闭）/无融资/非杠杆账户/NAV 非正 → 0（no-op）。
pub fn compute_required_repay(nav: f64, margin: f64, target_lev: f64, band: f64) -> Decimal {
    if nav <= 0.0 || margin <= 0.0 || target_lev <= 1.0 || band <= 0.0 {
        return Decimal::ZERO;
    }
    let actual_lev = (nav + margin) / nav;
    let upper = target_lev * (1.0 + band);
    if actual_lev > upper {
        let repay = (margin - nav * (target_lev - 1.0)).max(0.0);
        Decimal::from_f64_retain(repay).unwrap_or(Decimal::ZERO)
    } else {
        Decimal::ZERO
    }
}

/// 融资授信预算池（任务 86 前置口径）。
///
/// `buy_budget = cash + max(credit_cap - margin - required_repay, 0)`，
/// 其中 `credit_cap = max(NAV × (目标杠杆-1), 0)`。还款约束先冲抵授信余量。
pub fn compute_buy_budget(
    cash: Decimal,
    nav: Decimal,
    margin: Decimal,
    target_lev: Decimal,
    required_repay: Decimal,
) -> Decimal {
    let credit_cap = (nav * (target_lev - Decimal::ONE)).max(Decimal::ZERO);
    cash + (credit_cap - margin - required_repay).max(Decimal::ZERO)
}

/// pro-rata 统一缩放系数（任务 86 核心）。
///
/// 总买入需求超出可用预算 → k = 可用/需求（等比缩放，组合结构保真）；
/// 预算充足 → 1（与现状逐字节等价，零回归）。
pub fn compute_buy_scale(available: Decimal, total_need: Decimal) -> Decimal {
    if total_need > available && total_need > Decimal::ZERO {
        available / total_need
    } else {
        Decimal::ONE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 维保三态：Infinity 放行 / 平仓线强平 / 警戒禁买 / 正常
    #[test]
    fn maintenance_action_three_states() {
        assert_eq!(
            resolve_maintenance_action(f64::INFINITY, 1.3, 1.5),
            MaintenanceAction::Normal,
            "无融资账户放行"
        );
        assert_eq!(
            resolve_maintenance_action(1.2, 1.3, 1.5),
            MaintenanceAction::Liquidate
        );
        assert_eq!(
            resolve_maintenance_action(1.4, 1.3, 1.5),
            MaintenanceAction::WarnBlockBuy
        );
        assert_eq!(
            resolve_maintenance_action(2.0, 1.3, 1.5),
            MaintenanceAction::Normal
        );
        // 边界: 恰在阈值上=不属于该档(严格小于)
        assert_eq!(
            resolve_maintenance_action(1.3, 1.3, 1.5),
            MaintenanceAction::WarnBlockBuy
        );
    }

    /// 超带宽(2.5x > 2.3) → 还款压回 2.0; 带宽内 → 0
    #[test]
    fn required_repay_over_and_within_band() {
        // nav=100, margin=150 → 杠杆 2.5 > 2.3 → repay = 150-100 = 50
        let r = compute_required_repay(100.0, 150.0, 2.0, 0.15);
        assert!((r - Decimal::from(50)).abs() < Decimal::new(1, 6));
        // nav=100, margin=107 → 杠杆 2.07 < 2.3 → 0 (2026-03 闪崩实证口径)
        assert_eq!(
            compute_required_repay(100.0, 107.0, 2.0, 0.15),
            Decimal::ZERO
        );
    }

    /// no-op 分支: 带宽0/无融资/非杠杆/NAV非正
    #[test]
    fn required_repay_noop_branches() {
        assert_eq!(
            compute_required_repay(100.0, 150.0, 2.0, 0.0),
            Decimal::ZERO,
            "带宽0=关闭"
        );
        assert_eq!(
            compute_required_repay(100.0, 0.0, 2.0, 0.15),
            Decimal::ZERO,
            "无融资"
        );
        assert_eq!(
            compute_required_repay(100.0, 150.0, 1.0, 0.15),
            Decimal::ZERO,
            "目标杠杆1=无杠杆"
        );
        assert_eq!(
            compute_required_repay(0.0, 150.0, 2.0, 0.15),
            Decimal::ZERO,
            "NAV非正"
        );
        assert_eq!(
            compute_required_repay(-5.0, 150.0, 2.0, 0.15),
            Decimal::ZERO,
            "NAV负"
        );
    }

    /// 预算池: 满杠杆贴线 → 池余≈cash; 有还款约束 → 先冲抵授信
    #[test]
    fn buy_budget_credit_cap_and_repay() {
        let d = |x: f64| Decimal::from_f64_retain(x).unwrap();
        // nav=100, lev=2 → cap=100; margin=99 → 池余=1; cash=10 → budget=11
        let b = compute_buy_budget(d(10.0), d(100.0), d(99.0), d(2.0), Decimal::ZERO);
        assert!((b - d(11.0)).abs() < Decimal::new(1, 6));
        // required_repay=30 → 池余 max(1-30,0)=0 → budget=cash=10
        let b2 = compute_buy_budget(d(10.0), d(100.0), d(99.0), d(2.0), d(30.0));
        assert_eq!(b2, d(10.0));
        // 非杠杆账户(lev=1) → cap=0 → budget=cash
        let b3 = compute_buy_budget(d(10.0), d(100.0), d(0.0), d(1.0), Decimal::ZERO);
        assert_eq!(b3, d(10.0));
    }

    /// pro-rata: 需求超预算 → k<1; 充足 → 1
    #[test]
    fn buy_scale_prata_and_full() {
        let d = |x: f64| Decimal::from_f64_retain(x).unwrap();
        assert_eq!(
            compute_buy_scale(d(100.0), d(80.0)),
            Decimal::ONE,
            "预算足不缩放"
        );
        let k = compute_buy_scale(d(80.0), d(100.0));
        assert!((k - d(0.8)).abs() < Decimal::new(1, 6), "k=0.8: {k}");
        // 需求=2151801 可用=2000000 → k=0.9295...(任务86生产日志实证口径)
        let k2 = compute_buy_scale(d(2_000_000.0), d(2_151_801.0));
        assert!(
            (k2 - d(0.92953)).abs() < Decimal::new(5, 4),
            "k≈0.930: {k2}"
        );
        // 零需求 → 1(防除零)
        assert_eq!(compute_buy_scale(d(0.0), d(0.0)), Decimal::ONE);
    }
}
