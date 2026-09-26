//! 复权空间门禁测试（2026-09-26 用户定版：复权体系是量化系统的架构基础）
//!
//! 复权两空间一桥架构（SKILL.md §复权体系）：
//! - 信号层：后复权 `market_stock_daily_bar_adj` 视图（因子/MVO/回测价格序列）
//! - 账户层：raw 真实价（持仓/成交/估值/NAV）
//! - 桥：adj_factor（raw × factor = adj）
//! - **红线：跨空间比率计算须在同一空间内完成，禁止 raw 价除以后复权价**
//!
//! 本测试守护全系统 SQL 的空间归属正确性：
//! 1. 静态扫描所有 SQL 字符串中的 `bar_adj` JOIN 与 raw 价格列的交叉使用
//! 2. 验证关键函数的 SQL 模板使用正确的价格空间
//! 3. DB 实证：同日 raw/adj close 差异检测（若有 adj 行且 close 不同 → 需人工确认空间）

#[cfg(test)]
mod adjustment_space_guard {
    use std::fs;

    /// 扫描指定 Rust 源文件中的 SQL 字符串，检测跨空间除法模式。
    /// 模式：`price::double precision / NULLIF(bar.close` 且 bar JOIN 的是 adj 视图。
    fn scan_file_for_cross_space_division(path: &str) -> Vec<String> {
        let content = fs::read_to_string(path).unwrap_or_default();
        let lines: Vec<&str> = content.lines().collect();
        let mut violations = Vec::new();

        // 策略：找 "JOIN market_stock_daily_bar_adj" 的行号，
        // 向后搜索 20 行内的 "/ bar.close" 或 "/ NULLIF(bar.close" 模式
        for (i, line) in lines.iter().enumerate() {
            if line.contains("JOIN market_stock_daily_bar_adj") {
                // 检查是否有注释说明这是有意的同空间操作
                let context_start = i.saturating_sub(5);
                let context: String = lines[context_start..i].join("\n");
                let has_intentional_note = context.contains("同空间")
                    || context.contains("复权架构修复")
                    || context.contains("2026-09-26");

                // 向后搜索 20 行
                for (j, l) in lines.iter().enumerate().skip(i).take(20) {
                    // 检测跨空间除法模式
                    if (l.contains("/ bar.close") || l.contains("/ NULLIF(bar.close"))
                        && !has_intentional_note
                    {
                        violations.push(format!(
                            "{}:{} → adj JOIN 后 {} 行出现跨空间除法: {}",
                            path,
                            i + 1,
                            j - i,
                            l.trim()
                        ));
                        break;
                    }
                    // 检测 raw 价格列与 adj JOIN 的交叉
                    if l.contains("price") && l.contains("bar.close") && !has_intentional_note {
                        violations.push(format!(
                            "{}:{} → adj JOIN 后 {} 行出现 raw price × bar.close: {}",
                            path,
                            i + 1,
                            j - i,
                            l.trim()
                        ));
                        break;
                    }
                }
            }
        }
        violations
    }

    #[test]
    fn no_cross_space_division_in_backfill_sql() {
        let files = [
            "src/routes/factors/backfill/sql.rs",
            "src/routes/factors/backfill/specs.rs",
            "src/routes/factors/icir_materialize.rs",
            "src/routes/factors/crud.rs",
        ];
        let mut all_violations = Vec::new();
        for f in &files {
            all_violations.extend(scan_file_for_cross_space_division(f));
        }
        assert!(
            all_violations.is_empty(),
            "复权空间门禁违规（跨空间除法 detected）:\n{}\n\
             修复方式：raw 价格列与 adj close 的比率计算必须改用同空间（raw 表或 adj 视图），\
             或在 JOIN 附近添加「同空间」注释说明豁免理由",
            all_violations.join("\n")
        );
    }

    #[test]
    fn no_cross_space_division_in_engine_and_signal() {
        let files = [
            "src/routes/rebalance.rs",
            "src/routes/mvo_engine.rs",
            "src/routes/paper.rs",
            "src/routes/backtest.rs",
            "src/routes/signal_export.rs",
        ];
        let mut all_violations = Vec::new();
        for f in &files {
            all_violations.extend(scan_file_for_cross_space_division(f));
        }
        assert!(
            all_violations.is_empty(),
            "复权空间门禁违规（引擎/信号层）:\n{}",
            all_violations.join("\n")
        );
    }

    /// 直接验证 block_trade SQL 已修复：bar JOIN 是 raw 表不是 adj 视图
    #[test]
    fn block_trade_sql_file_uses_raw_table() {
        // 集成测试无法直接调用 pub(crate) 函数——通过文件内容检查
        let content =
            fs::read_to_string("src/routes/factors/backfill/sql.rs").expect("read sql.rs");
        // 找 block_trade 函数段
        let block_trade_start = content
            .find("fn phase7_block_trade_window_backfill_sql")
            .expect("block_trade fn exists");
        let block_trade_end = content[block_trade_start..]
            .find(
                "
}",
            )
            .map(|i| block_trade_start + i)
            .unwrap_or(content.len());
        let segment = &content[block_trade_start..block_trade_end];
        assert!(
            segment.contains("LEFT JOIN market_stock_daily_bar bar"),
            "block_trade SQL 必须 JOIN raw 表（大宗 price 是 raw 成交价）"
        );
        assert!(
            !segment.contains("JOIN market_stock_daily_bar_adj"),
            "block_trade SQL 禁止 JOIN adj 视图（跨空间除法 bug 已修复 2026-09-26）"
        );
    }

    /// DB 实证：检测是否有 adj 行存在但 raw close ≠ adj close（正常，后复权应有差异）
    /// 此测试验证的是：adj 视图确实是复权空间（非 raw 直通），确保空间分离有效
    #[tokio::test]
    async fn adjustment_view_produces_different_prices_from_raw() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://gaocheng@localhost/quant".into());
        let db = sqlx::PgPool::connect(&url).await.expect("test db connect");

        let diff_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM market_stock_daily_bar_adj a
             JOIN market_stock_daily_bar b ON b.symbol = a.symbol AND b.trade_date = a.trade_date
             WHERE a.trade_date = (SELECT MAX(trade_date) FROM market_stock_daily_bar)
               AND b.close > 0 AND ABS(a.close - b.close) > 0.001",
        )
        .fetch_one(&db)
        .await
        .expect("diff query");

        // adj 视图必须产生不同的价格（复权因子 ≠ 1.0 的股票存在）
        assert!(
            diff_count.0 > 0,
            "adj 视图应产生与 raw 不同的价格（复权因子存在的证据）——\
             如果全部相同说明复权体系失效"
        );
    }
}
