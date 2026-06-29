# SDD 进度账本 — 回放/实盘逻辑统一与策略配置化重构

计划: /Users/gaocheng/workspace/docs/superpowers/plans/2026-06-29-回放实盘逻辑统一与策略配置化重构.md
spec:  /Users/gaocheng/workspace/docs/projects/quant/tasks/quant/44-回放实盘逻辑统一与策略配置化重构.md

BASE(quant-api dev): 356fb21
BASE(docs dev):      a620b15

## 计划修订(2026-06-29)
用户选"按 spec 统一口径"。Task 5 改目标持仓驱动增量调仓(删全量 upsert);新增 mark_to_market 每日盯市;
Task 8 回放绩效基于 current_nav(非 load_a_share_daily 累乘),循环:select→rebalance→mtm→update_nav→读nav算收益→写snapshot;
回放账号重置(清持仓,cash/current_nav=initial_capital)。详见计划文件"计划修订"节。

## 任务状态
- [x] Task 1: complete (docs commit a620b15..5b58bd5; DB 字段已加并验证 v19=0.12 v21/v21_lev=0.06) — 直接执行(DB运维无代码可审)
- [x] Task 2: complete (commits 356fb21..2d5e069, review clean) — 直接执行(子代理分类器临时故障;3行可见性机械改动,编译通过)
- [x] Task 3: complete (commits 2d5e069..99e22ee, review clean) — 直接执行(骨架按计划落,backtest_position 表列已核对一致,编译通过)
- [x] Task 4: complete (commits 99e22ee..8af431a, review clean) — 直接执行(TDD:计划测试用 from_f64_retain 有浮点噪声致失败,改 Decimal::new(2,3) 精确构造后 2 测试 PASS;execute_simulated_trade 注入滑点,编译通过)
- [x] Task 5: complete (commits 8af431a..HEAD, review clean) — 直接执行(分类器临时故障;实现 rebalance_account 增量调仓+mark_to_market+apply_fill_to_position,自审发现并修复 cash/margin 流转缺失:买扣 cash 不足自动融资 margin+=缺口,卖 cash+=fill_amount,编译+2测试 PASS)
- [x] Task 6: complete (commits e168be5..HEAD, review clean) — 子代理(haiku)执行 4 处改动(struct 字段+default 函数、Default panic、load_strategy_config panic+SQL 追加 dynamic_target_floor、无 strategy_version_id 跳过);主代理补修行445单测(default→test_strategy_config 字面量)+ 行2265 函数参数 sc→_sc(shadow 后未用);编译+7测试 PASS
- [ ] Task 7: scheduler 建仓段委托 + 修 NAV bug
- [ ] Task 8: 回放逐日真实建仓(绩效基于盯市 current_nav)
- [ ] Task 9: paper_replay 去版本化重命名
- [ ] Task 10: mvo_engine 去版本化重命名 + 端到端验证
- [ ] Task 11: 数据前提 + 文档收尾

## Minor findings(待最终整支审查 triage)
- **行768/868/556/1637 仍硬编码 `"v19"`**:Default panic 后,若 DB 无 strategy_id="v19" active 记录会 panic。行868 的 sc 参数已改 _sc(未用),Task 7/10 清理函数签名时一并处理。行556(equity_curve_update 定时任务)、行1637 独立调用,依赖 DB 有 v19 记录(数据前提 Task 11 验证)。
