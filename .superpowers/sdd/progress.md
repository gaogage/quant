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
- [ ] Task 6: StrategyConfig 配置化
- [ ] Task 7: scheduler 建仓段委托 + 修 NAV bug
- [ ] Task 8: 回放逐日真实建仓(绩效基于盯市 current_nav)
- [ ] Task 9: paper_replay 去版本化重命名
- [ ] Task 10: mvo_engine 去版本化重命名 + 端到端验证
- [ ] Task 11: 数据前提 + 文档收尾

## Minor findings(待最终整支审查 triage)
(暂无)
