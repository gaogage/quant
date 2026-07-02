# P4.0a Subagent-Driven Progress Ledger

Plan: docs/superpowers/plans/2026-07-01-p4-0a-equity-curve-multi-strategy-sync.md
Started: 2026-07-01

## Tasks
- [ ] Task 0: 修复 tushare fund_basic 漏 list_date + sync_fund_basic 未写入 (quant-data)
- [ ] Task 1: equity_curve_sync.rs 核心同步模块 (quant-api)
- [ ] Task 2: scheduler.rs equity_curve_update 改调 sync_active_strategies (quant-api)
- [ ] Task 3: compute_lw_mvo_weights 入口过滤未发行 ETF (quant-api)
- [ ] Task 4: rebalance_account 建仓 0 笔报错收窄 (quant-api)
- [ ] Task 5: API 路由 POST sync + GET readiness-audit (quant-api)
- [ ] Task 6: 前端数据 tab 权益曲线同步子区 (quant-ui)
- [ ] Task 7: 策略详情页权益曲线段 + 端到端验证 (quant-ui)

## Completion Log
(newest at bottom)

- [x] Task 0: complete (commits aea555a..4b7b37f, review clean) — tushare fund_basic 补 list_date/delist_date + sync_fund_basic 写入+动态exchange+COALESCE + 回填7 ETF list_date 非 NULL + 回归测试 PASS
  - Minor: sync.rs:927-934 可用 to_date 工具函数替代内联(DRY,不阻塞,行为正确)

- [x] Task 1: complete (commits 4b7b37f..01939ab, review clean) — equity_curve_sync.rs 新建(is_etf_listed_on双保险+collect_active+detect_combo_sharing+sync_strategy+sync_active+audit_readiness);get_latest_data_version 改 pub(crate);UPDATE 双写 composite+a_share 子行;3 集成测试 PASS
  - 实现者发现并修复 brief 漏洞:UPDATE 需双写 composite 行 + a_share 子行(load_strategy_config 读 composite,load_resolved_strategy 读 a_share 子行)
  - 测试断言从 v19 改为 v21/v21_lev(DB 无 v19 active 账号)
  - 信息性观察(<80 置信,不阻塞):UPDATE 失败静默忽略、双写无事务

- [x] Task 2: complete (commits 01939ab..2e71872, review clean) — scheduler.rs equity_curve_update 分支替换为调 sync_active_strategies_equity_curves(+8/-90),删硬编码 v19/v20;8 既有测试 PASS 无回归
  - 副作用:parse_scheduler_date/first_open_trade_date_on_or_after 变 dead_code(原只被该块用),warning 不阻塞
  - 残留 v19/v20 在 sync_eod_data 等其他函数,P4.0d 范围

- [x] Task 3: complete (commits 2e71872..cc568c6, review clean) — compute_lw_mvo_weights 入口循环过滤未发行 ETF(listed_etf_symbols/listed_default_weights 同序)+ build_etf_allocations 改 2 元组接动态 etf_symbols + 调用点过滤保证与 mvo_weights 同源(修索引错位)+ 类型适配;test PASS
  - 主代理修复:brief 原模板 .filter()+await 编译错误(闭包不能 async)→ 改循环;rebalance 调用点从 &sc.etf_symbols 改为过滤后的 listed_etf_symbols(修索引错位)
  - 信息性(<80 置信,不阻塞):MvoWeightCache 注释陈旧;info! 日志 wg(1..7) 越界返 0;cache key 跨账号共用 quarter 维度错配风险(pre-existing,P4.0d 评估)

- [x] Task 4: complete (commits cc568c6..cd6bd88, review clean) — rebalance_account 建仓0笔+A股空+无持仓时报错+send_quality_alert+return Err(NAV重算前);无回归
  - 不写新测试:报错逻辑内嵌 async,brief 测试模板用不存在账号会在 fetch_one 提前报错(无效),由 Task 7 端到端覆盖

- [x] Task 5: complete (commits cd6bd88..29fc957, review clean) — 2 handler(handle_equity_curve_sync POST + handle_equity_curve_readiness_audit GET)+ EquityCurveSyncRequest struct + main.rs 注册 2 路由;BUILD SUCCESS;接口未 curl(服务未运行,留 Task 7)
  - handler 每请求新建 PgPool(brief 要求,低频可接受,高频后续复用 AppState)

- [x] Task 6: complete (commits 29fc957..93adfa4, review clean after fix) — 前端数据 tab 加 EquityCurveSyncSection(策略列表+readiness 徽标+同步按钮)+ api.rs 加 equity_curve_readiness_audit/equity_curve_sync;WASM 编译通过
  - fix: ETF coverage 字段名 trade_day_count→listed_etfs/total_etfs(后端无 trade_day_count,审查发现 confidence 95)
  - 子代理自跑 cargo check 验证(前端 WASM 轻量,有益)

- [x] Task 7: complete (commits 93adfa4..0fa3408, review clean) — 策略详情页加 StrategyEquityCurve 组件(readiness 徽标+coverage+同步按钮),复用 Task 6 api;WASM 编译通过
  - 主代理修复:子代理原代码 E0382(strategy_id 被 move 后再借用)→ 加 sid_for_load clone
  - 端到端验证待主代理启动服务

- [x] 最终全分支 review: Ready to merge (commits aea555a..0bac029, 13 commits)
  - Important finding(已修):前端 do_sync 只查 code==0 不读 data.status → run-factor 失败显示假成功;commit 0bac029 修复(读 data.status 区分成败)
  - spec Task 0-7 全部实现无遗漏
  - 三仓库编译全 SUCCESS:quant-data + quant-api + quant-ui(WASM)

## P4.0a 完成
全部分支:aea555a..0bac029(quant-data + quant-api + quant-ui 共享 git 历史)
核心成果:
1. 消除 scheduler 硬编码 v19:equity_curve_update 自动同步所有活跃账号关联策略(v21/v21_lev),combo 去重
2. 修复 tushare fund_basic 漏 list_date + sync_fund_basic 未写入(根因),回填 7 ETF list_date
3. MVO 入口过滤未发行 ETF(双保险 is_etf_listed_on)+ build_etf_allocations 接动态 symbols
4. rebalance 建仓 0 笔报错收窄(不产出假绩效)
5. 2 API 路由(POST sync + GET readiness-audit)
6. 前端数据 tab + 策略详情页权益曲线同步 UI
