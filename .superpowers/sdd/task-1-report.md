# P4.2b Task 1 报告:大盘动量反转交互特征 backfill

> 2026-07-03 | P4.2b Task1 | 状态:DONE

## 交付

`large_cap_mom_rev_daily_std` 因子 backfill 管线完成,4 处改动:

1. **枚举变体** `Phase7BackfillFactorKind::LargeCapMomentumReversal`(factors.rs:608)
2. **SQL 函数** `phase7_large_cap_momentum_reversal_backfill_sql`(factors.rs:11607)
   - 大盘股池(start_date 当日 total_mv>500亿 固定分桶,PIT)
   - CUME_DIST(reversal_5d) × CUME_DIST(momentum_60d) 交互
   - percent_rank 截面标准化,available_at=trade_date
3. **specs/run/handler/路由接线**(factors.rs:2519/9449/5456,main.rs:539)
   - 严格复用 phase7_price_volume 模式
   - task_type=p42b_large_cap_momentum_reversal_backfill
   - 路由 POST /api/v1/quant/factors/p42b-large-cap-momentum-reversal-backfill/background

## 实跑验证(2024 全年)

- backfill 30s 完成,163191 条落库,694 只大盘股
- 任务状态 completed,progress=100

## 分桶 IC 验证(P4.2a API)

| 桶 | mean_rank_ic | rank_ic_ir | n_obs |
|----|-------------:|-----------:|------:|
| **overall** | **+0.0179** | +0.089 | 162516 |
| large | +0.0123 | +0.054 | 58495 |
| mid | +0.0233 | +0.120 | 98002 |
| small | -0.0135 | -0.048 | 6019 |

**关键结论**:新因子全市场 rank_ic +0.0179(正向),大盘段 +0.0123(正向)。
**达成 P4.2b 目标**:构建正向 alpha 特征,补偿生产 combo 大盘段 ascending 失效
(生产 combo 大盘 IC -0.0238 负向,ascending 选低分有效但负向;
此 overlay 因子大盘 IC +0.0123 正向,descending 选高分有效,方向互补)。

## SQL 优化+逻辑修复

实跑发现两个问题并修复:
1. **性能**:priced CTE 无日期下限,扫全历史 LAG 窗口致 4min+ 卡死。
   加 `trade_date >= start - 120 days`(足够 momentum_60d 前置)。
2. **逻辑**:large_cap CTE 原用 `DISTINCT ON(symbol)` 每股只保留一行,丢失时间维度。
   改为 start_date 当日市值固定分桶(与 P4.1c 口径一致),JOIN large_cap_symbols,
   避免逐日 LATERAL 市值查询开销。

## commits

- 236038a: 枚举+SQL+match 核心
- 178eed0: specs/run/handler/路由接线
- (待提交): SQL 优化+逻辑修复

## 关注点

无。Task 2(防御板块低波质量)可按同样模式推进。
