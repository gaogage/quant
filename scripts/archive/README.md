# 已归档脚本

## check_schema_alignment.py（2026-09-19 归档）

2026 年 5-7 月的"实现完整性字符串锚定"守门脚本：要求关键实现的代码串
（如 multi_factor_value INSERT 带 available_at）必须存在于指定文件。

**归档原因**：字符串锚定与架构重构天然冲突——P0-P3 重构（combine 仓储
分离、factors 目录化、P2 九刀拆分）使几十项锚定大面积漂移，8 项 FAIL
均为检查过时而非代码回归（逐项甄别：旧 VALUES 参数错位等真坏模式已
不存在于代码）。真正的回归守门已由更强机制承担：

- audit hash 四维基线（equity/signal/config/data_version）
- 全量 1454 测试 + 60 个显式标注的集成/慢测
- 全 workspace strict clippy + pre-push 快门禁

若需重启实现完整性检查，应基于语义锚定（函数签名/类型）而非字符串。
