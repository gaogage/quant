-- 2026-09-28 追单机制跨系统配置化（任务83 第二批）：app_config 四键
-- 链路: quant DB(app_config) → signal_export 信号 JSON chase 块 → PTrade 执行器读取
-- 执行器侧默认值与此一致(无 chase 块信号时兜底)——调参改 DB 当晚信号即生效,
-- 双执行器(生产模拟 Rust 自执行 / PTrade Python)读同一信号源杜绝漂移。
-- 注意: 执行器**代码逻辑**变更仍须手动上传 PTrade 并重新部署(架构约束),
-- 本配置化只覆盖参数调优场景。

INSERT INTO app_config (config_key, config_value, description) VALUES
    ('chase.window_start', '"11:35"', '追单窗口起点(执行器滞后单撤挂分离检查)'),
    ('chase.window_end', '"14:50"', '追单窗口终点(兼尾盘兜底时刻)'),
    ('chase.min_age_min', '15', '挂单最小年龄(分钟), 低于不追——给市场时间消化'),
    ('chase.max_rounds', '3', '每标的最多追单轮次(让价上限), 超限留次日防大跌日追价损失')
ON CONFLICT (config_key) DO NOTHING;
