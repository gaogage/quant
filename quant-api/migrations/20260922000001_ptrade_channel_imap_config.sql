-- 任务76: PTrade 通道邮件配置迁 DB(执行器级配置)
-- 背景: 邮箱配置跟随执行器通道走(执行器 send_email 的 EMAIL_FROM = fetch 拉取邮箱,
-- 一一对应), 多通道可能各异; .env 全局唯一且授权码有 git 跟踪泄露风险。
-- 默认端点值只在 DDL 层, 代码零硬编码。
ALTER TABLE ptrade_channel_config
    ADD COLUMN IF NOT EXISTS imap_host text NOT NULL DEFAULT 'imap.qq.com',
    ADD COLUMN IF NOT EXISTS imap_port integer NOT NULL DEFAULT 993,
    ADD COLUMN IF NOT EXISTS imap_user text,
    ADD COLUMN IF NOT EXISTS imap_pwd text;

COMMENT ON COLUMN ptrade_channel_config.imap_host IS '通道回报邮箱 IMAP 主机(默认 QQ 邮箱)';
COMMENT ON COLUMN ptrade_channel_config.imap_port IS '通道回报邮箱 IMAP 端口(默认 993 SSL)';
COMMENT ON COLUMN ptrade_channel_config.imap_user IS 'fetch 拉取账号, 与该通道执行器 send_email 的 EMAIL_FROM 一致; NULL=未配置告警跳过';
COMMENT ON COLUMN ptrade_channel_config.imap_pwd IS '邮箱授权码(QQ 签发的 IMAP 授权码, 非登录密码); NULL=未配置告警跳过';
