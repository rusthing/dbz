-- 死信收窄为「仅下游连接器」
--
-- 上游连接器没有可重投的 HTTP 地址，页面也不再提供上游类型选项。
-- 清理库里已有的上游死信与重试记录（当前为 0 条）；用 DELETE 天然幂等。

DELETE FROM dbz_dead_letter WHERE connector_type = 1;
DELETE FROM dbz_dead_letter_retry WHERE connector_type = 1;
