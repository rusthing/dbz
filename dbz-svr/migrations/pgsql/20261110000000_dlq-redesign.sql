-- 死信机制改为 Kafka Connect 原生 DLQ。
--
-- 旧设计：magic 脚本显式上报 → dbz_dead_letter_retry 退避重投 → dbz_dead_letter 终态。
-- 新设计：连接器部署时注入 errors.tolerance / errors.deadletterqueue.*，坏记录由 Connect
--         写进 DLQ topic；dbz 只把 DLQ 记录同步成页面索引，并保存人工处置结论。
--
-- 按需求「不用管旧数据」，旧索引记录直接清空，重启后由 DLQ topic 重新同步。
-- 初始基线（20261109000000_create-table.sql）仍保留旧表定义，这是数据库演进的正常形态：
-- 已执行过的迁移不回改，本文件负责把跑过的库收敛到当前设计。

-- 自建重试队列整体下线。连带删除 20261109000001_create-index.sql 建的
-- ix_dbz_dead_letter_retry_next_at，不需要单独 DROP INDEX。
DROP TABLE IF EXISTS dbz_dead_letter_retry;

-- 旧幂等键 (connector_type, connector_id, topic, record_key) 会把同一条记录的多轮失败
-- 合并成一行，且会覆盖人工结论；DLQ 里每条 Kafka 记录都是独立事件，改用
-- (connector_id, topic, partition, offset)。带上 connector_id 是因为同一源 topic 可能
-- 被多条 sink 连接器订阅，同一条源记录在每条连接器上失败都是独立的一条死信。
DELETE FROM dbz_dead_letter;

DROP INDEX IF EXISTS ux_dbz_dead_letter_key;
CREATE UNIQUE INDEX ux_dbz_dead_letter_key
    ON dbz_dead_letter (connector_id, topic, partition, "offset");

COMMENT ON TABLE dbz_dead_letter IS
'死信记录（Kafka Connect DLQ 镜像 + 人工处置）；事实来源是 DLQ topic，本表只做页面索引与人工结论';

COMMENT ON COLUMN dbz_dead_letter.connector_type IS
'连接器类型；Connect 的 DLQ 只适用于 sink，故恒为 2
@enum DeadLetterConnectorType default=Sink
  1=Source  上游连接器（历史取值，不再产生死信）
  2=Sink    下游连接器';

COMMENT ON COLUMN dbz_dead_letter.connector_id IS
'下游连接器记录 ID（由 DLQ 上下文头里的连接器名反查，未知为 0）；与 topic/partition/offset 组成幂等键';

COMMENT ON COLUMN dbz_dead_letter.topic IS
'原始消息所在的源 topic（来自 __connect.errors.topic）；与 connector_id/partition/offset 组成幂等键';

COMMENT ON COLUMN dbz_dead_letter.partition IS
'原始消息所在分区（来自 __connect.errors.partition）；与 connector_id/topic/offset 组成幂等键';

COMMENT ON COLUMN dbz_dead_letter."offset" IS
'原始消息 offset（来自 __connect.errors.offset）；与 connector_id/topic/partition 组成幂等键';

COMMENT ON COLUMN dbz_dead_letter.failure_category IS
'异常分类，由 DLQ 记录头里的异常类名推导，只用于页面展示，不参与重试决策
@enum DeadLetterFailureCategory default=Deterministic
  1=Deterministic  数据/格式/配置类异常（DataException、序列化失败等）
  2=Transient      投递类异常（RetriableException、超时、IO、HTTP 错误）
  3=Unknown        其它无法归类的异常';

COMMENT ON COLUMN dbz_dead_letter.failure_detail IS
'DLQ 上下文原文：异常类名、消息、堆栈与 DLQ 坐标，原样保存不截断';

COMMENT ON COLUMN dbz_dead_letter.status IS
'处理状态
@enum DeadLetterStatus default=Pending
  0=Pending        待处理
  1=Retried        已重投（报文写回源 topic）
  2=Discarded      已废弃
  3=ManualHandled  已人工处理';

COMMENT ON COLUMN dbz_dead_letter.failure_subtype IS
'异常细分子类，由 DLQ 异常消息推导
@enum DeadLetterFailureSubtype default=Other
  0=Other            未细分
  1=FieldValidation  字段不合规，允许修改报文后重投';

COMMENT ON COLUMN dbz_dead_letter.last_retry_message IS
'最近一次人工重投的结果说明（写回源 topic 的结果）';

COMMENT ON COLUMN dbz_dead_letter.reason IS
'给人看的一句失败原因，由后端按异常分类统一生成；原始技术内容在 failure_detail';

COMMENT ON COLUMN dbz_dead_letter.retry_count IS
'保留列：Connect 的重试次数不写进 DLQ，故恒为 0';

COMMENT ON COLUMN dbz_dead_letter.first_seen_ms IS
'Kafka 记录时间戳（毫秒）';

COMMENT ON COLUMN dbz_dead_letter.last_seen_ms IS
'Kafka 记录时间戳（毫秒），与 first_seen_ms 相同';
