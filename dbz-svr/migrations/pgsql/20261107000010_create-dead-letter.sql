-- dbz 死信与重试队列
--
-- 背景：HttpSink 只把 IOException / InterruptedException 当成可重试异常；对端返回
-- HTTP 4xx/5xx 时只记日志并照常提交偏移量。magic 脚本的业务失败又是
-- HTTP 200 + {"result":-2}，Connect 视为成功。因此重试与死信都必须由脚本显式
-- 写入本表、由 dbz 的重试 worker 驱动，不能依赖 Connect 的 DLQ 或 errors.tolerance。

CREATE TABLE public.dbz_dead_letter (
    id bigint NOT NULL,
    connector_type smallint NOT NULL,
    connector_id bigint NOT NULL,
    topic character varying(255) NOT NULL,
    record_key character varying(512) DEFAULT '' NOT NULL,
    payload jsonb NOT NULL,
    failure_category smallint NOT NULL,
    failure_detail text DEFAULT '' NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    first_seen_ms bigint NOT NULL,
    last_seen_ms bigint NOT NULL,
    status smallint DEFAULT 0 NOT NULL,
    handled_by bigint,
    handled_ms bigint,
    remark character varying(500),
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL
);

COMMENT ON TABLE public.dbz_dead_letter IS '死信记录（最终处理态，重试耗尽或确定性失败后写入）';

COMMENT ON COLUMN public.dbz_dead_letter.connector_type IS '连接器类型
@enum DeadLetterConnectorType default=Sink
  1=Source  上游连接器
  2=Sink    下游连接器';

COMMENT ON COLUMN public.dbz_dead_letter.connector_id IS '连接器记录 ID（按 connector_type 指向上游或下游表）';

COMMENT ON COLUMN public.dbz_dead_letter.failure_category IS '失败分类
@enum DeadLetterFailureCategory default=Deterministic
  1=Deterministic  确定性失败（参数校验不通过、唯一键冲突、外键缺失等）
  2=Transient      传输类失败（连接失败、5xx、超时）
  3=Unknown        响应异常（result 非成功且无法归类、响应无法解析）';

COMMENT ON COLUMN public.dbz_dead_letter.status IS '处理状态
@enum DeadLetterStatus default=Pending
  0=Pending        待处理
  1=Retried        已重投
  2=Discarded      已废弃
  3=ManualHandled  已人工处理';

CREATE UNIQUE INDEX ux_dbz_dead_letter_key ON public.dbz_dead_letter
    (connector_type, connector_id, topic, record_key);
CREATE INDEX ix_dbz_dead_letter_connector_status_seen ON public.dbz_dead_letter
    (connector_id, status, last_seen_ms);

CREATE TABLE public.dbz_dead_letter_retry (
    id bigint NOT NULL,
    connector_type smallint NOT NULL,
    connector_id bigint NOT NULL,
    topic character varying(255) NOT NULL,
    record_key character varying(512) DEFAULT '' NOT NULL,
    payload jsonb NOT NULL,
    failure_category smallint NOT NULL,
    failure_detail text DEFAULT '' NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_at_ms bigint NOT NULL,
    last_error text,
    first_seen_ms bigint NOT NULL,
    last_seen_ms bigint NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL
);

COMMENT ON TABLE public.dbz_dead_letter_retry IS '传输类失败的重试队列（由 dbz 后台 worker 按退避策略重投）';

CREATE UNIQUE INDEX ux_dbz_dead_letter_retry_key ON public.dbz_dead_letter_retry
    (connector_type, connector_id, topic, record_key);
CREATE INDEX ix_dbz_dead_letter_retry_next_at ON public.dbz_dead_letter_retry (next_at_ms);
