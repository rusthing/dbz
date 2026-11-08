-- dbz 数据库迁移基线：init-create-table（最终态建表）
--
-- 清库后按 000000 → 000001 → 000002 顺序执行一次建成。
-- 表、列、主键、唯一键、外键、检查约束与注释；普通索引在 init-index.sql。
-- 与线上库 pg_dump -s 归一化后逐行一致。

CREATE TABLE public.dbz_control_plane_edge_layout (
    id bigint NOT NULL,
    edge_id character varying(255) NOT NULL,
    mid_y double precision NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    start_dx double precision DEFAULT 0 NOT NULL,
    end_dx double precision DEFAULT 0 NOT NULL
);

COMMENT ON TABLE public.dbz_control_plane_edge_layout IS '控制面画布连线折点';

COMMENT ON COLUMN public.dbz_control_plane_edge_layout.edge_id IS '连线 ID（由两端节点 key 拼成，如 own:broker:1:connect:2）';

COMMENT ON COLUMN public.dbz_control_plane_edge_layout.mid_y IS '正交走线中间水平段的世界坐标 Y；拖动折点只改这一个值';

COMMENT ON COLUMN public.dbz_control_plane_edge_layout.start_dx IS '上游连接点相对卡片中心的横向偏移，0 表示默认位置';

COMMENT ON COLUMN public.dbz_control_plane_edge_layout.end_dx IS '下游连接点相对卡片中心的横向偏移，0 表示默认位置';

CREATE TABLE public.dbz_control_plane_layout (
    id bigint NOT NULL,
    node_type smallint NOT NULL,
    node_id bigint NOT NULL,
    x double precision NOT NULL,
    y double precision NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL
);

COMMENT ON TABLE public.dbz_control_plane_layout IS '控制面画布节点位置';

COMMENT ON COLUMN public.dbz_control_plane_layout.node_type IS '节点类型
@enum ControlPlaneNodeType default=Broker
  1=Broker   Kafka 集群
  2=Connect  Kafka Connect
  3=Source   上游连接器
  4=Sink     下游连接器';

COMMENT ON COLUMN public.dbz_control_plane_layout.node_id IS '对应业务表的记录 ID';

COMMENT ON COLUMN public.dbz_control_plane_layout.x IS '画布世界坐标 X';

COMMENT ON COLUMN public.dbz_control_plane_layout.y IS '画布世界坐标 Y';

CREATE TABLE public.dbz_dead_letter (
    id bigint NOT NULL,
    connector_type smallint NOT NULL,
    connector_id bigint NOT NULL,
    topic character varying(255) NOT NULL,
    record_key character varying(512) DEFAULT ''::character varying NOT NULL,
    payload jsonb NOT NULL,
    failure_category smallint NOT NULL,
    failure_detail text DEFAULT ''::text NOT NULL,
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
    update_ms bigint NOT NULL,
    failure_subtype smallint DEFAULT 0 NOT NULL,
    pending_payload jsonb,
    payload_edited_by bigint,
    payload_edited_ms bigint,
    last_retry_success boolean,
    last_retry_ms bigint,
    last_retry_message text,
    reason text DEFAULT ''::text NOT NULL,
    partition integer,
    "offset" bigint
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

COMMENT ON COLUMN public.dbz_dead_letter.failure_detail IS '原始失败详情（HTTP 状态与响应体、校验报错原文、RTW 错误码、异常栈摘要等），原样保存不截断';

COMMENT ON COLUMN public.dbz_dead_letter.status IS '处理状态
@enum DeadLetterStatus default=Pending
  0=Pending        待处理
  1=Retried        已重投
  2=Discarded      已废弃
  3=ManualHandled  已人工处理';

COMMENT ON COLUMN public.dbz_dead_letter.failure_subtype IS '确定性失败的细分子类
@enum DeadLetterFailureSubtype default=Other
  0=Other            未细分（唯一键冲突、外键缺失、旧脚本未上报子类等）
  1=FieldValidation  字段不合规（必填缺失、类型不符、长度超限等），允许修改报文后重投';

COMMENT ON COLUMN public.dbz_dead_letter.pending_payload IS '修改后的待投报文（JSONB）；原始报文 payload 永不覆盖，为空表示没有修改过';

COMMENT ON COLUMN public.dbz_dead_letter.payload_edited_by IS '待投报文的最后编辑人 ID';

COMMENT ON COLUMN public.dbz_dead_letter.payload_edited_ms IS '待投报文的最后编辑时间戳（毫秒）';

COMMENT ON COLUMN public.dbz_dead_letter.last_retry_success IS '最近一次人工重投是否成功；为空表示还没有人工重投过';

COMMENT ON COLUMN public.dbz_dead_letter.last_retry_ms IS '最近一次人工重投的时间戳（毫秒）';

COMMENT ON COLUMN public.dbz_dead_letter.last_retry_message IS '最近一次人工重投的结果说明（失败时为对端返回的原因）';

COMMENT ON COLUMN public.dbz_dead_letter.reason IS '给人看的一句失败原因，由后端按分类/子类统一生成；原始技术内容在 failure_detail';

CREATE TABLE public.dbz_dead_letter_retry (
    id bigint NOT NULL,
    connector_type smallint NOT NULL,
    connector_id bigint NOT NULL,
    topic character varying(255) NOT NULL,
    record_key character varying(512) DEFAULT ''::character varying NOT NULL,
    payload jsonb NOT NULL,
    failure_category smallint NOT NULL,
    failure_detail text DEFAULT ''::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_at_ms bigint NOT NULL,
    last_error text,
    first_seen_ms bigint NOT NULL,
    last_seen_ms bigint NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    partition integer,
    "offset" bigint
);

COMMENT ON TABLE public.dbz_dead_letter_retry IS '传输类失败的重试队列（由 dbz 后台 worker 按退避策略重投）';

COMMENT ON COLUMN public.dbz_dead_letter_retry.failure_detail IS '最近一次原始失败详情，原样保存不加工';

COMMENT ON COLUMN public.dbz_dead_letter_retry.reason IS '给人看的一句失败原因，与死信表同一口径，由后端按分类统一生成';

CREATE TABLE public.dbz_delete_audit (
    id bigint NOT NULL,
    node_type character varying(32) NOT NULL,
    node_id bigint NOT NULL,
    node_name character varying(255) NOT NULL,
    operator_id bigint NOT NULL,
    delete_ms bigint NOT NULL
);

COMMENT ON TABLE public.dbz_delete_audit IS '控制面删除审计';

COMMENT ON COLUMN public.dbz_delete_audit.node_type IS '节点类型：broker / connect / source / sink';

COMMENT ON COLUMN public.dbz_delete_audit.operator_id IS '删除人（当前登录用户）';

COMMENT ON COLUMN public.dbz_delete_audit.delete_ms IS '删除时间戳（毫秒）';

CREATE TABLE public.dbz_kafka_broker (
    id bigint NOT NULL,
    name character varying(50) NOT NULL,
    bootstrap_servers character varying(500) NOT NULL,
    security_protocol smallint DEFAULT 0 NOT NULL,
    sasl_mechanism smallint DEFAULT 0 NOT NULL,
    sasl_username character varying(100),
    sasl_password character varying(200),
    run_status smallint DEFAULT 0 NOT NULL,
    remark character varying(50),
    enabled boolean DEFAULT true NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    run_msg character varying(500)
);

COMMENT ON TABLE public.dbz_kafka_broker IS 'Kafka Broker';

COMMENT ON COLUMN public.dbz_kafka_broker.id IS 'ID';

COMMENT ON COLUMN public.dbz_kafka_broker.name IS '名称
@len 1..=50';

COMMENT ON COLUMN public.dbz_kafka_broker.bootstrap_servers IS 'Broker 地址(host:port，多个用逗号分隔)
@len 1..=500';

COMMENT ON COLUMN public.dbz_kafka_broker.security_protocol IS '安全协议
@enum KafkaSecurityProtocol default=Plaintext
  0=Plaintext      明文
  1=SaslPlaintext  SASL 明文（用户名/密码认证）';

COMMENT ON COLUMN public.dbz_kafka_broker.sasl_mechanism IS 'SASL 机制
@enum KafkaSaslMechanism default=None
  0=None         不使用（安全协议为明文时）
  1=Plain        SASL/PLAIN
  2=ScramSha256  SCRAM-SHA-256
  3=ScramSha512  SCRAM-SHA-512';

COMMENT ON COLUMN public.dbz_kafka_broker.sasl_username IS '认证用户名
@len 0..=100';

COMMENT ON COLUMN public.dbz_kafka_broker.sasl_password IS '认证密码(明文存储，读写窗口只对运维开放)
@len 0..=200';

COMMENT ON COLUMN public.dbz_kafka_broker.run_status IS '状态
@enum KafkaBrokerStatus default=None
  0=None           无状态（刚添加记录，或刚改过地址/认证/重新启用，等待下一轮检查）
  1=Normal         正常
  2=Disabled       已禁用
  3=ConnectFailed  连接失败';

COMMENT ON COLUMN public.dbz_kafka_broker.remark IS '备注
@len 0..=50';

COMMENT ON COLUMN public.dbz_kafka_broker.enabled IS '启用';

COMMENT ON COLUMN public.dbz_kafka_broker.creater_id IS '创建者ID';

COMMENT ON COLUMN public.dbz_kafka_broker.create_ms IS '创建时间戳';

COMMENT ON COLUMN public.dbz_kafka_broker.updater_id IS '更新者ID';

COMMENT ON COLUMN public.dbz_kafka_broker.update_ms IS '更新时间戳';

COMMENT ON COLUMN public.dbz_kafka_broker.run_msg IS '检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

CREATE TABLE public.dbz_kafka_connect (
    id bigint NOT NULL,
    name character varying(50) NOT NULL,
    url character varying(100) NOT NULL,
    run_status smallint DEFAULT 0 NOT NULL,
    remark character varying(50),
    enabled boolean DEFAULT true NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    kafka_broker_id bigint,
    run_msg character varying(500)
);

COMMENT ON TABLE public.dbz_kafka_connect IS 'Kafka Connect';

COMMENT ON COLUMN public.dbz_kafka_connect.id IS 'ID';

COMMENT ON COLUMN public.dbz_kafka_connect.name IS '名称';

COMMENT ON COLUMN public.dbz_kafka_connect.url IS '地址';

COMMENT ON COLUMN public.dbz_kafka_connect.run_status IS '状态
0-无状态(刚添加记录)
1-正常
2-已禁用
3-连接失败';

COMMENT ON COLUMN public.dbz_kafka_connect.remark IS '备注';

COMMENT ON COLUMN public.dbz_kafka_connect.enabled IS '启用';

COMMENT ON COLUMN public.dbz_kafka_connect.creater_id IS '创建者ID';

COMMENT ON COLUMN public.dbz_kafka_connect.create_ms IS '创建时间戳';

COMMENT ON COLUMN public.dbz_kafka_connect.updater_id IS '更新者ID';

COMMENT ON COLUMN public.dbz_kafka_connect.update_ms IS '更新时间戳';

COMMENT ON COLUMN public.dbz_kafka_connect.kafka_broker_id IS 'Kafka Broker ID';

COMMENT ON COLUMN public.dbz_kafka_connect.run_msg IS '检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

CREATE TABLE public.dbz_sink_connector (
    id bigint NOT NULL,
    kafka_connect_id bigint NOT NULL,
    source_connector_id bigint NOT NULL,
    name character varying(50) NOT NULL,
    connector_name character varying(50) NOT NULL,
    connector_config text NOT NULL,
    run_status smallint DEFAULT 0 NOT NULL,
    remark character varying(50),
    enabled boolean DEFAULT true NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    run_msg character varying(500),
    inconsistent boolean DEFAULT false NOT NULL
);

COMMENT ON TABLE public.dbz_sink_connector IS '下游连接器';

COMMENT ON COLUMN public.dbz_sink_connector.id IS 'ID';

COMMENT ON COLUMN public.dbz_sink_connector.kafka_connect_id IS 'Kafka Connect ID';

COMMENT ON COLUMN public.dbz_sink_connector.source_connector_id IS '上游连接器ID';

COMMENT ON COLUMN public.dbz_sink_connector.name IS '名称';

COMMENT ON COLUMN public.dbz_sink_connector.connector_name IS '连接器名称';

COMMENT ON COLUMN public.dbz_sink_connector.connector_config IS '连接器配置(JSON)';

COMMENT ON COLUMN public.dbz_sink_connector.run_status IS '连接器统一状态
@enum ConnectorRunStatus default=Unknown
  0=Unknown       未知
  1=Running       运行中
  2=Disabled      已停用
  3=Failed        失败
  4=NotDeployed   仅纳管未部署
  5=Unreachable   不可达（Connect 连不上，保留最后已知状态）
  6=Stopped       已停止（PAUSED / STOPPED）';

COMMENT ON COLUMN public.dbz_sink_connector.remark IS '备注';

COMMENT ON COLUMN public.dbz_sink_connector.enabled IS '启用';

COMMENT ON COLUMN public.dbz_sink_connector.creater_id IS '创建者ID';

COMMENT ON COLUMN public.dbz_sink_connector.create_ms IS '创建时间戳';

COMMENT ON COLUMN public.dbz_sink_connector.updater_id IS '更新者ID';

COMMENT ON COLUMN public.dbz_sink_connector.update_ms IS '更新时间戳';

COMMENT ON COLUMN public.dbz_sink_connector.run_msg IS '检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

COMMENT ON COLUMN public.dbz_sink_connector.inconsistent IS '检测不一致标记：库内配置与 Connect 实际配置不一致，或远端已不存在';

CREATE TABLE public.dbz_source_connector (
    id bigint NOT NULL,
    kafka_connect_id bigint NOT NULL,
    name character varying(50) NOT NULL,
    connector_name character varying(50) NOT NULL,
    connector_config text NOT NULL,
    run_status smallint DEFAULT 0 NOT NULL,
    remark character varying(50),
    enabled boolean DEFAULT true NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    run_msg character varying(500),
    inconsistent boolean DEFAULT false NOT NULL
);

COMMENT ON TABLE public.dbz_source_connector IS '上游连接器';

COMMENT ON COLUMN public.dbz_source_connector.id IS 'ID';

COMMENT ON COLUMN public.dbz_source_connector.kafka_connect_id IS 'Kafka Connect ID';

COMMENT ON COLUMN public.dbz_source_connector.name IS '名称';

COMMENT ON COLUMN public.dbz_source_connector.connector_name IS '连接器名称';

COMMENT ON COLUMN public.dbz_source_connector.connector_config IS '连接器配置(JSON)';

COMMENT ON COLUMN public.dbz_source_connector.run_status IS '连接器统一状态
@enum ConnectorRunStatus default=Unknown
  0=Unknown       未知
  1=Running       运行中
  2=Disabled      已停用
  3=Failed        失败
  4=NotDeployed   仅纳管未部署
  5=Unreachable   不可达（Connect 连不上，保留最后已知状态）
  6=Stopped       已停止（PAUSED / STOPPED）';

COMMENT ON COLUMN public.dbz_source_connector.remark IS '备注';

COMMENT ON COLUMN public.dbz_source_connector.enabled IS '启用';

COMMENT ON COLUMN public.dbz_source_connector.creater_id IS '创建者ID';

COMMENT ON COLUMN public.dbz_source_connector.create_ms IS '创建时间戳';

COMMENT ON COLUMN public.dbz_source_connector.updater_id IS '更新者ID';

COMMENT ON COLUMN public.dbz_source_connector.update_ms IS '更新时间戳';

COMMENT ON COLUMN public.dbz_source_connector.run_msg IS '检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

COMMENT ON COLUMN public.dbz_source_connector.inconsistent IS '检测不一致标记：库内配置与 Connect 实际配置不一致，或远端已不存在';

CREATE TABLE public.debezium_signal (
    id character varying(42) NOT NULL,
    type character varying(32) NOT NULL,
    data character varying(2048)
);

COMMENT ON TABLE public.debezium_signal IS 'Debezium 信号表，用于接收并协调增量快照等控制指令';

COMMENT ON COLUMN public.debezium_signal.id IS '信号唯一标识符，通常为 UUID 或自定义的字符串 ID';

COMMENT ON COLUMN public.debezium_signal.type IS '信号类型，例如 execute-snapshot（触发快照）、log（打印日志）等';

COMMENT ON COLUMN public.debezium_signal.data IS '信号的具体参数，通常为 JSON 格式字符串（如指定要快照的表名、快照类型等）';

ALTER TABLE ONLY public.dbz_kafka_broker
    ADD CONSTRAINT ak_bootstrap_servers_dbz_kafka_broker UNIQUE (bootstrap_servers);

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT ak_connector_name_dbz_sink_connector UNIQUE (connector_name);

ALTER TABLE ONLY public.dbz_source_connector
    ADD CONSTRAINT ak_connector_name_dbz_source_connector UNIQUE (connector_name);

ALTER TABLE ONLY public.dbz_control_plane_edge_layout
    ADD CONSTRAINT ak_edge_dbz_control_plane_edge_layout UNIQUE (edge_id);

ALTER TABLE ONLY public.dbz_kafka_broker
    ADD CONSTRAINT ak_name_dbz_kafka_broker UNIQUE (name);

ALTER TABLE ONLY public.dbz_kafka_connect
    ADD CONSTRAINT ak_name_dbz_kafka_connect UNIQUE (name);

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT ak_name_dbz_sink_connector UNIQUE (name);

ALTER TABLE ONLY public.dbz_source_connector
    ADD CONSTRAINT ak_name_dbz_source_connector UNIQUE (name);

ALTER TABLE ONLY public.dbz_control_plane_layout
    ADD CONSTRAINT ak_node_dbz_control_plane_layout UNIQUE (node_type, node_id);

ALTER TABLE ONLY public.dbz_kafka_connect
    ADD CONSTRAINT ak_url_dbz_kafka_connect UNIQUE (url);

ALTER TABLE ONLY public.debezium_signal
    ADD CONSTRAINT debezium_signal_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_control_plane_edge_layout
    ADD CONSTRAINT pk_dbz_control_plane_edge_layout PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_control_plane_layout
    ADD CONSTRAINT pk_dbz_control_plane_layout PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_delete_audit
    ADD CONSTRAINT pk_dbz_delete_audit PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_kafka_broker
    ADD CONSTRAINT pk_dbz_kafka_broker PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_kafka_connect
    ADD CONSTRAINT pk_dbz_kafka_connect PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT pk_dbz_sink_connector PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_source_connector
    ADD CONSTRAINT pk_dbz_source_connector PRIMARY KEY (id);

CREATE UNIQUE INDEX dbz_kafka_broker_pk ON public.dbz_kafka_broker USING btree (id);

CREATE UNIQUE INDEX dbz_kafka_connect_pk ON public.dbz_kafka_connect USING btree (id);

CREATE UNIQUE INDEX dbz_sink_connector_pk ON public.dbz_sink_connector USING btree (id);

CREATE UNIQUE INDEX dbz_source_connector_pk ON public.dbz_source_connector USING btree (id);

CREATE UNIQUE INDEX ux_dbz_dead_letter_key ON public.dbz_dead_letter USING btree (connector_type, connector_id, topic, record_key);

CREATE UNIQUE INDEX ux_dbz_dead_letter_retry_key ON public.dbz_dead_letter_retry USING btree (connector_type, connector_id, topic, record_key);

ALTER TABLE ONLY public.dbz_kafka_connect
    ADD CONSTRAINT fk_kafka_broker_id__from__dbz_kafka_broker FOREIGN KEY (kafka_broker_id) REFERENCES public.dbz_kafka_broker(id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT fk_kafka_connect_id__from__dbz_kafka_connect FOREIGN KEY (kafka_connect_id) REFERENCES public.dbz_kafka_connect(id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE ONLY public.dbz_source_connector
    ADD CONSTRAINT fk_kafka_connect_id__from__dbz_kafka_connect FOREIGN KEY (kafka_connect_id) REFERENCES public.dbz_kafka_connect(id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT fk_source_connector_id__from__dbz_source_connector FOREIGN KEY (source_connector_id) REFERENCES public.dbz_source_connector(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
