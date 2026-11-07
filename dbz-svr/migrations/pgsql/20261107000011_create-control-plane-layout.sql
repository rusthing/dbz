-- 控制面画布节点位置
--
-- 画布上的节点来自四张业务表，位置单独存一张表：多人打开同一套连接器时
-- 看到一致的布局，拖动节点只写自己那一行，不会覆盖别人整理的其他节点。

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

ALTER TABLE ONLY public.dbz_control_plane_layout
    ADD CONSTRAINT pk_dbz_control_plane_layout PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_control_plane_layout
    ADD CONSTRAINT ak_node_dbz_control_plane_layout UNIQUE (node_type, node_id);
