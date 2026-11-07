-- 控制面画布连线折点
--
-- 连线由归属关系自动生成，只允许拖动正交走线的中间折点来避开卡片重叠；
-- 端点仍锁定在两端节点上，连线不可删除。每个连线一条记录。

CREATE TABLE public.dbz_control_plane_edge_layout (
    id bigint NOT NULL,
    edge_id character varying(255) NOT NULL,
    mid_y double precision NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL
);

COMMENT ON TABLE public.dbz_control_plane_edge_layout IS '控制面画布连线折点';

COMMENT ON COLUMN public.dbz_control_plane_edge_layout.edge_id IS '连线 ID（由两端节点 key 拼成，如 own:broker:1:connect:2）';

COMMENT ON COLUMN public.dbz_control_plane_edge_layout.mid_y IS '正交走线中间水平段的世界坐标 Y；拖动折点只改这一个值';

ALTER TABLE ONLY public.dbz_control_plane_edge_layout
    ADD CONSTRAINT pk_dbz_control_plane_edge_layout PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_control_plane_edge_layout
    ADD CONSTRAINT ak_edge_dbz_control_plane_edge_layout UNIQUE (edge_id);
