-- 控制面画布连线连接点偏移
--
-- 连接点从「卡片中心固定」改为可左右拖动：存相对卡片中心的横向偏移，
-- 节点移动时连接点跟着卡片走。默认 0 表示沿用原来的居中/同源错开位置。

ALTER TABLE public.dbz_control_plane_edge_layout
    ADD COLUMN start_dx double precision NOT NULL DEFAULT 0,
    ADD COLUMN end_dx double precision NOT NULL DEFAULT 0;

COMMENT ON COLUMN public.dbz_control_plane_edge_layout.start_dx IS '上游连接点相对卡片中心的横向偏移，0 表示默认位置';

COMMENT ON COLUMN public.dbz_control_plane_edge_layout.end_dx IS '下游连接点相对卡片中心的横向偏移，0 表示默认位置';
