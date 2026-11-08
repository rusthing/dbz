-- 连接器检测不一致标记
--
-- Connect 卡片「检测」时只比对 connectorConfig 内容与远端是否存在，不看运行状态，
-- 结果写入这里，供卡片持续展示「不一致」标记：
--   true  库内配置与 Connect 实际配置不一致，或远端已不存在（同时置为禁用）
--   false 最近一次检测一致（含远端有、库内没有而自动补录的记录）

ALTER TABLE public.dbz_source_connector
    ADD COLUMN IF NOT EXISTS inconsistent boolean NOT NULL DEFAULT false;

ALTER TABLE public.dbz_sink_connector
    ADD COLUMN IF NOT EXISTS inconsistent boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.dbz_source_connector.inconsistent IS '检测不一致标记：库内配置与 Connect 实际配置不一致，或远端已不存在';
COMMENT ON COLUMN public.dbz_sink_connector.inconsistent IS '检测不一致标记：库内配置与 Connect 实际配置不一致，或远端已不存在';
