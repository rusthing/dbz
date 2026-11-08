-- 连接器不一致对比快照
--
-- Connect 卡片「检测」时，除了写入 inconsistent 标记，还把库内配置与远端实际配置
-- 逐项比对的结果写入这里，供「不一致」标签点开查看：
--   remote_exists = true   逐项 rows（键名排序），敏感项值已脱敏
--   remote_exists = false  远端已不存在，rows 为空，reason 说明原因
-- 一致时清空（NULL）。敏感字段（password / secret / token / sasl.jaas.config 等）
-- 一律不落明文。

ALTER TABLE public.dbz_source_connector
    ADD COLUMN IF NOT EXISTS inconsistent_detail text;

ALTER TABLE public.dbz_sink_connector
    ADD COLUMN IF NOT EXISTS inconsistent_detail text;

COMMENT ON COLUMN public.dbz_source_connector.inconsistent_detail IS '检测不一致时的对比快照（脱敏 JSON）：库内/远端逐项配置、差异标记与检测时间；一致时为 NULL';
COMMENT ON COLUMN public.dbz_sink_connector.inconsistent_detail IS '检测不一致时的对比快照（脱敏 JSON）：库内/远端逐项配置、差异标记与检测时间；一致时为 NULL';
