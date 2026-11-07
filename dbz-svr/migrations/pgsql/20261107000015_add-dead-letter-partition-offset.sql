-- 死信与重试队列补充原始消息位置：脚本上报时可选带 partition / offset，
-- 列表据此展示「这条记录来自哪个分区、哪个位点」。
-- 两列都允许为空，兼容旧脚本与历史数据。
ALTER TABLE dbz_dead_letter
    ADD COLUMN IF NOT EXISTS "partition" INT,
    ADD COLUMN IF NOT EXISTS "offset" BIGINT;

ALTER TABLE dbz_dead_letter_retry
    ADD COLUMN IF NOT EXISTS "partition" INT,
    ADD COLUMN IF NOT EXISTS "offset" BIGINT;
