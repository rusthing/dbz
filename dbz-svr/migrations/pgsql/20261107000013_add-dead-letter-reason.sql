-- 死信失败信息拆分：failure_detail 保留原始技术内容，reason 只放给人看的一句说明。
--
-- 失败详情（failure_detail）由脚本上报后原样落库，不再截断；失败原因（reason）统一
-- 由后端按 failure_category / failure_subtype 生成，上游脚本无法改写措辞。
--
-- 历史数据回填：
-- 1. 若历史环境里 reason 曾承载原始技术内容、而 failure_detail 为空，先搬到 failure_detail；
-- 2. 再按分类/子类重算 reason。
--
-- 两个 UPDATE 都是幂等的（重复执行结果一致），ADD COLUMN 使用 IF NOT EXISTS，
-- 可安全重复执行。本迁移只新增文件，不改历史基线。

ALTER TABLE public.dbz_dead_letter
    ADD COLUMN IF NOT EXISTS reason text NOT NULL DEFAULT '';

ALTER TABLE public.dbz_dead_letter_retry
    ADD COLUMN IF NOT EXISTS reason text NOT NULL DEFAULT '';

COMMENT ON COLUMN public.dbz_dead_letter.reason IS '给人看的一句失败原因，由后端按分类/子类统一生成；原始技术内容在 failure_detail';

COMMENT ON COLUMN public.dbz_dead_letter.failure_detail IS '原始失败详情（HTTP 状态与响应体、校验报错原文、RTW 错误码、异常栈摘要等），原样保存不截断';

COMMENT ON COLUMN public.dbz_dead_letter_retry.reason IS '给人看的一句失败原因，与死信表同一口径，由后端按分类统一生成';

COMMENT ON COLUMN public.dbz_dead_letter_retry.failure_detail IS '最近一次原始失败详情，原样保存不加工';

-- 历史回填 1：先把可能承载原始内容的 reason 搬到 failure_detail，避免回填时丢失
UPDATE public.dbz_dead_letter
SET failure_detail = reason
WHERE btrim(failure_detail) = '' AND btrim(reason) <> '';

UPDATE public.dbz_dead_letter_retry
SET failure_detail = reason
WHERE btrim(failure_detail) = '' AND btrim(reason) <> '';

-- 历史回填 2：按分类/子类生成清晰说明；重复执行结果一致
UPDATE public.dbz_dead_letter
SET reason = CASE
    WHEN failure_category = 1 AND failure_subtype = 1 THEN '字段校验不通过，请核对并修改报文后重投'
    WHEN failure_category = 1 THEN '数据与下游约束冲突（唯一键冲突或外键缺失），请核对数据'
    WHEN failure_category = 2 THEN '下游连接失败，重试多次后仍失败'
    WHEN failure_category = 3 THEN '下游响应异常，无法确认处理结果，请核对后重投'
    ELSE '处理失败，请查看失败详情'
END;

UPDATE public.dbz_dead_letter_retry
SET reason = CASE
    WHEN failure_category = 2 THEN '下游连接失败，已进入重试队列'
    WHEN failure_category = 3 THEN '下游响应异常，已进入重试队列'
    ELSE '处理失败，请查看失败详情'
END;
