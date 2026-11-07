-- 死信「修改报文后重投」
--
-- 原始报文 `payload` 永不覆盖：修改后的待投报文另存 `pending_payload`，并记录编辑人
-- 与编辑时间；最近一次人工重投的结果也回写到本行，避免页面上反复盲改。
--
-- 迁移按现有规范只新增本文件，不改历史基线。

ALTER TABLE public.dbz_dead_letter
    ADD COLUMN failure_subtype smallint DEFAULT 0 NOT NULL,
    ADD COLUMN pending_payload jsonb,
    ADD COLUMN payload_edited_by bigint,
    ADD COLUMN payload_edited_ms bigint,
    ADD COLUMN last_retry_success boolean,
    ADD COLUMN last_retry_ms bigint,
    ADD COLUMN last_retry_message text;

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
