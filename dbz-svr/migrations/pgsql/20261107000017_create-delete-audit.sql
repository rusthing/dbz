-- 控制面删除审计
--
-- 业务记录删除后原行不可再查，这里保留不可变的删除审计：谁、何时、
-- 删了哪类节点、节点叫什么。删除口令由 adm-rs 校验当前登录用户，不落表。

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

ALTER TABLE ONLY public.dbz_delete_audit
    ADD CONSTRAINT pk_dbz_delete_audit PRIMARY KEY (id);
