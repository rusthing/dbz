-- dbz 数据库迁移基线：create-index（PD 表达不了的补充对象）
--
-- 本文件与 create-table.sql 共同构成完整 DDL：
--   * create-table.sql 是 model/create-table-pgsql.sql（PowerDesigner 导出）的副本；
--   * 这里只放 PowerDesigner 无法生成的 CHECK 约束、部分/表达式索引、性能索引。
-- 约束型索引（PK / AK / 外键索引）一律由 create-table.sql 里的 PD 语句定义，
-- 不在此重复创建。
--
-- 与 create-table.sql 一样，本文件两侧共用，执行结果必须零差异。

-- 部分 / 表达式 / 性能索引（PowerDesigner 不能自动生成）

CREATE INDEX ix_dbz_dead_letter_connector_status_seen ON dbz_dead_letter USING btree (connector_id, status, last_seen_ms);

CREATE INDEX ix_dbz_dead_letter_retry_next_at ON dbz_dead_letter_retry USING btree (next_at_ms);
