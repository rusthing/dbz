-- dbz 数据库迁移基线：init-index（普通索引）
--
-- 约束型索引随建表语句定义；此处只放独立 CREATE INDEX（含部分/表达式索引）。

CREATE INDEX ix_dbz_dead_letter_connector_status_seen ON public.dbz_dead_letter USING btree (connector_id, status, last_seen_ms);

CREATE INDEX ix_dbz_dead_letter_retry_next_at ON public.dbz_dead_letter_retry USING btree (next_at_ms);

CREATE INDEX relationship_1_fk ON public.dbz_sink_connector USING btree (source_connector_id);

CREATE INDEX relationship_2_fk ON public.dbz_source_connector USING btree (kafka_connect_id);

CREATE INDEX relationship_3_fk ON public.dbz_sink_connector USING btree (kafka_connect_id);

CREATE INDEX relationship_4_fk ON public.dbz_kafka_connect USING btree (kafka_broker_id);
