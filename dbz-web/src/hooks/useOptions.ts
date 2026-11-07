/**
 * # 集群与连接器下拉选项
 *
 * 上游/下游连接器的表单、筛选器都要按「集群」「上游连接器」选择，这里统一取一次全量列表
 * 并在内存里做 id → 名称的映射。数量级是「几十台集群 / 几百条连接器」，不值得为它加缓存层。
 */

import { useMemo } from 'react';
import type { ApiError } from '../api/client';
import { kafkaBrokerApi } from '../api/kafka-broker';
import { kafkaConnectApi } from '../api/kafka-connect';
import { sourceConnectorApi } from '../api/source-connector';
import type { DbzKafkaBrokerVo, DbzKafkaConnectVo, DbzSourceConnectorVo, Id } from '../api/types';
import { useAsync } from './useAsync';

/** # Kafka 集群（broker）选项集合 */
export interface BrokerOptions {
  brokers: DbzKafkaBrokerVo[];
  /** antd Select 选项 */
  options: { value: Id; label: string }[];
  /** id → Kafka 集群 */
  byId: Map<Id, DbzKafkaBrokerVo>;
  loading: boolean;
  /** 加载失败的原因，供列表给出提示而不是显示成「没有集群」 */
  error: ApiError | null;
  reload: () => void;
}

/** # 取 Kafka 集群选项（用于展示 Connect 的归属关系） */
export function useBrokerOptions(): BrokerOptions {
  const { data, loading, error, reload } = useAsync<DbzKafkaBrokerVo[]>(
    () => kafkaBrokerApi.list(),
    [],
    [],
  );

  const options = useMemo(
    () =>
      [...data]
        .sort((left, right) => left.name.localeCompare(right.name))
        .map((broker) => ({ value: broker.id, label: broker.name })),
    [data],
  );

  const byId = useMemo(() => new Map(data.map((broker) => [broker.id, broker])), [data]);

  return { brokers: data, options, byId, loading, error, reload };
}

/** # 集群选项集合 */
export interface ClusterOptions {
  clusters: DbzKafkaConnectVo[];
  /** antd Select 选项 */
  options: { value: Id; label: string }[];
  /** id → 集群 */
  byId: Map<Id, DbzKafkaConnectVo>;
  loading: boolean;
  /** 加载失败的原因，供筛选器给出提示而不是显示成「没有集群」 */
  error: ApiError | null;
  reload: () => void;
}

/** # 取集群选项 */
export function useClusterOptions(): ClusterOptions {
  const { data, loading, error, reload } = useAsync<DbzKafkaConnectVo[]>(
    () => kafkaConnectApi.list(),
    [],
    [],
  );

  const options = useMemo(
    () =>
      [...data]
        .sort((left, right) => left.name.localeCompare(right.name))
        .map((cluster) => ({ value: cluster.id, label: cluster.name })),
    [data],
  );

  const byId = useMemo(() => new Map(data.map((cluster) => [cluster.id, cluster])), [data]);

  return { clusters: data, options, byId, loading, error, reload };
}

/** # 上游连接器选项集合 */
export interface SourceOptions {
  sources: DbzSourceConnectorVo[];
  /** antd Select 选项 */
  options: { value: Id; label: string }[];
  /** id → 上游连接器 */
  byId: Map<Id, DbzSourceConnectorVo>;
  loading: boolean;
  /** 加载失败的原因，供筛选器给出提示而不是显示成「没有上游连接器」 */
  error: ApiError | null;
  reload: () => void;
}

/** # 取上游连接器选项 */
export function useSourceOptions(): SourceOptions {
  const { data, loading, error, reload } = useAsync<DbzSourceConnectorVo[]>(
    () => sourceConnectorApi.list(),
    [],
    [],
  );

  const options = useMemo(
    () =>
      [...data]
        .sort((left, right) => left.name.localeCompare(right.name))
        .map((source) => ({ value: source.id, label: source.name })),
    [data],
  );

  const byId = useMemo(() => new Map(data.map((source) => [source.id, source])), [data]);

  return { sources: data, options, byId, loading, error, reload };
}
