/**
 * # 集群与连接器下拉选项
 *
 * 上游/下游连接器的表单、筛选器都要按「集群」「上游连接器」选择，这里统一取一次全量列表
 * 并在内存里做 id → 名称的映射。数量级是「几十台集群 / 几百条连接器」，不值得为它加缓存层。
 */

import { useMemo } from 'react';
import { kafkaConnectApi } from '../api/kafka-connect';
import { sourceConnectorApi } from '../api/source-connector';
import type { DbzKafkaConnectVo, DbzSourceConnectorVo } from '../api/types';
import { useAsync } from './useAsync';

/** # 集群选项集合 */
export interface ClusterOptions {
  clusters: DbzKafkaConnectVo[];
  /** antd Select 选项 */
  options: { value: number; label: string }[];
  /** id → 集群 */
  byId: Map<number, DbzKafkaConnectVo>;
  loading: boolean;
  reload: () => void;
}

/** # 取集群选项 */
export function useClusterOptions(): ClusterOptions {
  const { data, loading, reload } = useAsync<DbzKafkaConnectVo[]>(
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

  return { clusters: data, options, byId, loading, reload };
}

/** # 上游连接器选项集合 */
export interface SourceOptions {
  sources: DbzSourceConnectorVo[];
  /** antd Select 选项 */
  options: { value: number; label: string }[];
  /** id → 上游连接器 */
  byId: Map<number, DbzSourceConnectorVo>;
  loading: boolean;
  reload: () => void;
}

/** # 取上游连接器选项 */
export function useSourceOptions(): SourceOptions {
  const { data, loading, reload } = useAsync<DbzSourceConnectorVo[]>(
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

  return { sources: data, options, byId, loading, reload };
}
