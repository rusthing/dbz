/**
 * # 字典
 *
 * 服务端用 `#[dic]` 标注的枚举**序列化为整数判别式**（不是字符串），所以前端只能靠
 * 这张表把数字翻译成文案与颜色。改枚举时必须同步这里，否则页面上会出现「未知状态」。
 *
 * ## 三个状态枚举的分工
 *
 * - {@link kafkaConnectStatusDic} —— 集群自身是否可达（`dbz_kafka_connect.run_status`）；
 * - {@link connectorRunStatusDic} —— 检查任务对连接器得出的**结论**，会回写库；
 * - {@link kafkaConnectorStatusDic} —— 远端连接器**此刻**的实时状态，不落库。
 */

import type {
  ConnectorRunStatus,
  KafkaConnectStatus,
  KafkaConnectorStatus,
} from '../api/types';

/** # 字典项 */
export interface DicItem<V extends number = number> {
  value: V;
  label: string;
  /** antd Tag 的预设色 */
  color: string;
}

/** # 集群状态 */
export const kafkaConnectStatusDic: DicItem<KafkaConnectStatus>[] = [
  { value: 0, label: '无状态', color: 'default' },
  { value: 1, label: '正常', color: 'success' },
  { value: 2, label: '已禁用', color: 'default' },
  { value: 3, label: '连接失败', color: 'error' },
];

/** # 连接器运行状态（上游/下游共用） */
export const connectorRunStatusDic: DicItem<ConnectorRunStatus>[] = [
  { value: 0, label: '无状态', color: 'default' },
  { value: 1, label: '运行中', color: 'success' },
  { value: 2, label: '已禁用', color: 'default' },
  { value: 3, label: '连接集群失败', color: 'error' },
  { value: 4, label: '注册失败', color: 'error' },
  { value: 5, label: '检测失败', color: 'error' },
  { value: 6, label: '检测异常', color: 'warning' },
  { value: 7, label: '状态查询失败', color: 'error' },
  { value: 8, label: '状态异常', color: 'warning' },
];

/** # 远端连接器实时状态 */
export const kafkaConnectorStatusDic: DicItem<KafkaConnectorStatus>[] = [
  { value: 0, label: '运行中', color: 'success' },
  { value: 1, label: '已失败', color: 'error' },
  { value: 2, label: '已暂停', color: 'warning' },
  { value: 3, label: '未分配', color: 'processing' },
  { value: 4, label: '已销毁', color: 'default' },
];

/** # 取字典项 */
export function dicItem<V extends number>(dic: DicItem<V>[], value: V | null | undefined): DicItem<V> | undefined {
  return dic.find((item) => item.value === value);
}

/** # 取字典文案，未命中时给出可读兜底 */
export function dicLabel<V extends number>(dic: DicItem<V>[], value: V | null | undefined): string {
  if (value === null || value === undefined) {
    return '-';
  }
  return dicItem(dic, value)?.label ?? `未知状态(${value})`;
}

/** # 转成 antd Select 的选项 */
export function dicOptions<V extends number>(dic: DicItem<V>[]): { value: V; label: string }[] {
  return dic.map((item) => ({ value: item.value, label: item.label }));
}
