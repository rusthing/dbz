/**
 * # 状态标签
 *
 * 三个字典共用一个组件：传哪个字典由调用方决定，避免为每个枚举各写一个 Tag。
 *
 * 传 `detail` 时鼠标移上去弹出运行详情：状态是怎么得出的、失败原因是什么、什么时候
 * 检查的、在哪台集群上。列表里的状态是**检查任务的结论**（回写库），光看一个标签
 * 回答不了「为什么失败」；而原因其实都在手边（库里的 `run_msg`、远端集群返回的
 * 连接器 trace），悬停展示比让人去翻日志省事得多。
 */

import { Popover, Tag } from 'antd';
import type { ReactNode } from 'react';
import type { DicItem } from '../dic';
import { dicItem } from '../dic';

/** 悬停详情：一句话概括 + 明细行 + 失败原因 */
export interface StatusDetail {
  /** 弹层标题，例如「连接 Kafka Connect 失败」 */
  summary?: string;
  /** 明细行，例如「所属集群 / 状态时间 / 地址」 */
  rows?: { label: string; value: ReactNode }[];
  /** 失败原因（长文本，等宽字体、超长可滚动） */
  reason?: string | null;
}

interface StatusTagProps {
  /** 字典表 */
  dic: DicItem[];
  /** 服务端返回的整数判别式 */
  value?: number | null;
  /** 悬停详情；不传则只是普通标签 */
  detail?: StatusDetail;
}

export default function StatusTag({ dic, value, detail }: StatusTagProps) {
  const tag = renderTag(dic, value);

  if (!detail) {
    return tag;
  }

  const rows = detail.rows ?? [];
  // 既没有明细也没有原因时不必弹一个空框
  if (rows.length === 0 && !detail.reason) {
    return tag;
  }

  return (
    <Popover
      trigger="hover"
      mouseEnterDelay={0.25}
      // 原因可能很长：限制宽度后让它在内部滚动，不把整页撑开
      styles={{ body: { maxWidth: 380, padding: '10px 12px' } }}
      title={detail.summary}
      content={
        <div className="dbz-status-detail">
          {rows.map((row) => (
            <div key={row.label} className="dbz-status-row">
              <span className="dbz-status-row-label">{row.label}</span>
              <span className="dbz-status-row-value">{row.value}</span>
            </div>
          ))}
          {detail.reason && <pre className="dbz-status-reason">{detail.reason}</pre>}
        </div>
      }
    >
      <span className="dbz-status-hover">{tag}</span>
    </Popover>
  );
}

/** 按字典渲染标签；未知取值也要显式说出来，别让单元格变成空白 */
function renderTag(dic: DicItem[], value?: number | null) {
  if (value === null || value === undefined) {
    return <Tag>-</Tag>;
  }
  const item = dicItem(dic, value);
  if (!item) {
    return <Tag color="default">未知({value})</Tag>;
  }
  return <Tag color={item.color}>{item.label}</Tag>;
}
