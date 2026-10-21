/**
 * # 启用标记
 */

import { Tag } from 'antd';

interface EnabledTagProps {
  enabled: boolean;
}

export default function EnabledTag({ enabled }: EnabledTagProps) {
  return enabled ? <Tag color="blue">启用</Tag> : <Tag>禁用</Tag>;
}
