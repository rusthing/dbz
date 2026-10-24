/**
 * # 启用开关
 *
 * 列表里的「启用」列直接用开关切换，编辑弹窗不再管这一列。
 *
 * 提交时只带 `{ id, enabled }`：服务端 ModifyDto 的字段全是 `Option`，没传的字段
 * 不会被写回，所以这里不必把整条记录带上。
 *
 * 禁用不是「只改个标记」：服务端的修改钩子会先把连接器从集群上注销再写库，
 * 注销失败整个请求就失败。因此关掉开关要二次确认，把影响说清楚。
 *
 * 开关先按点击后的状态翻过去（列表刷新回来有肉眼可见的延迟），失败了再退回原状。
 */

import { App as AntdApp, Switch } from 'antd';
import { useEffect, useState } from 'react';
import { errorMessage } from '../utils/format';

interface EnabledSwitchProps {
  /** 当前启用状态，来自列表数据 */
  enabled: boolean;
  /** 记录名称，用于确认框标题 */
  name: string;
  /** 禁用前的说明：这次改动会动到集群上的什么 */
  confirmContent: string;
  /** 提交切换；成功后由调用方负责刷新列表 */
  onToggle: (enabled: boolean) => Promise<void>;
}

export default function EnabledSwitch({
  enabled,
  name,
  confirmContent,
  onToggle,
}: EnabledSwitchProps) {
  const { message, modal } = AntdApp.useApp();
  const [checked, setChecked] = useState(enabled);
  const [submitting, setSubmitting] = useState(false);

  // 列表刷新回来后以服务端数据为准（也覆盖别人改过的情况）
  useEffect(() => setChecked(enabled), [enabled]);

  const submit = async (next: boolean) => {
    setChecked(next);
    setSubmitting(true);
    try {
      await onToggle(next);
      message.success(next ? '已启用' : '已禁用');
    } catch (reason) {
      setChecked(!next);
      message.error(errorMessage(reason));
    } finally {
      setSubmitting(false);
    }
  };

  const handleChange = (next: boolean) => {
    // 重新启用没有破坏性：下一轮检查会把连接器重新部署回去
    if (next) {
      void submit(true);
      return;
    }
    modal.confirm({
      title: `禁用「${name}」？`,
      content: confirmContent,
      okText: '禁用',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: () => submit(false),
    });
  };

  return (
    <Switch
      size="small"
      checked={checked}
      loading={submitting}
      checkedChildren="启用"
      unCheckedChildren="禁用"
      onChange={handleChange}
    />
  );
}
