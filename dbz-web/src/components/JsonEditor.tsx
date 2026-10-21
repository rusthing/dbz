/**
 * # JSON 编辑器
 *
 * 连接器配置是 Debezium 原样的 JSON 文本，用普通输入框编辑很容易写出非法 JSON，
 * 导致检查任务在 `deploy_connector` 阶段才失败。这里在失焦时先校验、并提供一键格式化，
 * 把错误挡在提交之前。
 *
 * 组件是受控的（`value` / `onChange`），可以直接放进 `Form.Item`。
 */

import { Button, Input, Space, Typography } from 'antd';
import { useMemo, useState } from 'react';

interface JsonEditorProps {
  value?: string;
  onChange?: (value: string) => void;
  /** 输入框行数 */
  rows?: number;
}

/** 校验 JSON 文本，合法时返回 `null` */
function validate(text: string): string | null {
  if (!text.trim()) {
    return '连接器配置不能为空';
  }
  try {
    JSON.parse(text);
    return null;
  } catch (error) {
    return error instanceof Error ? error.message : 'JSON 格式不正确';
  }
}

export default function JsonEditor({ value = '', onChange, rows = 14 }: JsonEditorProps) {
  const [touched, setTouched] = useState(false);
  const error = useMemo(() => (touched ? validate(value) : null), [touched, value]);

  const handleFormat = () => {
    setTouched(true);
    if (validate(value)) {
      return;
    }
    onChange?.(JSON.stringify(JSON.parse(value), null, 2));
  };

  return (
    <Space direction="vertical" size={4} style={{ width: '100%' }}>
      <Space size={8}>
        <Button size="small" onClick={handleFormat}>
          格式化
        </Button>
        <Typography.Text type={error ? 'danger' : 'secondary'} style={{ fontSize: 12 }}>
          {error ?? 'JSON 合法'}
        </Typography.Text>
      </Space>
      <Input.TextArea
        value={value}
        onChange={(event) => onChange?.(event.target.value)}
        onBlur={() => setTouched(true)}
        rows={rows}
        spellCheck={false}
        className="dbz-json-editor"
        placeholder='例如 {"connector.class": "io.debezium.connector.mysql.MySqlConnector", ...}'
      />
    </Space>
  );
}
