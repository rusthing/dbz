/**
 * # 连接器配置编辑器（图形化 ⇄ JSON）
 *
 * 上游连接器的 `connectorConfig` 就是 Debezium 原样的 JSON：检查任务发现连接器不存在
 * 时直接拿这段文本去部署。手写 JSON 最常犯的错是少括号、值写成裸词、`signal.*` 漏配，
 * 而这些都要等到部署失败才暴露。因此这里给两种写法，**同一份数据**：
 *
 * - **图形化**：按 Debezium 的配置分组列出常用项，选择/填写即可；
 * - **JSON**：原文编辑，适合图形化没覆盖到的参数（`column.exclude.list`、
 *   `decimal.handling.mode` 之类）。
 *
 * ## 为什么两个模式能来回切
 *
 * JSON 文本是唯一的数据源（`value` / `onChange`），图形化模式只是把它解析出来渲染成
 * 表单，改动后立刻反序列化写回。所以：
 *
 * - 图形化没列出的键**不会丢**——反序列化时整份对象都在，只是没渲染；
 * - 切到 JSON 时看到的就是表单改完的结果，不会出现两份配置各说各话。
 *
 * 切不过去只有一种情况：JSON 本身不合法（或者不是对象），这时留在 JSON 模式让它改完。
 */

import { Alert, AutoComplete, Button, Empty, Input, Segmented, Select, Space, Typography } from 'antd';
import { useMemo, useState } from 'react';
import JsonEditor from './JsonEditor';

/** 表单项类型 */
type FieldKind = 'text' | 'password' | 'number' | 'select';

/** 一个 Debezium 配置项 */
interface FieldSpec {
  /** JSON 里的键名 */
  key: string;
  label: string;
  kind: FieldKind;
  /** 说明文字，写在输入框下面 */
  help?: string;
  placeholder?: string;
  /** `select` 的候选值；`AutoComplete` 也用它，但允许填别的 */
  options?: string[];
  /** 允许在候选项之外自己填（`connector.class` 这类） */
  freeform?: boolean;
  /** 图形化里标红，不做强制校验（语义正确性由集群反馈） */
  required?: boolean;
}

/** 一组配置项 */
interface FieldGroup {
  title: string;
  desc?: string;
  fields: FieldSpec[];
}

/**
 * # 常用配置分组
 *
 * 只收「配错了才知道错」的项：地址、捕获范围、信号、快照与序列化。其余参数留在
 * JSON 模式，避免这一屏长得像 Debezium 的完整参数表。
 */
const FIELD_GROUPS: FieldGroup[] = [
  {
    title: '基本信息',
    desc: '连接器类与 topic 前缀：前缀决定了 Kafka 上的 topic 名，也决定 Kafka 信号的消息 key。',
    fields: [
      {
        key: 'connector.class',
        label: '连接器类',
        kind: 'select',
        freeform: true,
        required: true,
        options: [
          'io.debezium.connector.mysql.MySqlConnector',
          'io.debezium.connector.mariadb.MariaDbConnector',
          'io.debezium.connector.postgresql.PostgresConnector',
        ],
      },
      {
        key: 'topic.prefix',
        label: 'topic 前缀（topic.prefix）',
        kind: 'text',
        required: true,
        placeholder: '例如 orders',
        help: 'Kafka 信号通道要求消息 key 等于它，写错不报错、只会被连接器忽略。',
      },
    ],
  },
  {
    title: '源库连接',
    desc: 'Debezium 用这个账号读变更；dbz 的「再同步」走信号表时也用同一套账号。',
    fields: [
      { key: 'database.hostname', label: '主机', kind: 'text', required: true, placeholder: '10.0.0.1' },
      { key: 'database.port', label: '端口', kind: 'number', placeholder: '3306 / 5432' },
      { key: 'database.user', label: '用户名', kind: 'text', required: true },
      { key: 'database.password', label: '密码', kind: 'password' },
      {
        key: 'database.dbname',
        label: '数据库',
        kind: 'text',
        required: true,
        help: 'MySQL 填要捕获的库；PostgreSQL 填连接用的库（捕获范围看 schema.include.list）。',
      },
    ],
  },
  {
    title: '捕获范围',
    desc: '都是逗号分隔的正则，写窄了会漏表、写宽了会多读；dbz 的再同步预检也按这里算范围。',
    fields: [
      {
        key: 'database.include.list',
        label: '捕获的库（MySQL）',
        kind: 'text',
        placeholder: 'orders,settle',
        help: '留空表示只捕获 database.dbname 指定的那一个库。',
      },
      {
        key: 'schema.include.list',
        label: '捕获的模式（PostgreSQL）',
        kind: 'text',
        placeholder: 'public',
        help: '留空表示 public。',
      },
      {
        key: 'table.include.list',
        label: '包含的表',
        kind: 'text',
        placeholder: 'orders\\.*, settle\\.bill',
      },
      { key: 'table.exclude.list', label: '排除的表', kind: 'text', placeholder: '.*\\.tmp_.*' },
    ],
  },
  {
    title: '信号（再同步 / 重置位点都靠它）',
    desc: 'source 通道要信号表，kafka 通道要 topic 与 broker；两条都开表示两种方式都能用。',
    fields: [
      {
        key: 'signal.enabled.channels',
        label: '启用的信号通道',
        kind: 'select',
        options: ['source', 'source,kafka', 'kafka'],
        help: '不填等价于 source。选 kafka 时必须同时配下面的 topic 与 broker 地址。',
      },
      {
        key: 'signal.data.collection',
        label: '信号表（source 通道）',
        kind: 'text',
        placeholder: 'orders.debezium_signal',
        help: '源库里的表，需先建好：id VARCHAR(42) 主键 + type + data 三列。',
      },
      {
        key: 'signal.kafka.topic',
        label: '信号 topic（kafka 通道）',
        kind: 'text',
        placeholder: 'dbz-signal',
      },
      {
        key: 'signal.kafka.bootstrap.servers',
        label: '信号 broker 地址（kafka 通道）',
        kind: 'text',
        placeholder: '10.0.0.1:9092',
        help: '留空时连接器沿用 worker 的 bootstrap.servers；填了要和 dbz 里的 Kafka 集群一致，否则信号投过去没人读。',
      },
    ],
  },
  {
    title: '快照与序列化',
    desc: 'snapshot.mode 决定首次启动读什么；converter 决定写进 Kafka 的消息格式。',
    fields: [
      {
        key: 'snapshot.mode',
        label: '快照模式',
        kind: 'select',
        options: [
          'initial',
          'initial_only',
          'schema_only',
          'schema_only_recovery',
          'when_needed',
          'no_data',
          'configuration_based',
        ],
        help: 'initial：没有位点时整表快照（重置位点后重做全量的前提）；schema_only：只读表结构。',
      },
      {
        key: 'key.converter',
        label: 'key 转换器',
        kind: 'select',
        freeform: true,
        options: [
          'org.apache.kafka.connect.json.JsonConverter',
          'org.apache.kafka.connect.storage.StringConverter',
          'io.confluent.connect.avro.AvroConverter',
        ],
      },
      {
        key: 'value.converter',
        label: 'value 转换器',
        kind: 'select',
        freeform: true,
        options: [
          'org.apache.kafka.connect.json.JsonConverter',
          'org.apache.kafka.connect.storage.StringConverter',
          'io.confluent.connect.avro.AvroConverter',
        ],
      },
      {
        key: 'key.converter.schemas.enable',
        label: 'key 携带 schema',
        kind: 'select',
        options: ['true', 'false'],
        help: 'JsonConverter 下建议 false：下游按普通 JSON 解析更省事。',
      },
      {
        key: 'value.converter.schemas.enable',
        label: 'value 携带 schema',
        kind: 'select',
        options: ['true', 'false'],
      },
    ],
  },
];

/** 数值型键：写回 JSON 时要变成数字，否则 Debezium 起不来 */
const NUMBER_KEYS = new Set(['database.port']);

/** 模板：只补空着的键，不动已经填好的值 */
const TEMPLATES: { label: string; values: Record<string, string> }[] = [
  {
    label: 'MySQL 模板',
    values: {
      'connector.class': 'io.debezium.connector.mysql.MySqlConnector',
      'database.port': '3306',
      'signal.enabled.channels': 'source',
      'snapshot.mode': 'initial',
      'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
      'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
      'key.converter.schemas.enable': 'false',
      'value.converter.schemas.enable': 'false',
    },
  },
  {
    label: 'PostgreSQL 模板',
    values: {
      'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
      'database.port': '5432',
      'schema.include.list': 'public',
      'signal.enabled.channels': 'source',
      'snapshot.mode': 'initial',
      'plugin.name': 'pgoutput',
      'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
      'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
      'key.converter.schemas.enable': 'false',
      'value.converter.schemas.enable': 'false',
    },
  },
];

interface ConnectorConfigEditorProps {
  value?: string;
  onChange?: (value: string) => void;
}

/** 把 JSON 文本解析成对象；不是对象或解析失败时返回 `null` */
function parseObject(text: string): Record<string, unknown> | null {
  if (!text.trim()) {
    return {};
  }
  try {
    const parsed: unknown = JSON.parse(text);
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return null;
    }
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** 把 JSON 里的值转成输入框能显示的字符串 */
function toText(value: unknown): string {
  if (value === undefined || value === null) {
    return '';
  }
  if (typeof value === 'string') {
    return value;
  }
  return String(value);
}

export default function ConnectorConfigEditor({ value = '', onChange }: ConnectorConfigEditorProps) {
  const [mode, setMode] = useState<'gui' | 'json'>('gui');
  const parsed = useMemo(() => parseObject(value), [value]);
  const canUseGui = parsed !== null;

  /** 写回一个键；空值等于删掉这个键（Debezium 对空串的处理与「没配」并不等价） */
  const setField = (key: string, next: string) => {
    const draft: Record<string, unknown> = { ...(parsed ?? {}) };
    if (next === '') {
      delete draft[key];
    } else if (NUMBER_KEYS.has(key)) {
      const number = Number(next);
      draft[key] = Number.isFinite(number) ? number : next;
    } else {
      draft[key] = next;
    }
    onChange?.(JSON.stringify(draft, null, 2));
  };

  /** 套模板：只补空键，已填的值不覆盖 */
  const applyTemplate = (values: Record<string, string>) => {
    const draft: Record<string, unknown> = { ...(parsed ?? {}) };
    let filled = 0;
    for (const [key, templateValue] of Object.entries(values)) {
      const current = draft[key];
      if (current === undefined || current === null || current === '') {
        draft[key] = NUMBER_KEYS.has(key) ? Number(templateValue) : templateValue;
        filled += 1;
      }
    }
    onChange?.(JSON.stringify(draft, null, 2));
    return filled;
  };

  const missing = useMemo(() => {
    if (!parsed) {
      return [];
    }
    return FIELD_GROUPS.flatMap((group) => group.fields)
      .filter((field) => field.required)
      .filter((field) => toText(parsed[field.key]).trim() === '')
      .map((field) => field.label);
  }, [parsed]);

  const renderField = (field: FieldSpec) => {
    const current = toText(parsed?.[field.key]);
    const common = {
      value: current,
      placeholder: field.placeholder,
      allowClear: true,
    };

    let control;
    if (field.kind === 'select') {
      control = field.freeform ? (
        <AutoComplete
          value={current}
          options={field.options?.map((option) => ({ value: option }))}
          placeholder={field.placeholder}
          filterOption
          onChange={(next) => setField(field.key, next ?? '')}
        />
      ) : (
        <Select
          value={current || undefined}
          allowClear
          placeholder={field.placeholder}
          options={field.options?.map((option) => ({ value: option, label: option }))}
          onChange={(next) => setField(field.key, next ?? '')}
        />
      );
    } else if (field.kind === 'password') {
      control = (
        <Input.Password
          {...common}
          autoComplete="new-password"
          onChange={(event) => setField(field.key, event.target.value)}
        />
      );
    } else if (field.kind === 'number') {
      control = (
        <Input
          {...common}
          inputMode="numeric"
          className="dbz-mono"
          onChange={(event) => setField(field.key, event.target.value.trim())}
        />
      );
    } else {
      control = (
        <Input
          {...common}
          className="dbz-mono"
          onChange={(event) => setField(field.key, event.target.value)}
        />
      );
    }

    return (
      <div key={field.key} style={{ marginBottom: 12 }}>
        <div style={{ marginBottom: 4 }}>
          <Typography.Text>
            {field.label}
            {field.required && <Typography.Text type="danger"> *</Typography.Text>}
          </Typography.Text>
          <Typography.Text type="secondary" className="dbz-mono" style={{ marginLeft: 8, fontSize: 12 }}>
            {field.key}
          </Typography.Text>
        </div>
        {control}
        {field.help && (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {field.help}
          </Typography.Text>
        )}
      </div>
    );
  };

  return (
    <Space direction="vertical" size={10} style={{ width: '100%' }}>
      <Space size={8} wrap>
        <Segmented
          size="small"
          value={mode}
          onChange={(next) => {
            if (next === 'gui' && !canUseGui) {
              return;
            }
            setMode(next as 'gui' | 'json');
          }}
          options={[
            { label: '图形化', value: 'gui', disabled: !canUseGui },
            { label: 'JSON', value: 'json' },
          ]}
        />
        {mode === 'gui' &&
          TEMPLATES.map((template) => (
            <Button
              key={template.label}
              size="small"
              onClick={() => applyTemplate(template.values)}
            >
              套用{template.label}
            </Button>
          ))}
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          {mode === 'gui'
            ? '只列出常用项，其他参数在 JSON 里加；切过去不会丢'
            : '这里是最终提交的内容，图形化改动会实时反映过来'}
        </Typography.Text>
      </Space>

      {!canUseGui && (
        <Alert
          type="warning"
          showIcon
          message="JSON 不合法，图形化不可用"
          description="先修好 JSON（比如补上括号、给值加上引号），再切回图形化。"
        />
      )}

      {mode === 'json' ? (
        <JsonEditor value={value} onChange={onChange} rows={18} />
      ) : (
        <>
          {missing.length > 0 && (
            <Alert
              type="warning"
              showIcon
              message={`还缺必填项：${missing.join('、')}`}
              description="缺这些项时连接器会在部署阶段失败，或捕获范围与预期不符。"
            />
          )}
          {FIELD_GROUPS.map((group) => (
            <div key={group.title} className="dbz-config-group">
              <Typography.Text strong>{group.title}</Typography.Text>
              {group.desc && (
                <div>
                  <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                    {group.desc}
                  </Typography.Text>
                </div>
              )}
              <div style={{ marginTop: 8 }}>
                {group.fields.map((field) => renderField(field))}
              </div>
            </div>
          ))}
          {Object.keys(parsed ?? {}).length === 0 && (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="还没有任何配置：先套用模板，或直接在 JSON 模式里粘贴"
            />
          )}
        </>
      )}
    </Space>
  );
}
