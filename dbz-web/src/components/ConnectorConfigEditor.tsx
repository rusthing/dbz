/**
 * # 连接器配置编辑器（图形化 ⇄ JSON）
 *
 * 连接器的 `connectorConfig` 就是 Kafka Connect 原样的 JSON：检查任务发现连接器不存在
 * 时直接拿这段文本去部署。手写 JSON 最常犯的错是少括号、值写成裸词、`signal.*` 漏配，
 * 而这些都要等到部署失败才暴露。因此这里给两种写法，**同一份数据**：
 *
 * - **图形化**：按 Kafka Connect 的参数分组列出常用项，选择/填写即可；
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
 *
 * ## 上游与下游各有一套分组
 *
 * 同一个连接器名字段，source（Debezium）看的是源库地址与捕获范围，sink（Kafka Connect）
 * 看的是目标库地址与写入方式，键名完全不同。所以分组、模板、数值键都按 `kind` 分表，
 * 靠 `PROFILES` 选一份，而不是写一堆 `if (kind === 'sink')`。
 *
 * ## 模板要能「换库」，不是「补空」
 *
 * 早先的实现是「只补空键」，于是从 MySQL 模板点 PostgreSQL 模板时一个键都不改，
 * `connector.class` 还停在 MySqlConnector——用户以为换成功了，部署到集群才发现还是 MySQL。
 * 现在按意图区分：
 *
 * - `overwrite`：声明用哪家库的键，直接改写；
 * - `stale`：另一家的专属键，直接删掉（留着是无效参数，只会告警，容易埋雷）；
 * - `port`：端口还是模板自己写的默认值（或为空）时才改，自定义端口不能被模板改回去；
 * - `fill`：其余默认项只补空键，用户调过的值不动。
 */

import { Alert, AutoComplete, Button, Input, Segmented, Select, Space, Typography } from 'antd';
import { useMemo, useState } from 'react';
import JsonEditor from './JsonEditor';

/** 编辑器适用的方向：上游（Debezium source）与下游（Kafka Connect sink）参数不同 */
export type ConnectorKind = 'source' | 'sink';

/** 表单项类型 */
type FieldKind = 'text' | 'password' | 'number' | 'select';

/** 数据库方言：只用来决定「这家库的专属项要不要显示」 */
type Dialect = 'mysql' | 'postgres';

/** 一个 Kafka Connect 配置项 */
interface FieldSpec {
  /** JSON 里的键名，同时作为输入框的 id（点标题能聚焦） */
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
  /** 只在这家库里显示；不写表示两家都显示 */
  dialects?: Dialect[];
}

/** 一组配置项 */
interface FieldGroup {
  title: string;
  desc?: string;
  fields: FieldSpec[];
}

/**
 * # 套用模板
 *
 * `overwrite` / `stale` / `port` 是「换库」要动的部分，`fill` 是「起手式」默认值。
 * 分开是必须的：换库要能覆盖，默认值不能覆盖用户已经调好的参数。
 */
interface Template {
  /** 按钮上显示成「套用 XXX」 */
  label: string;
  /** 套用后的提示，讲清这次改了什么 */
  desc: string;
  overwrite: Record<string, string>;
  stale: string[];
  /** 端口：当前为空、或还是两家模板的默认端口时，才改成这家 */
  port?: { key: string; value: string; generated: string[] };
  fill: Record<string, string>;
}

/** 一个方向的完整配置：分组、模板、数值键、方言推断、额外提醒 */
interface Profile {
  groups: FieldGroup[];
  templates: Template[];
  /** 写回 JSON 时要变成数字的键，否则连接器起不来 */
  numberKeys: Set<string>;
  /** 从当前配置认出方言，用来隐藏另一家的专属项；认不出（自定义类）时全部展示 */
  dialect?: (config: Record<string, unknown>) => Dialect | null;
  /** 必填项之外的提醒：取值组合不对时给一句人话 */
  warnings?: (config: Record<string, unknown>) => string[];
}

/** 序列化那 4 项在上下游都一样，抽出来复用 */
const CONVERTER_FILL: Record<string, string> = {
  'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
  'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
  'key.converter.schemas.enable': 'false',
  'value.converter.schemas.enable': 'false',
};

const CONVERTER_OPTIONS = [
  'org.apache.kafka.connect.json.JsonConverter',
  'org.apache.kafka.connect.storage.StringConverter',
  'io.confluent.connect.avro.AvroConverter',
];

/** 序列化分组：上下游通用 */
const converterGroup: FieldGroup = {
  title: '序列化',
  desc: 'converter 决定写进 / 读出 Kafka 的消息格式，两端要一致，否则下游解析不了。',
  fields: [
    { key: 'key.converter', label: 'key 转换器', kind: 'select', freeform: true, options: CONVERTER_OPTIONS },
    { key: 'value.converter', label: 'value 转换器', kind: 'select', freeform: true, options: CONVERTER_OPTIONS },
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
};

/* ---------------------------------------------------------------- 上游 */

/**
 * # 上游（source）配置分组
 *
 * 只收「配错了才知道错」的项：地址、捕获范围、信号、快照与序列化。其余参数留在
 * JSON 模式，避免这一屏长得像 Debezium 的完整参数表。
 */
const SOURCE_GROUPS: FieldGroup[] = [
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
        help: '改这里等于换数据库方言，下面的「捕获范围」会跟着换字段。',
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
      {
        key: 'database.port',
        label: '端口',
        kind: 'number',
        placeholder: '3306 / 5432',
        help: '套用模板时跟着换库改（3306 / 5432）；自己填过的端口不会被改。',
      },
      { key: 'database.user', label: '用户名', kind: 'text', required: true },
      { key: 'database.password', label: '密码', kind: 'password' },
      {
        key: 'database.dbname',
        label: '数据库',
        kind: 'text',
        required: true,
        help: 'MySQL 填要捕获的库；PostgreSQL 填连接用的库（捕获范围看 schema.include.list）。',
      },
      {
        key: 'plugin.name',
        label: '复制插件（plugin.name）',
        kind: 'select',
        freeform: true,
        options: ['pgoutput', 'wal2json', 'decoderbufs'],
        help: 'PostgreSQL 专用：pgoutput 是逻辑复制自带的，不用额外装；改插件要重建 publication。',
        dialects: ['postgres'],
      },
    ],
  },
  {
    title: '捕获范围',
    desc: '都是逗号分隔的正则（写成 JSON 数组也认），写窄了会漏表、写宽了会多读；dbz 的再同步预检也按这里算范围。',
    fields: [
      {
        key: 'database.include.list',
        label: '捕获的库（database.include.list）',
        kind: 'text',
        placeholder: 'orders,settle',
        help: '留空表示只捕获 database.dbname 指定的那一个库。',
        dialects: ['mysql'],
      },
      {
        key: 'schema.include.list',
        label: '捕获的模式（schema.include.list）',
        kind: 'text',
        placeholder: 'public',
        help: 'PostgreSQL 用模式而不是库；留空表示 public。',
        dialects: ['postgres'],
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
        help: '不填等价于 source。选 kafka 时要配下面的 topic；broker 地址留空会自动填成这颗连接器所在 Connect 关联集群的地址。',
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
        help: '留空会在保存时自动填成关联 Kafka 集群的地址（Debezium 这个键没有默认值，也不会沿用 worker，留空老版本会直接起不来）。填了就要和集群地址一致，否则信号投过去没人读。',
      },
    ],
  },
  {
    title: '快照',
    desc: 'snapshot.mode 决定首次启动读什么；重置位点后重做全量就靠 initial。',
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
        help: 'initial：没有位点时整表快照；schema_only：只读表结构。',
      },
    ],
  },
  converterGroup,
];

const SOURCE_TEMPLATES: Template[] = [
  {
    label: 'MySQL 模板',
    desc: 'MySQL / MariaDB：端口 3306，捕获库看 database.include.list。',
    overwrite: { 'connector.class': 'io.debezium.connector.mysql.MySqlConnector' },
    stale: ['schema.include.list', 'plugin.name'],
    port: { key: 'database.port', value: '3306', generated: ['3306', '5432'] },
    fill: {
      'signal.enabled.channels': 'source',
      'snapshot.mode': 'initial',
      ...CONVERTER_FILL,
    },
  },
  {
    label: 'PostgreSQL 模板',
    desc: 'PostgreSQL：端口 5432，捕获模式看 schema.include.list，复制插件 pgoutput。',
    overwrite: { 'connector.class': 'io.debezium.connector.postgresql.PostgresConnector' },
    stale: ['database.include.list'],
    port: { key: 'database.port', value: '5432', generated: ['3306', '5432'] },
    fill: {
      // plugin.name 放 fill 而不是 overwrite：用户自己换成 wal2json 的不能被模板改回去
      'plugin.name': 'pgoutput',
      'schema.include.list': 'public',
      'signal.enabled.channels': 'source',
      'snapshot.mode': 'initial',
      ...CONVERTER_FILL,
    },
  },
];

/** 从 `connector.class` 认方言；MariaDB 走 MySQL 那一套参数 */
function sourceDialect(config: Record<string, unknown>): Dialect | null {
  const className = toText(config['connector.class']).toLowerCase();
  if (className.includes('mysql') || className.includes('mariadb')) {
    return 'mysql';
  }
  if (className.includes('postgres')) {
    return 'postgres';
  }
  return null;
}

/* ---------------------------------------------------------------- 下游 */

/**
 * # 下游（sink）配置分组
 *
 * sink 不捕获变更，关心的是「从哪个 topic 拿、往哪个库写、按什么语义写」。所以这里
 * 没有一个 `database.*` 键，地址全在 `connection.*` 上。
 */
const SINK_GROUPS: FieldGroup[] = [
  {
    title: '基本信息',
    desc: '连接器类与要消费的 topic：topic 名对不上就收不到数据，也不报错。',
    fields: [
      {
        key: 'connector.class',
        label: '连接器类',
        kind: 'select',
        freeform: true,
        required: true,
        options: [
          'io.debezium.connector.jdbc.JdbcSinkConnector',
          'io.confluent.connect.jdbc.JdbcSinkConnector',
        ],
      },
      {
        key: 'topics',
        label: '消费的 topic',
        kind: 'text',
        required: true,
        placeholder: 'orders.orders.t_order',
        help: '与上游「topic 前缀 + 库 + 表」拼出来的名字一致，多个用逗号分隔；可去远端连接器的 topic 列表核对。',
      },
      {
        key: 'tasks.max',
        label: '任务数',
        kind: 'number',
        placeholder: '1',
        help: '一般 1：多任务并行写同一张表容易互相锁住，也可能乱序。',
      },
    ],
  },
  {
    title: '目标库连接',
    desc: '写入用的 JDBC 账号，需要有目标表（或建表）的权限。',
    fields: [
      {
        key: 'connection.url',
        label: 'JDBC 地址',
        kind: 'text',
        required: true,
        placeholder: 'jdbc:postgresql://10.0.0.3:5432/dw',
        help: 'PostgreSQL 形如 jdbc:postgresql://host:5432/db；MySQL 形如 jdbc:mysql://host:3306/db。',
      },
      { key: 'connection.username', label: '用户名', kind: 'text', required: true },
      { key: 'connection.password', label: '密码', kind: 'password' },
    ],
  },
  {
    title: '写入方式',
    desc: 'insert 只追加、不依赖主键；upsert / update 必须给出主键，否则连接器起不来。',
    fields: [
      {
        key: 'insert.mode',
        label: '写入模式',
        kind: 'select',
        options: ['insert', 'upsert', 'update'],
      },
      {
        key: 'primary.key.mode',
        label: '主键来源',
        kind: 'select',
        options: ['record_key', 'record_value', 'none'],
        help: 'record_key 取 Kafka 消息 key（上游开了 schemas.enable 才可能是结构体）；record_value 从下面填的字段取。',
      },
      {
        key: 'primary.key.fields',
        label: '主键字段（primary.key.fields）',
        kind: 'text',
        placeholder: 'id',
        help: 'upsert / update 必填，逗号分隔，顺序与目标表主键一致。',
      },
      {
        key: 'delete.enabled',
        label: '同步删除',
        kind: 'select',
        options: ['true', 'false'],
        help: '上游的删除事件要不要真的删目标行；数仓明细表一般是 false。',
      },
      {
        key: 'table.name.format',
        label: '目标表名模板',
        kind: 'text',
        placeholder: '${topic}',
        help: '${topic} 表示沿用 topic 名；想统一放某个 schema 可以写成 dw_${topic}。',
      },
    ],
  },
  converterGroup,
  {
    title: '容错',
    desc: '坏消息（字段对不上、主键冲突）怎么处理；排查期建议保守，稳定后再放开。',
    fields: [
      {
        key: 'errors.tolerance',
        label: '错误容忍',
        kind: 'select',
        options: ['none', 'all'],
        help: 'none：一条坏消息就停任务；all：跳过继续。跳过时必须有地方记录，否则等于丢数据。',
      },
      {
        key: 'errors.log.enable',
        label: '记录错误日志',
        kind: 'select',
        options: ['true', 'false'],
      },
      {
        key: 'errors.deadletterqueue.topic.name',
        label: '死信 topic',
        kind: 'text',
        placeholder: 'dbz-dlq',
        help: '配了它、并且错误容忍为 all 时，坏消息进这个 topic 而不是直接丢。',
      },
    ],
  },
];

const SINK_TEMPLATES: Template[] = [
  {
    label: 'JDBC Sink 模板',
    desc: '按原样追加写入：不依赖主键，适合明细表。',
    // insert.mode 也直接改写：两个模板互为「切换」，点哪条就该是哪条
    overwrite: {
      'connector.class': 'io.debezium.connector.jdbc.JdbcSinkConnector',
      'insert.mode': 'insert',
    },
    // insert 模式下主键配置用不上，留着容易让人以为在按主键写
    stale: ['primary.key.mode', 'primary.key.fields'],
    fill: {
      'delete.enabled': 'false',
      'table.name.format': '${topic}',
      'tasks.max': '1',
      ...CONVERTER_FILL,
    },
  },
  {
    label: 'JDBC Sink 模板（upsert）',
    desc: '按主键覆盖写入：还要补 primary.key.fields，删除会同步到目标表。',
    overwrite: {
      'connector.class': 'io.debezium.connector.jdbc.JdbcSinkConnector',
      'insert.mode': 'upsert',
    },
    stale: [],
    fill: {
      'primary.key.mode': 'record_value',
      'delete.enabled': 'true',
      'table.name.format': '${topic}',
      'tasks.max': '1',
      ...CONVERTER_FILL,
    },
  },
];

/** 下游没有方言差别，但「upsert 却不给主键」是部署时才会暴露的错，提前说一句 */
function sinkWarnings(config: Record<string, unknown>): string[] {
  const mode = toText(config['insert.mode']);
  const keyMode = toText(config['primary.key.mode']);
  const fields = toText(config['primary.key.fields']).trim();
  if ((mode === 'upsert' || mode === 'update') && keyMode === 'record_value' && fields === '') {
    return ['写入模式是 upsert / update，但没填 primary.key.fields：连接器起不来。'];
  }
  return [];
}

/* ---------------------------------------------------------------- 档案 */

const PROFILES: Record<ConnectorKind, Profile> = {
  source: {
    groups: SOURCE_GROUPS,
    templates: SOURCE_TEMPLATES,
    numberKeys: new Set(['database.port']),
    dialect: sourceDialect,
  },
  sink: {
    groups: SINK_GROUPS,
    templates: SINK_TEMPLATES,
    numberKeys: new Set(['tasks.max']),
    warnings: sinkWarnings,
  },
};

interface ConnectorConfigEditorProps {
  /** 上游还是下游；默认上游 */
  kind?: ConnectorKind;
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

/** 套用模板的结果，用来给一句「刚才到底改了什么」 */
interface TemplateResult {
  label: string;
  desc: string;
  overwritten: number;
  cleared: number;
  filled: number;
}

export default function ConnectorConfigEditor({
  kind = 'source',
  value = '',
  onChange,
}: ConnectorConfigEditorProps) {
  const profile = PROFILES[kind];
  const [mode, setMode] = useState<'gui' | 'json'>('gui');
  const [applied, setApplied] = useState<TemplateResult | null>(null);
  const parsed = useMemo(() => parseObject(value), [value]);
  const canUseGui = parsed !== null;
  const dialect = parsed ? (profile.dialect?.(parsed) ?? null) : null;

  /** 数值键写成数字；其余原样写字符串 */
  const write = (draft: Record<string, unknown>, key: string, next: string) => {
    if (profile.numberKeys.has(key)) {
      const number = Number(next);
      draft[key] = Number.isFinite(number) ? number : next;
      return;
    }
    draft[key] = next;
  };

  /** 写回一个键；空值等于删掉这个键（Kafka Connect 对空串的处理与「没配」并不等价） */
  const setField = (key: string, next: string) => {
    const draft: Record<string, unknown> = { ...(parsed ?? {}) };
    if (next === '') {
      delete draft[key];
    } else {
      write(draft, key, next);
    }
    setApplied(null);
    onChange?.(JSON.stringify(draft, null, 2));
  };

  /**
   * 套用模板
   *
   * 顺序是「先清另一家、再改写方言键、最后补默认值」：先清是为了让 `fill` 能补回来
   * （例如从 MySQL 换到 PostgreSQL 时 schema.include.list 被清空后要补 public）。
   */
  const applyTemplate = (template: Template) => {
    const draft: Record<string, unknown> = { ...(parsed ?? {}) };
    let overwritten = 0;
    let cleared = 0;
    let filled = 0;

    for (const key of template.stale) {
      if (key in draft) {
        delete draft[key];
        cleared += 1;
      }
    }

    for (const [key, next] of Object.entries(template.overwrite)) {
      if (toText(draft[key]) !== next) {
        write(draft, key, next);
        overwritten += 1;
      }
    }

    if (template.port) {
      const current = toText(draft[template.port.key]);
      // 空着、或还是模板写的默认端口 → 说明用户没定制过，跟着换；自己填过端口就不动
      if (current === '') {
        write(draft, template.port.key, template.port.value);
        filled += 1;
      } else if (current !== template.port.value && template.port.generated.includes(current)) {
        write(draft, template.port.key, template.port.value);
        overwritten += 1;
      }
    }

    for (const [key, next] of Object.entries(template.fill)) {
      if (toText(draft[key]) === '') {
        write(draft, key, next);
        filled += 1;
      }
    }

    setApplied({ label: template.label, desc: template.desc, overwritten, cleared, filled });
    onChange?.(JSON.stringify(draft, null, 2));
  };

  const visibleGroups = useMemo(
    () =>
      profile.groups.map((group) => ({
        ...group,
        fields: group.fields.filter(
          (field) => !field.dialects || dialect === null || field.dialects.includes(dialect),
        ),
      })),
    [profile, dialect],
  );

  const missing = useMemo(() => {
    if (!parsed) {
      return [];
    }
    return visibleGroups
      .flatMap((group) => group.fields)
      .filter((field) => field.required)
      .filter((field) => toText(parsed[field.key]).trim() === '')
      .map((field) => field.label);
  }, [parsed, visibleGroups]);

  const warnings = useMemo(() => (parsed ? (profile.warnings?.(parsed) ?? []) : []), [parsed, profile]);

  const renderField = (field: FieldSpec) => {
    const current = toText(parsed?.[field.key]);
    const common = {
      id: field.key,
      value: current,
      placeholder: field.placeholder,
      allowClear: true,
    };

    let control;
    if (field.kind === 'select') {
      control = field.freeform ? (
        <AutoComplete
          id={field.key}
          value={current}
          options={field.options?.map((option) => ({ value: option }))}
          placeholder={field.placeholder}
          filterOption
          onChange={(next) => setField(field.key, next ?? '')}
        />
      ) : (
        <Select
          id={field.key}
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
        <label htmlFor={field.key} style={{ display: 'block', marginBottom: 4 }}>
          <Typography.Text>
            {field.label}
            {field.required && <Typography.Text type="danger"> *</Typography.Text>}
          </Typography.Text>
          <Typography.Text type="secondary" className="dbz-mono" style={{ marginLeft: 8, fontSize: 12 }}>
            {field.key}
          </Typography.Text>
        </label>
        {control}
        {field.help && (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {field.help}
          </Typography.Text>
        )}
      </div>
    );
  };

  const resultText = applied
    ? `已套用 ${applied.label}：${[
        applied.overwritten > 0 && `改写 ${applied.overwritten} 项`,
        applied.filled > 0 && `补 ${applied.filled} 项`,
        applied.cleared > 0 && `清掉 ${applied.cleared} 项`,
      ]
        .filter(Boolean)
        .join('、') || '没有需要改的键'}`
    : '';
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
          profile.templates.map((template) => (
            <Button key={template.label} size="small" onClick={() => applyTemplate(template)}>
              套用 {template.label}
            </Button>
          ))}
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          {mode === 'gui'
            ? '只列出常用项，其他参数在 JSON 里加；切过去不会丢'
            : '这里是最终提交的内容，图形化改动会实时反映过来'}
        </Typography.Text>
      </Space>

      {mode === 'gui' && applied && (
        <Space direction="vertical" size={2}>
          <Typography.Text type="success" style={{ fontSize: 12 }}>
            {resultText}
          </Typography.Text>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            {applied.desc}
          </Typography.Text>
        </Space>
      )}

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
          {warnings.length > 0 && (
            <Alert
              type="warning"
              showIcon
              message="配置前后矛盾"
              description={warnings.join('')}
            />
          )}
          {visibleGroups.map((group) => (
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
        </>
      )}
    </Space>
  );
}
