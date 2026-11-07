/**
 * # 冒烟测试
 *
 * 用 Playwright 起一个真实的 Chromium，把 `/dbz/**` 全部拦下来喂夹具，因此**不需要 dbz-svr**、
 * 也不碰数据库，就能把每个页面真实渲染一遍，并校验：
 *
 * - 页面能渲染、表格行数与状态标签正确；
 * - 写接口带了 `X-User-Id`、请求体是 camelCase；
 * - 表单默认值、编辑回填、提交后关闭；
 * - 联动字段（broker 选 SASL 才出现认证项）、双模式编辑器（图形化 ⇄ JSON）、
 *   快照弹窗按通道重新预检、远端连接器的停止与重置位点；
 * - 悬停状态弹出运行详情：失败原因要真的从 `runMsg` / trace 渲染出来；
 * - 布局体检：顶栏标题不被裁、表格不横向溢出、工具栏不换行。
 *
 * ## 用法
 *
 * ```bash
 * npx playwright install chromium   # 首次，约 100MB
 * npm run smoke
 * # 系统不支持 Playwright 自带浏览器时，可复用已有 Chromium：
 * SMOKE_CHROME=/path/to/chrome-headless-shell npm run smoke
 * ```
 *
 * 截图输出到系统临时目录 `dbz-web-smoke/shots`，失败时按截图定位问题。
 */

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { createServer } from 'vite';

const SHOTS = join(tmpdir(), 'dbz-web-smoke', 'shots');
mkdirSync(SHOTS, { recursive: true });

/* ------------------------------------------------------------------ 夹具 */

/** 包一层 Ro 信封 */
const ro = (extra) => ({ result: 1, msg: '查询成功', timestamp: Date.now(), extra });

// 后端 u64 一律序列化成字符串（见 src/api/types.ts 的 Id），夹具必须照着来，
// 否则 Number() / dayjs() 这类精度与解析问题在冒烟里看不出来。
const brokers = [
  {
    id: '401',
    name: '订单消息集群',
    bootstrapServers: '10.0.0.21:9092,10.0.0.22:9092',
    securityProtocol: 0,
    saslMechanism: 0,
    saslUsername: null,
    runStatus: 1,
    remark: '下游全量同步都写这里',
    enabled: true,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000600000',
  },
  {
    id: '402',
    name: '风控消息集群',
    bootstrapServers: '10.0.1.31:9092',
    securityProtocol: 1,
    saslMechanism: 2,
    saslUsername: 'dbz',
    runStatus: 3,
    runMsg:
      '连接 Kafka 集群<风控消息集群>失败（10.0.1.31:9092）：SASL authentication failed, broker closed the connection',
    enabled: true,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000700000',
  },
  {
    // 禁用 + 没有任何 worker 指向它：用来验证「禁用中的集群不算异常」与「未挂载提示」
    id: '403',
    name: '归档消息集群',
    bootstrapServers: '10.0.2.41:9092',
    securityProtocol: 0,
    saslMechanism: 0,
    saslUsername: null,
    runStatus: 2,
    enabled: false,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000800000',
  },
];

const clusters = [
  {
    id: '101',
    name: '生产集群-华东',
    url: 'http://10.0.0.11:8083',
    kafkaBrokerId: '401',
    runStatus: 1,
    remark: '主力集群',
    enabled: true,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000000000',
  },
  {
    id: '102',
    name: '灰度集群',
    url: 'http://10.0.0.12:8083',
    kafkaBrokerId: '402',
    runStatus: 3,
    runMsg:
      '连接 Kafka Connect 失败: error sending request for url (http://10.0.0.12:8083/connectors): connection refused',
    enabled: true,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000500000',
  },
];

const sources = [
  {
    id: '201',
    kafkaConnectId: '101',
    name: '订单库增量',
    connectorName: 'mysql-orders-source',
    connectorConfig:
      '{"connector.class":"io.debezium.connector.mysql.MySqlConnector","snapshot.mode":"when_needed","database.password":"dbz-secret"}',
    runStatus: 1,
    remark: '',
    enabled: true,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000100000',
  },
  {
    // 19 位雪花 ID：验证列表里的长 ID 截断（完整值仍放 Tooltip）
    id: '1928374650123456789',
    kafkaConnectId: '102',
    name: '用户库增量',
    connectorName: 'mysql-users-source',
    connectorConfig: '{"connector.class":"io.debezium.connector.mysql.MySqlConnector"}',
    runStatus: 6,
    runMsg: "任务 #0: io.debezium.DebeziumException: Access denied for user 'dbz'@'10.0.0.%'",
    enabled: true,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000200000',
  },
];

const sinks = [
  {
    id: '301',
    sourceConnectorId: '201',
    kafkaConnectId: '101',
    name: '订单同步到数仓',
    connectorName: 'jdbc-orders-sink',
    connectorConfig:
      '{"connector.class":"io.debezium.connector.jdbc.JdbcSinkConnector","auto.offset.reset":"earliest"}',
    runStatus: 4,
    runMsg:
      '部署连接器失败: {"error_code":400,"message":"Connector configuration is invalid: connection.url is required"}',
    enabled: true,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000300000',
  },
];

/** 死信：901/903 为可改报文的确定性字段不合规，902 为传输类（禁止改报文） */
const deadLetters = [
  {
    id: '901',
    connectorType: 2,
    connectorId: '301',
    topic: 'dbz.orders',
    partition: 0,
    offset: 1001,
    recordKey: '{"id":1001}',
    payload: { id: 1001, order_no: 'SO-20261107-1', amount: 99.5 },
    failureCategory: 1,
    failureSubtype: 1,
    canEditPayload: true,
    pendingPayload: null,
    payloadEditedBy: null,
    payloadEditedMs: null,
    lastRetrySuccess: null,
    lastRetryMs: null,
    lastRetryMessage: null,
    reason: '字段校验不通过，请核对并修改报文后重投',
    failureDetail:
      'HTTP 200 result=-2\n' +
      'response: {"code":"FIELD_INVALID","message":"amount must be number","field":"amount"}\n' +
      'record: {"id":1001,"order_no":"SO-20261107-1","amount":99.5}\n' +
      'validate: amount=99.5 is not a number\n' +
      'validate: shipping_address is required\n' +
      'rtw: error_code=2201\n' +
      'trace: com.dbz.sync.FieldValidator.validate(FieldValidator.java:88)\n' +
      'trace: com.dbz.sync.HttpSink.apply(HttpSink.java:41)',
    retryCount: 1,
    firstSeenMs: '1760000000000',
    lastSeenMs: '1760000100000',
    status: 0,
    handledBy: null,
    handledMs: null,
    remark: null,
    createMs: '1760000000000',
    updateMs: '1760000100000',
  },
  {
    id: '902',
    connectorType: 2,
    connectorId: '301',
    topic: 'dbz.orders',
    partition: 0,
    offset: 1002,
    recordKey: '{"id":1002}',
    payload: { id: 1002, order_no: 'SO-20261107-2', amount: 66 },
    failureCategory: 2,
    failureSubtype: 0,
    canEditPayload: false,
    pendingPayload: null,
    payloadEditedBy: null,
    payloadEditedMs: null,
    lastRetrySuccess: null,
    lastRetryMs: null,
    lastRetryMessage: null,
    reason: '下游连接失败，重试多次后仍失败',
    failureDetail: '连接下游超时（15s），退避 10 次后仍失败：request timed out',
    retryCount: 10,
    firstSeenMs: '1759990000000',
    lastSeenMs: '1759999900000',
    status: 1,
    handledBy: '1',
    handledMs: '1759999950000',
    remark: '重投成功',
    createMs: '1759990000000',
    updateMs: '1759999950000',
  },
  {
    id: '903',
    connectorType: 2,
    connectorId: '301',
    topic: 'dbz.orders',
    partition: 1,
    offset: 1003,
    recordKey: '{"id":1003}',
    payload: { id: 1003, order_no: 'SO-20261107-3', amount: 'abc', shipping_address: null },
    failureCategory: 1,
    failureSubtype: 1,
    canEditPayload: true,
    pendingPayload: { id: 1003, order_no: 'SO-20261107-3', amount: 88, shipping_address: null },
    payloadEditedBy: '1',
    payloadEditedMs: '1760000500000',
    lastRetrySuccess: null,
    lastRetryMs: null,
    lastRetryMessage: null,
    reason: '字段校验不通过，请核对并修改报文后重投',
    failureDetail:
      '历史记录原始报错：\n' +
      'HTTP 200 result=-2\n' +
      'response: {"code":"FIELD_INVALID","message":"amount must be number"}\n' +
      'validate: shipping_address is required',
    retryCount: 2,
    firstSeenMs: '1760000200000',
    lastSeenMs: '1760000400000',
    status: 0,
    handledBy: null,
    handledMs: null,
    remark: null,
    createMs: '1760000200000',
    updateMs: '1760000500000',
  },
];

const connectors = [
  {
    name: 'mysql-orders-source',
    connectorClass: 'io.debezium.connector.mysql.MySqlConnector',
    type: 'source',
    state: 0,
    workerId: '10.0.0.11:8083',
    trace: null,
    tasks: [
      { id: 0, state: 0, workerId: '10.0.0.11:8083' },
      {
        id: 1,
        state: 1,
        workerId: '10.0.0.11:8083',
        trace: 'org.apache.kafka.connect.errors.ConnectException: boom\n\tat ...',
      },
    ],
  },
  {
    name: 'jdbc-orders-sink',
    connectorClass: 'io.debezium.connector.jdbc.JdbcSinkConnector',
    type: 'sink',
    state: 0,
    workerId: '10.0.0.11:8083',
    trace: null,
    tasks: [{ id: 0, state: 0, workerId: '10.0.0.11:8083' }],
  },
  {
    name: 'manual-test-connector',
    connectorClass: 'io.debezium.connector.postgresql.PostgresConnector',
    type: 'source',
    state: 1,
    workerId: '10.0.0.12:8083',
    trace:
      'io.debezium.DebeziumException: Failed to read from the WAL\n\tat io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.execute(...)',
    tasks: [],
  },
];

/** 每台 Connect 上真实存在的连接器：导入冲突检测按 kafkaConnectId 取 */
const connectorsByConnect = {
  101: [connectors[0], connectors[1]],
  102: [
    {
      name: 'mysql-users-source',
      connectorClass: 'io.debezium.connector.mysql.MySqlConnector',
      type: 'source',
      state: 0,
      workerId: '10.0.0.12:8083',
      trace: null,
      tasks: [{ id: 0, state: 0, workerId: '10.0.0.12:8083' }],
    },
    connectors[2],
  ],
};

const plugins = [
  { class: 'io.debezium.connector.mysql.MySqlConnector', type: 'source', version: '2.5.0.Final' },
  { class: 'io.debezium.connector.jdbc.JdbcSinkConnector', type: 'sink', version: '2.5.0.Final' },
];

/** 单集群模式：只返回 401 下的数据，用来验证 tab 栏在单集群时隐藏 */
let singleClusterOnly = false;
const visibleBrokers = () => (singleClusterOnly ? brokers.filter((item) => item.id === '401') : brokers);
const visibleClusters = () =>
  singleClusterOnly ? clusters.filter((item) => item.kafkaBrokerId === '401') : clusters;
const visibleSources = () =>
  singleClusterOnly ? sources.filter((item) => item.kafkaConnectId === '101') : sources;
const visibleSinks = () =>
  singleClusterOnly ? sinks.filter((item) => item.kafkaConnectId === '101') : sinks;

const pageOf = (list) => ro({ pageNum: 1, total: list.length, list });

/**
 * 快照预检结果
 *
 * 两条通道能读到的范围本来就不一样，所以按 `channel` 返回两份不同的数据：
 * `source` 是源库表名，`kafka` 是交给连接器展开的正则，且没有信号表。
 */
const snapshotPreview = (channel) =>
  channel === 'kafka'
    ? {
        dialect: 'mysql',
        database: 'orders',
        channels: ['source', 'kafka'],
        channel: 'kafka',
        signalCollection: null,
        kafkaTopic: 'dbz-signal',
        kafkaBroker: '订单消息集群',
        dataCollections: ['orders\\.*', 'settle\\.bill'],
        warnings: [],
      }
    : {
        dialect: 'mysql',
        database: 'orders',
        channels: ['source', 'kafka'],
        channel: 'source',
        signalCollection: 'orders.debezium_signal',
        kafkaTopic: null,
        kafkaBroker: null,
        dataCollections: ['orders.t_order', 'orders.t_order_item'],
        warnings: ['table.include.list 里的 orders.t_coupon 没有匹配到表'],
      };

/* -------------------------------------------------------------- 断言工具 */

const checks = [];

/** 记一条断言结果 */
function check(name, ok, detail = '') {
  checks.push({ name, ok });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
}

/**
 * # 布局体检
 *
 * 这两项是截图里最容易发现、断言又最难覆盖的问题：表格横向溢出会让右侧固定列压住前一列，
 * 工具栏换行会让操作按钮掉到第二行。
 */
async function layoutCheck(target, label) {
  const overflow = await target.evaluate(() => {
    const el = document.querySelector('.ant-table-content');
    return el ? el.scrollWidth - el.clientWidth : 0;
  });
  check(`${label}表格无横向溢出`, overflow <= 0, `溢出 ${overflow}px`);

  const tops = await target.evaluate(() => {
    const search = document.querySelector('.dbz-toolbar .ant-input-search');
    const button = [...document.querySelectorAll('.dbz-toolbar button')].find((item) =>
      item.textContent.includes('新增'),
    );
    if (!search || !button) {
      return null;
    }
    return {
      search: Math.round(search.getBoundingClientRect().top),
      button: Math.round(button.getBoundingClientRect().top),
    };
  });
  check(`${label}工具栏单行`, Boolean(tops) && Math.abs(tops.search - tops.button) < 6, JSON.stringify(tops));
}

/**
 * # 只对可见弹窗操作
 *
 * antd 关掉的 Modal 在退出动画期间还留在 DOM 里，`text=` 这类选择器照样能匹配到隐藏元素，
 * 于是「下游表单里没有 database.include.list」这种断言会被上游的残留节点骗过去。
 */
const visibleModal = () => page.locator('.ant-modal-wrap:visible');

/**
 * # 选中画布节点
 *
 * 直接派发 pointerdown/pointerup：节点可能被图例、缩略图等浮层压住，
 * 这里只验证选中逻辑，不再为像素级遮挡单开一条用例。
 */
async function selectNode(key) {
  const node = page.locator(`.dbz-graph-node[data-node-key="${key}"]`);
  await node.dispatchEvent('pointerdown', { pointerId: 1, button: 0, buttons: 1 });
  await node.dispatchEvent('pointerup', { pointerId: 1, button: 0 });
}

/**
 * # 点「取消」关表单弹窗
 *
 * 改过字段的表单会先弹「放弃未保存的修改？」（脏状态确认）；只浏览过的编辑表单
 * 不应该弹。这里按预期断言，避免以后把确认逻辑删了却没人发现。
 */
async function waitNoVisibleModal() {
  try {
    await page.waitForFunction(
      () =>
        Array.from(document.querySelectorAll('.ant-modal-wrap')).every((element) => {
          const style = window.getComputedStyle(element);
          return style.display === 'none' || style.visibility === 'hidden';
        }),
      undefined,
      { timeout: 10000 },
    );
  } catch (error) {
    const openModals = await page.locator('.ant-modal-wrap:visible').evaluateAll((wrappers) =>
      wrappers.map((wrapper) => ({
        className: wrapper.firstElementChild?.className ?? '',
        text: wrapper.innerText.slice(0, 120),
      })),
    );
    console.log('DIAG visible modals', JSON.stringify(openModals));
    throw error;
  }
}

async function cancelFormModal({ expectDirty = false } = {}) {
  // 旧弹窗的 confirm 容器会在 DOM 里留一层，取最后一个（最上层）才是当前表单
  await visibleModal().last().locator('.ant-modal-footer .ant-btn').first().click();
  if (expectDirty) {
    const confirm = page.locator('.ant-modal-confirm');
    await confirm.waitFor({ state: 'visible', timeout: 3000 });
    check(
      '脏表单取消先确认',
      (await confirm.innerText()).includes('放弃未保存的修改'),
      await confirm.innerText(),
    );
    await confirm.locator('.ant-btn-primary').click();
  }
  await waitNoVisibleModal();
}

/**
 * # 悬停状态看运行详情
 *
 * 列表里的状态是检查任务的结论，失败原因另外存（库里的 `run_msg`、远端集群返回的
 * trace），靠悬停弹出。这里断言弹层里确实出现了那句话——只断言标签的话，把 `runMsg`
 * 从 VO 里漏掉、或某页忘了传 `detail`，都测不出来。
 *
 * ## 参数
 * * `rowText` - 用来定位表格行的文案（名称）
 * * `expectText` - 弹层里必须出现的片段（失败原因的关键部分）
 * * `label` - 断言名前缀
 */
async function hoverStatus(rowText, expectText, label, shot) {
  const row = page.locator('tbody tr.ant-table-row', { hasText: rowText }).first();
  await row.locator('.dbz-status-hover').first().hover();
  const popover = page.locator('.ant-popover:visible').first();
  await popover.waitFor({ state: 'visible', timeout: 5000 });
  // 等淡入动画结束再读文本/截图，否则拿到的是半透明的中间帧
  await page.waitForTimeout(350);
  const content = (await popover.innerText()).replace(/\s+/g, ' ');
  const ok = content.includes(expectText);
  check(`${label}悬停状态能看到运行详情`, ok, ok ? '' : content.slice(0, 160));
  if (shot) {
    await page.screenshot({ path: `${SHOTS}/${shot}` });
  }
  // 移开鼠标：弹层会盖住下一行，点按钮就点不到了
  await page.mouse.move(0, 0);
  await page.waitForTimeout(250);
  return content;
}

/* ---------------------------------------------------------------- 跑起来 */

const server = await createServer({
  logLevel: 'error',
  server: { port: 5199, strictPort: false },
});
await server.listen();
const base = server.resolvedUrls.local[0].replace(/\/$/, '');

// `SMOKE_CHROME` 可指向已有的 Chromium（例如 CI/本机缓存），省去重复下载；
// 不设置时走 Playwright 默认安装路径。
const browser = await chromium.launch({
  executablePath: process.env.SMOKE_CHROME || undefined,
});
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });

/**
 * 登录态夹具：业务用例都跑在已登录的前提下，首次进入前写入 localStorage 会话。
 * `sessionStorage` 标记保证「登出 / 令牌失效」用例之后不会被重新注入。
 */
const seedSession = {
  token: 'smoke-seed-token',
  tokenType: 'Bearer',
  expireMs: String(Date.now() + 24 * 3600 * 1000),
  userId: '7',
  name: '冒烟用户',
  nickName: '冒烟用户',
  loginPswdDefault: false,
  loginLogId: '1',
};
await context.addInitScript((session) => {
  if (!sessionStorage.getItem('dbz.smoke.auth-tested')) {
    localStorage.setItem('dbz.auth.session', JSON.stringify(session));
  }
}, seedSession);

const page = await context.newPage();

const pageErrors = [];
const consoleErrors = [];
const posted = [];
/** 只读接口单独记录：`posted` 只装写请求，GET 断言不能从它里翻 */
const getRequests = [];
/** 控制面画布布局：PUT 落库、GET 读取，用来验证「刷新后位置保持」 */
const layoutStore = [];
let nextBrokerId = 499;
/** 导入会真的新增连接器：返回带 id 的记录，让下游按上游 id 重挂 */
let nextSourceId = 601;
let nextSinkId = 701;
/** 置 true 让下一次预检失败一次，用完自动复位：用来验证「预检失败能原地重试」 */
let failNextPreview = false;
/** 控制面状态刷新次数：验证进入页面、手动刷新与自动刷新都真的回写了状态 */
let refreshCalls = 0;
/** 置 true 让下一次重置位点失败：验证弹窗说明停在哪一步 */
let failNextReset = false;
/** 置 true 让下一次启用开关失败：验证开关回滚 */
let failNextSinkToggle = false;
/** 下一次删除口令校验的失败类型：wrong=口令错误，rate=频控，unavailable=认证服务不可用 */
let failNextDelete = null;
/** 下一次登录的失败类型：wrong=账号或口令不正确，network=网络错误 */
let failNextLogin = null;
/** 置 true 让下一次控制面状态刷新返回 401，验证令牌失效回登录页 */
let failNext401 = false;
/** 401 用例期间忽略浏览器打印的资源加载错误（这是用例主动制造的） */
let ignore401Console = false;
/** 登录请求记录：验证 loginPswd 是 64 位摘要而不是明文 */
const loginRequests = [];
/** 连线折点：edgeId → midY，模拟服务端持久化 */
const edgeBendStore = new Map();

page.on('pageerror', (error) => pageErrors.push(String(error)));
page.on('console', (message) => {
  if (message.type() === 'error') {
    if (ignore401Console && message.text().includes('401')) {
      return;
    }
    consoleErrors.push(message.text());
  }
});

// adm-rs 登录/登出：不依赖真实服务，直接按契约返回
await page.route('**/admrs-svr/**', async (route) => {
  const request = route.request();
  const path = new URL(request.url()).pathname;

  if (path === '/admrs-svr/adm/auth/login' && request.method() === 'POST') {
    const body = request.postDataJSON();
    loginRequests.push({ body, headers: request.headers() });
    if (failNextLogin === 'network') {
      failNextLogin = null;
      return route.abort('failed');
    }
    if (failNextLogin === 'wrong') {
      failNextLogin = null;
      return route.fulfill({
        json: {
          result: -2,
          msg: '账号或口令不正确',
          code: 'ADM00001',
          timestamp: Date.now(),
        },
      });
    }
    return route.fulfill({
      json: ro({
        token: 'smoke-login-token',
        tokenType: 'Bearer',
        expireMs: String(Date.now() + 3600 * 1000),
        userId: '7',
        name: 'smoke-user',
        nickName: '冒烟用户',
        loginPswdDefault: false,
        loginLogId: '2',
      }),
    });
  }

  if (path === '/admrs-svr/adm/auth/logout' && request.method() === 'POST') {
    return route.fulfill({ json: ro({ logoutMs: String(Date.now()), loginLogId: '1' }) });
  }

  return route.fulfill({ status: 404, json: { result: -1, msg: '未模拟的认证接口' } });
});

await page.route('**/dbz/**', async (route) => {
  const request = route.request();
  const url = new URL(request.url());
  const path = url.pathname;

  if (request.method() === 'GET') {
    getRequests.push({ path, query: url.search, headers: request.headers() });
  }

  // 令牌失效：返回 401，前端应清会话并跳登录页
  if (failNext401 && path === '/dbz/control-plane/status/refresh') {
    failNext401 = false;
    return route.fulfill({
      status: 401,
      json: { result: -1, msg: '登录状态已失效', code: 'ADM00005', timestamp: Date.now() },
    });
  }

  if (request.method() !== 'GET') {
    posted.push({
      method: request.method(),
      path,
      query: url.search,
      headers: request.headers(),
      // DELETE 不带请求体，postDataJSON() 在没有 body 时会抛错
      body: request.postData() ? request.postDataJSON() : null,
    });
    // 检测接口要回业务数据（reachable/topicCount…），空信封会让页面走进异常分支
    if (path === '/dbz/kafka-broker/check') {
      return route.fulfill({ json: ro({ reachable: true, topicCount: 12, elapsedMs: 34 }) });
    }
    // 死信重投：返回逐条结果；带 payload 表示用修改后的报文，903 模拟仍然失败
    if (path === '/dbz/dead-letter/retry') {
      const body = request.postDataJSON();
      return route.fulfill({
        json: ro(
          (body.ids ?? []).map((id) =>
            id === '903' && body.payload
              ? {
                  id,
                  success: false,
                  message: '字段校验仍失败：shipping_address 必填，amount 类型不匹配',
                }
              : { id, success: true, message: '重投成功' },
          ),
        ),
      });
    }
    // 保存待投报文：只返回待投与编辑审计，模拟原始报文不被覆盖
    if (path === '/dbz/dead-letter/edit-payload') {
      const body = request.postDataJSON();
      const record = deadLetters.find((item) => item.id === body.id);
      return route.fulfill({
        json: ro({
          ...record,
          pendingPayload: JSON.parse(body.payload),
          payloadEditedBy: '1',
          payloadEditedMs: String(Date.now()),
        }),
      });
    }
    // 死信废弃只改状态，返回空信封即可
    if (path === '/dbz/dead-letter/discard') {
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    }
    // 标记已人工处理 / 撤销：改夹具状态，验证列表与终态标签
    if (path === '/dbz/dead-letter/mark-handled') {
      const body = request.postDataJSON();
      for (const id of body.ids ?? []) {
        const record = deadLetters.find((item) => item.id === id);
        if (record) {
          record.status = 3;
          record.handledBy = '1';
          record.handledMs = String(Date.now());
          record.remark = body.remark;
          record.updateMs = String(Date.now());
        }
      }
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    }
    if (path === '/dbz/dead-letter/unmark-handled') {
      const body = request.postDataJSON();
      for (const id of body.ids ?? []) {
        const record = deadLetters.find((item) => item.id === id);
        if (record) {
          record.status = 0;
          record.handledBy = null;
          record.handledMs = null;
          record.remark = null;
          record.updateMs = String(Date.now());
        }
      }
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    }
    // 重置位点：按停 → 清 → 恢复三步返回；失败时给出停在哪一步与当前状态
    if (path === '/dbz/control-plane/connector/reset-offsets') {
      const body = request.postDataJSON();
      if (failNextReset) {
        failNextReset = false;
        return route.fulfill({
          json: ro({
            ok: false,
            failedStep: 'clear',
            message: '删除位点失败：Connect 返回 409（连接器状态为 STOPPED）',
            connectorState: '已停止',
            steps: [
              { step: 'stop', ok: true, message: '已停止连接器' },
              { step: 'clear', ok: false, message: '删除位点失败' },
            ],
          }),
        });
      }
      const steps = [
        { step: 'stop', ok: true, message: '已停止连接器' },
        { step: 'clear', ok: true, message: '已清空位点' },
      ];
      if (body.resume) {
        steps.push({ step: 'resume', ok: true, message: '已恢复运行' });
      }
      return route.fulfill({
        json: ro({
          ok: true,
          message: body.resume ? '已清空位点并恢复运行' : '已清空位点',
          connectorState: body.resume ? '运行中' : '已停止',
          steps,
        }),
      });
    }
    // 启用开关：把 enabled 写回夹具，页面重新拉数据后开关与服务端一致
    if (
      request.method() === 'PUT' &&
      ['/dbz/kafka-broker', '/dbz/kafka-connect', '/dbz/source-connector', '/dbz/sink-connector'].includes(
        path,
      )
    ) {
      if (path === '/dbz/sink-connector' && failNextSinkToggle) {
        failNextSinkToggle = false;
        return route.fulfill({
          json: { result: 0, msg: '模拟停用失败：服务端拒绝', timestamp: Date.now() },
        });
      }
      const body = request.postDataJSON();
      const store =
        path === '/dbz/kafka-broker'
          ? brokers
          : path === '/dbz/kafka-connect'
            ? clusters
            : path === '/dbz/source-connector'
              ? sources
              : sinks;
      const record = store.find((item) => item.id === body.id);
      if (record) {
        Object.assign(record, body);
      }
      return route.fulfill({ json: ro(record ?? null) });
    }
    // 画布布局：按 (nodeType, nodeId) 覆盖写，模拟服务端的 upsert
    if (path === '/dbz/control-plane/layout' && request.method() === 'PUT') {
      const body = request.postDataJSON();
      const index = layoutStore.findIndex(
        (item) => item.nodeType === body.nodeType && item.nodeId === body.nodeId,
      );
      const row = { ...body, updateMs: String(Date.now()) };
      if (index >= 0) {
        layoutStore[index] = row;
      } else {
        layoutStore.push(row);
      }
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    }
    if (path === '/dbz/control-plane/layout' && request.method() === 'DELETE') {
      const nodeType = Number(url.searchParams.get('nodeType'));
      const nodeId = url.searchParams.get('nodeId');
      const index = layoutStore.findIndex(
        (item) => item.nodeType === nodeType && item.nodeId === nodeId,
      );
      if (index >= 0) {
        layoutStore.splice(index, 1);
      }
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    }
    // 连线折点：按 edgeId upsert / 删除，模拟服务端持久化
    if (path === '/dbz/control-plane/edges' && request.method() === 'PUT') {
      const body = request.postDataJSON();
      edgeBendStore.set(body.edgeId, body.midY);
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    }
    if (path === '/dbz/control-plane/edges' && request.method() === 'DELETE') {
      edgeBendStore.delete(url.searchParams.get('edgeId'));
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    }
    // 删除节点：口令错误 / 频控 / 认证服务不可用返回可区分文案
    if (path === '/dbz/control-plane/node-delete' && request.method() === 'POST') {
      if (failNextDelete === 'wrong') {
        failNextDelete = null;
        return route.fulfill({
          json: {
            result: 0,
            msg: '口令错误：当前登录用户口令不正确',
            timestamp: Date.now(),
          },
        });
      }
      if (failNextDelete === 'rate') {
        failNextDelete = null;
        return route.fulfill({
          json: {
            result: 0,
            msg: '口令校验过于频繁：短时间内错误尝试过多，请稍后重试',
            timestamp: Date.now(),
          },
        });
      }
      if (failNextDelete === 'unavailable') {
        failNextDelete = null;
        return route.fulfill({
          json: {
            result: 0,
            msg: '认证服务不可用：无法连接认证服务，本次删除未执行',
            timestamp: Date.now(),
          },
        });
      }
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    }
    // 上游重新同步：写入信号表，返回写入结果
    if (path === '/dbz/source-connector/snapshot') {
      const body = request.postDataJSON();
      return route.fulfill({
        json: ro({
          connectorId: body.id,
          tables: body.dataCollections ?? [],
          snapshotType: body.snapshotType,
          channel: body.channel,
          message: 'execute-snapshot 信号已写入源库信号表',
        }),
      });
    }
    // 导入新增上游：返回带 id 的记录，下游才能按 imported id 重挂
    if (path === '/dbz/source-connector' && request.method() === 'POST') {
      const body = request.postDataJSON();
      const record = {
        id: String(nextSourceId),
        kafkaConnectId: body.kafkaConnectId,
        name: body.name,
        connectorName: body.connectorName,
        connectorConfig: body.connectorConfig,
        runStatus: 0,
        runMsg: null,
        remark: body.remark ?? '',
        enabled: body.enabled ?? true,
        createrId: '1',
        createMs: String(Date.now()),
        updaterId: '1',
        updateMs: String(Date.now()),
      };
      nextSourceId += 1;
      return route.fulfill({ json: ro(record) });
    }
    // 导入新增下游：返回带 id 的记录，验证「按上游 id 重挂」
    if (path === '/dbz/sink-connector' && request.method() === 'POST') {
      const body = request.postDataJSON();
      const record = {
        id: String(nextSinkId),
        sourceConnectorId: body.sourceConnectorId,
        kafkaConnectId: body.kafkaConnectId,
        name: body.name,
        connectorName: body.connectorName,
        connectorConfig: body.connectorConfig,
        runStatus: 0,
        runMsg: null,
        remark: body.remark ?? '',
        enabled: body.enabled ?? true,
        createrId: '1',
        createMs: String(Date.now()),
        updaterId: '1',
        updateMs: String(Date.now()),
      };
      nextSinkId += 1;
      return route.fulfill({ json: ro(record) });
    }
    // 调色板新增 Kafka 集群：真的把记录塞进夹具，页面刷新后才能找到新节点并保存布局
    if (path === '/dbz/kafka-broker' && request.method() === 'POST') {
      const body = request.postDataJSON();
      const record = {
        id: String(nextBrokerId),
        name: body.name,
        bootstrapServers: body.bootstrapServers,
        securityProtocol: body.securityProtocol,
        saslMechanism: body.saslMechanism,
        saslUsername: body.saslUsername ?? null,
        runStatus: 0,
        remark: body.remark ?? '',
        enabled: body.enabled ?? true,
        createrId: '1',
        createMs: String(Date.now()),
        updaterId: '1',
        updateMs: String(Date.now()),
      };
      nextBrokerId += 1;
      brokers.push(record);
      return route.fulfill({ json: ro(record) });
    }
    await route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    return;
  }

  if (path === '/dbz/kafka-broker/page') return route.fulfill({ json: pageOf(visibleBrokers()) });
  if (path === '/dbz/kafka-broker/list') return route.fulfill({ json: ro(visibleBrokers()) });
  if (path === '/dbz/kafka-connect/page') return route.fulfill({ json: pageOf(visibleClusters()) });
  if (path === '/dbz/kafka-connect/list') return route.fulfill({ json: ro(visibleClusters()) });
  if (path === '/dbz/source-connector/page') return route.fulfill({ json: pageOf(visibleSources()) });
  if (path === '/dbz/source-connector/list') return route.fulfill({ json: ro(visibleSources()) });
  if (path === '/dbz/sink-connector/page') return route.fulfill({ json: pageOf(visibleSinks()) });
  if (path === '/dbz/sink-connector/list') return route.fulfill({ json: ro(visibleSinks()) });
  if (path === '/dbz/kafka-connector/list') {
    const kafkaConnectId = url.searchParams.get('kafkaConnectId');
    return route.fulfill({
      json: ro(kafkaConnectId ? connectorsByConnect[kafkaConnectId] ?? [] : connectors),
    });
  }
  if (path === '/dbz/kafka-connector/plugins') return route.fulfill({ json: ro(plugins) });
  // Connect 自身健康：可达、版本与 worker 数（两台启用中的 Connect 都命中）
  if (path === '/dbz/kafka-connect/health') {
    return route.fulfill({
      json: ro({
        reachable: true,
        version: '3.7.1',
        commit: '9c1f4a2',
        kafkaClusterId: 'kafka-cluster-1',
        connectorCount: 2,
        workerCount: 1,
        workerIds: ['connect-1:8083'],
        elapsedMs: 42,
      }),
    });
  }
  // 集群侧连接器配置：一致性检测拿它和库内 connectorConfig 比对
  if (path === '/dbz/kafka-connector/config') {
    const connectorName = url.searchParams.get('connectorName');
    const stored = [...sources, ...sinks].find((item) => item.connectorName === connectorName);
    let config = {};
    try {
      config = JSON.parse(stored?.connectorConfig ?? '{}');
    } catch {
      config = {};
    }
    // 上游额外多出两个连接键：模拟「库内配置缺字段，Connect 实际有」
    if (connectorName === 'mysql-orders-source') {
      config = {
        ...config,
        'database.hostname': '10.0.0.11',
        'table.include.list': 'dbz.orders',
      };
    }
    return route.fulfill({ json: ro({ connectorName, config }) });
  }
  if (path === '/dbz/dead-letter/page') return route.fulfill({ json: pageOf(deadLetters) });
  if (path === '/dbz/control-plane/status/refresh') {
    refreshCalls += 1;
    return route.fulfill({ json: ro(0) });
  }
  if (path === '/dbz/control-plane/layout') {
    return route.fulfill({ json: ro(layoutStore) });
  }
  if (path === '/dbz/control-plane/edges') {
    return route.fulfill({
      json: ro(
        [...edgeBendStore.entries()].map(([edgeId, midY]) => ({
          edgeId,
          midY,
          updateMs: String(Date.now()),
        })),
      ),
    });
  }
  if (path === '/dbz/dead-letter/pending-count') {
    return route.fulfill({
      json: ro([{ connectorType: 2, connectorId: '301', count: 1 }]),
    });
  }
  if (path === '/dbz/source-connector/snapshot/preview') {
    if (failNextPreview) {
      failNextPreview = false;
      return route.fulfill({
        json: { result: 0, msg: '连接源库失败：Connection refused (10.0.0.11:3306)', timestamp: Date.now() },
      });
    }
    // 没指定通道时服务端默认挑 source，这条默认顺序也要照着演；
    // 注意 `channels` 与本次选哪条无关，切过去还能切回来
    return route.fulfill({ json: ro(snapshotPreview(url.searchParams.get('channel') ?? 'source')) });
  }
  return route.fulfill({ json: ro(null) });
});

try {
  /* 1. 菜单与旧地址：左侧只剩控制面与死信，旧 CRUD 地址重定向到控制面 */
  await page.goto(`${base}/`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.dbz-graph-node');
  check('根地址重定向到控制面', page.url().includes('/control-plane'), page.url());
  const menuTexts = (await page.locator('.ant-menu-item').allInnerTexts()).map((text) =>
    text.replace(/\s+/g, ''),
  );
  check(
    '左侧菜单只剩控制面与死信',
    menuTexts.length === 2 &&
      menuTexts.some((text) => text.includes('控制面')) &&
      menuTexts.some((text) => text.includes('死信')),
    menuTexts.join(','),
  );
  for (const oldPath of [
    'kafka-broker',
    'kafka-connect',
    'source-connector',
    'sink-connector',
    'remote-connector',
    'dashboard',
  ]) {
    await page.goto(`${base}/${oldPath}`, { waitUntil: 'networkidle' });
    // `<Navigate>` 是 React 渲染后才执行的：networkidle 只说明资源加载完，
    // 这里要等路由真的换掉，否则会把「还没重定向」误判成失败
    await page.waitForURL('**/control-plane', { timeout: 5000 }).catch(() => {});
    check(
      `旧地址 /${oldPath} 重定向到控制面`,
      page.url().includes('/control-plane'),
      page.url(),
    );
  }
  await page.goto(`${base}/control-plane`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.dbz-graph-node');
  check(
    '画布没有指向已删页面的按钮或链接',
    (await page.locator('.dbz-canvas-page a[href*="/kafka-broker"], .dbz-canvas-page a[href*="/kafka-connect"], .dbz-canvas-page a[href*="/source-connector"], .dbz-canvas-page a[href*="/sink-connector"], .dbz-canvas-page a[href*="/remote-connector"]').count()) === 0 &&
      !(await page.locator('.dbz-canvas-page').innerText()).includes('打开明细页'),
  );

  /* 1a. 控制面视图：统一状态、统一卡片、+ 新增、只读归属线、自动刷新与重置位点 */
  // 临时补三条下游，凑齐六种统一状态；用完移除，后续死信用例仍按原始夹具
  const tempSinks = [
    {
      id: '302',
      sourceConnectorId: '201',
      kafkaConnectId: '101',
      name: '临时-已停用',
      connectorName: 'tmp-disabled-sink',
      connectorConfig: '{"connector.class":"x","auto.offset.reset":"latest"}',
      runStatus: 2,
      runMsg: null,
      remark: '',
      enabled: false,
      createrId: '1',
      createMs: '1759990000000',
      updaterId: '1',
      updateMs: '1760000900000',
    },
    {
      id: '303',
      sourceConnectorId: '201',
      kafkaConnectId: '101',
      name: '临时-失败',
      connectorName: 'tmp-failed-sink',
      connectorConfig: '{"connector.class":"x"}',
      runStatus: 3,
      runMsg: '任务持续失败：目标库连接被拒绝',
      remark: '',
      enabled: true,
      createrId: '1',
      createMs: '1759990000000',
      updaterId: '1',
      updateMs: '1760001000000',
    },
    {
      id: '304',
      sourceConnectorId: '201',
      kafkaConnectId: '101',
      name: '临时-不可达',
      connectorName: 'tmp-unreachable-sink',
      connectorConfig: '{"connector.class":"x"}',
      runStatus: 5,
      runMsg: '所属 Connect 不可达，展示最后已知状态',
      remark: '',
      enabled: true,
      createrId: '1',
      createMs: '1759990000000',
      updaterId: '1',
      updateMs: '1760001100000',
    },
  ];
  sinks.push(...tempSinks);

  // 临时夹具是在页面首次加载之后才塞进 mock 的，仅点菜单不会重新拉数据；
  // 重新进入控制面并等节点稳定，避免拿首屏未加载完的计数误判
  await page.goto(`${base}/control-plane`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.dbz-graph-node[data-node-key="1:401"]');
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 7, null, {
      timeout: 15000,
    })
    .catch(() => {});
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-edge-hit').length === 6, null, {
      timeout: 15000,
    })
    .catch(() => {});
  check(
    '默认只渲染当前集群 7 个节点（1 broker + 1 Connect + 1 上游 + 4 下游）',
    (await page.locator('.dbz-graph-node').count()) === 7,
    `实际 ${await page.locator('.dbz-graph-node').count()}`,
  );
  check(
    '当前集群只渲染归属连线（6 条）',
    (await page.locator('.dbz-graph-edge-hit').count()) === 6,
    `实际 ${await page.locator('.dbz-graph-edge-hit').count()}`,
  );

  // 多集群 tab：每个 tab 显示本集群连接器数与异常数
  check(
    '多集群时显示集群 tab 栏',
    (await page.locator('[data-cluster-tabs]').isVisible()) &&
      (await page.locator('[data-cluster-tab]').count()) === 3,
    `tab 数 ${await page.locator('[data-cluster-tab]').count()}`,
  );
  const tab401Text = (await page.locator('[data-cluster-tab="401"]').innerText()).replace(/\s+/g, ' ');
  const tab402Text = (await page.locator('[data-cluster-tab="402"]').innerText()).replace(/\s+/g, ' ');
  const tab403Text = (await page.locator('[data-cluster-tab="403"]').innerText()).replace(/\s+/g, ' ');
  check(
    'tab 显示连接器数与异常数',
    tab401Text.includes('连接器 5') &&
      tab401Text.includes('异常 2') &&
      tab402Text.includes('连接器 1') &&
      tab402Text.includes('异常 0') &&
      tab403Text.includes('连接器 0') &&
      tab403Text.includes('异常 0'),
    `${tab401Text} | ${tab402Text} | ${tab403Text}`,
  );

  // 切到风控消息集群：只显示该集群的 3 个节点，再切回来不串数据
  await page.locator('[data-cluster-tab="402"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 3, null, {
      timeout: 15000,
    })
    .catch(() => {});
  check(
    '切 tab 只显示该集群节点',
    (await page.locator('.dbz-graph-node').count()) === 3 &&
      (await page.locator('.dbz-graph-node[data-node-key="4:301"]').count()) === 0 &&
      (await page.locator('[data-canvas-summary] .ant-tag:has-text("3 个节点")').count()) === 1 &&
      (await page.locator('[data-canvas-summary] [data-active-cluster]').innerText()).includes(
        '风控消息集群',
      ),
    `节点 ${await page.locator('.dbz-graph-node').count()}`,
  );
  await page.locator('[data-cluster-tab="401"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 7, null, {
      timeout: 15000,
    })
    .catch(() => {});
  check(
    '没有数据流连线，连线不再分两种类型',
    (await page.locator('.dbz-graph-edge--flow').count()) === 0 &&
      (await page.locator('[data-edge-id^="flow:"]').count()) === 0,
  );
  check(
    '归属线是浅色实线并带箭头',
    await page
      .locator('.dbz-graph-edge-line')
      .first()
      .evaluate((element) => {
        const stroke = element.getAttribute('stroke');
        const marker = element.getAttribute('marker-end');
        return stroke === '#b7c2d1' && !element.getAttribute('stroke-dasharray') && marker === 'url(#dbz-arrow-edge)';
      }),
  );
  check(
    '组件面板恢复且只有 Kafka 集群可点击',
    (await page.locator('.dbz-component-panel').isVisible()) &&
      (await page.locator('.dbz-component-item[data-component="broker"]').evaluate(
        (element) => element.tagName === 'BUTTON',
      )) &&
      (await page.locator('.dbz-component-item.is-disabled[aria-disabled]').count()) === 3,
    '面板 1 个可点击 + 3 个禁用',
  );
  check(
    '缩略图与缩放控件可见',
    (await page.locator('.dbz-graph-minimap').isVisible()) &&
      (await page.locator('.dbz-graph-controls').isVisible()),
  );
  const legendText = await page.locator('.dbz-graph-legend').innerText();
  check(
    '图例说明端点固定、折点可拖',
    legendText.includes('归属') &&
      legendText.includes('端点固定') &&
      legendText.includes('折点') &&
      !legendText.includes('数据流'),
    legendText,
  );
  check(
    'tab 栏外汇总 Kafka 集群总数',
    (await page.locator('.dbz-canvas-topbar .ant-tag:has-text("3 个 Kafka 集群")').count()) === 1,
  );
  check(
    '画布区展示当前集群节点数',
    (await page.locator('[data-canvas-summary] .ant-tag:has-text("7 个节点")').count()) === 1,
  );
  check(
    '画布区汇总当前集群 Connect 健康可达数',
    (await page.locator('[data-canvas-summary] .ant-tag:has-text("Connect 可达 1/1")').count()) ===
      1,
  );
  check(
    'tab 栏外汇总全局待处理死信总数',
    (await page.locator('.dbz-canvas-topbar .ant-tag:has-text("死信 1")').count()) === 1,
  );
  check(
    '下游节点显示待处理死信角标',
    (await page
      .locator('.dbz-graph-node[data-node-key="4:301"] .ant-tag:has-text("死信 1")')
      .count()) >= 1,
  );

  // 单一状态：六种取值都能呈现，且每张连接器卡片只有一个状态标签
  const statusLabels = (
    await page
      .locator(
        '.dbz-graph-node--source .dbz-graph-status-tag .ant-tag, .dbz-graph-node--sink .dbz-graph-status-tag .ant-tag',
      )
      .allInnerTexts()
  ).map((item) => item.trim());
  for (const label of ['运行中', '仅纳管未部署', '失败', '不可达', '已停用']) {
    check(`统一状态「${label}」正确呈现`, statusLabels.includes(label), statusLabels.join(','));
  }
  // 「已停止」只存在于风控消息集群，切过去单独确认
  await page.locator('[data-cluster-tab="402"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 3, null, {
      timeout: 15000,
    })
    .catch(() => {});
  const otherTabStatusLabels = (
    await page
      .locator('.dbz-graph-node--source .dbz-graph-status-tag .ant-tag')
      .allInnerTexts()
  ).map((item) => item.trim());
  check(
    '统一状态「已停止」正确呈现',
    otherTabStatusLabels.includes('已停止'),
    otherTabStatusLabels.join(','),
  );
  await page.locator('[data-cluster-tab="401"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 7, null, {
      timeout: 15000,
    })
    .catch(() => {});
  const connectorStatusCounts = await page
    .locator('.dbz-graph-node--source, .dbz-graph-node--sink')
    .evaluateAll((elements) =>
      elements.map((element) => element.querySelectorAll('.dbz-graph-status-tag').length),
    );
  check(
    '每张连接器卡片只有一个状态标签',
    connectorStatusCounts.length > 0 && connectorStatusCounts.every((count) => count === 1),
    connectorStatusCounts.join(','),
  );
  check(
    '原「不一致」标记不再出现',
    (await page.locator('.dbz-graph-mismatch').count()) === 0 &&
      !(await page.locator('.dbz-canvas-topbar').innerText()).includes('不一致'),
  );

  // 四类卡片统一版式：类型文字、图标、主色、高度都要能区分
  const typeLabels = await page.locator('.dbz-graph-node-type-label').allInnerTexts();
  for (const label of ['Kafka 集群', 'Kafka Connect', '上游连接器', '下游连接器']) {
    check(`卡片类型文字「${label}」存在`, typeLabels.includes(label), typeLabels.join(','));
  }
  const kindNames = ['broker', 'connect', 'source', 'sink'];
  const iconLabels = [];
  const iconColors = [];
  const heights = [];
  for (const kind of kindNames) {
    iconLabels.push(
      await page
        .locator(`.dbz-graph-node--${kind} .dbz-graph-node-icon .anticon`)
        .first()
        .getAttribute('aria-label'),
    );
    iconColors.push(
      await page
        .locator(`.dbz-graph-node--${kind} .dbz-graph-node-icon`)
        .first()
        .evaluate((element) => window.getComputedStyle(element).backgroundColor),
    );
    heights.push(
      await page
        .locator(`.dbz-graph-node--${kind}`)
        .evaluateAll((elements) => [...new Set(elements.map((element) => element.style.height))]),
    );
  }
  check('四类图标互不相同', new Set(iconLabels).size === 4, iconLabels.join(','));
  check('四类卡片主色互不相同', new Set(iconColors).size === 4, iconColors.join(','));
  check(
    '同类卡片高度一致',
    heights.every((list) => list.length === 1),
    heights.map((list) => list.join('/')).join(' | '),
  );

  // A. 卡片布局：开关在左上，编辑最左，删除隔一道分隔线放最右
  const sinkCard = page.locator('.dbz-graph-node[data-node-key="4:301"]');
  const sinkCardBox = await sinkCard.boundingBox();
  const switchBox = await sinkCard.locator('.dbz-graph-node-head .ant-switch').boundingBox();
  const editBox = await sinkCard.locator('[data-node-action="edit"]').boundingBox();
  const deleteBox = await sinkCard.locator('[data-node-action="delete"]').boundingBox();
  const dividerBox = await sinkCard.locator('.dbz-graph-actions-divider').boundingBox();
  check(
    '启用开关在卡片左上角',
    Boolean(switchBox) &&
      switchBox.x < sinkCardBox.x + 40 &&
      switchBox.y < sinkCardBox.y + 40,
    JSON.stringify(switchBox),
  );
  check(
    '操作行没有第二个开关',
    (await sinkCard.locator('.dbz-graph-node-actions .dbz-graph-switch').count()) === 0,
  );
  check(
    '编辑最左，删除最右且中间有分隔线与间距',
    Boolean(editBox && deleteBox && dividerBox) &&
      editBox.x < dividerBox.x &&
      dividerBox.x < deleteBox.x &&
      deleteBox.x - dividerBox.x >= 6,
    `edit=${editBox?.x} divider=${dividerBox?.x} delete=${deleteBox?.x}`,
  );
  check(
    '下游卡片不再有「查看死信」图标',
    (await page.locator('.dbz-graph-node [data-node-action="dead-letter"]').count()) === 0,
  );

  // G. 画布搜索：按名称/topic/connectorName 高亮并定位，不隐藏节点
  const nodeCountBeforeSearch = await page.locator('.dbz-graph-node').count();
  const searchInput = page.locator('.dbz-canvas-search input');
  await searchInput.fill('订单消息集群');
  await page.waitForTimeout(400);
  check(
    '搜索按名称命中并高亮',
    (await page.locator('.dbz-graph-node[data-node-key="1:401"].is-hit').count()) === 1 &&
      (await page.locator('.dbz-graph-node[data-node-key="1:401"].is-active-hit').count()) === 1,
  );
  check(
    '搜索不隐藏其它节点，整体拓扑保留',
    (await page.locator('.dbz-graph-node').count()) === nodeCountBeforeSearch,
    `${nodeCountBeforeSearch}`,
  );
  check(
    '搜索结果显示 1/1',
    (await page.locator('[data-search-count]').innerText()).trim() === '1/1',
    await page.locator('[data-search-count]').innerText(),
  );
  await searchInput.fill('orders');
  await page.waitForTimeout(400);
  const multiHitCount = Number(
    ((await page.locator('[data-search-count]').innerText()).split('/')[1] ?? '0').trim(),
  );
  check('搜索按 topic/connectorName 命中多个节点', multiHitCount >= 2, `${multiHitCount}`);
  const firstActiveKey = await page
    .locator('.dbz-graph-node.is-active-hit')
    .getAttribute('data-node-key');
  await page.locator('[data-search-next]').click();
  await page.waitForTimeout(400);
  const secondActiveKey = await page
    .locator('.dbz-graph-node.is-active-hit')
    .getAttribute('data-node-key');
  check(
    '「下一个」切换到另一个命中并保持高亮',
    secondActiveKey !== firstActiveKey,
    `${firstActiveKey} -> ${secondActiveKey}`,
  );
  await page.locator('[data-search-clear]').click();
  await page.waitForTimeout(300);
  check(
    '清空搜索后不再有高亮',
    (await page.locator('.dbz-graph-node.is-hit').count()) === 0 &&
      (await page.locator('.dbz-graph-node').count()) === nodeCountBeforeSearch,
  );

  // 只有 broker / connect / source 有「+」，sink 没有下级
  const addCounts = await page
    .locator('.dbz-graph-node')
    .evaluateAll((elements) => {
      const counts = {};
      for (const element of elements) {
        const kind = element.getAttribute('data-node-type');
        counts[kind] = (counts[kind] ?? 0) + element.querySelectorAll('.dbz-graph-add-child').length;
      }
      return counts;
    });
  check(
    '「+」只出现在 Kafka 集群/Connect/上游连接器底部',
    addCounts.broker === 1 &&
      addCounts.connect === 1 &&
      addCounts.source === 1 &&
      addCounts.sink === 0,
    JSON.stringify(addCounts),
  );

  // 状态悬停详情：状态、最后检查时间、失败原因与任务数
  const source201 = page.locator('.dbz-graph-node[data-node-key="3:201"]');
  await source201.locator('.dbz-graph-status-tag').hover();
  await page.waitForSelector('.ant-tooltip:visible');
  const statusHint = await page.locator('.ant-tooltip:visible').innerText();
  check(
    '状态 hint 包含状态、最后检查时间、失败原因与实时任务数',
    statusHint.includes('状态') &&
      statusHint.includes('最后检查时间') &&
      statusHint.includes('失败原因') &&
      statusHint.includes('共 2 个，失败 1 个'),
    statusHint.replace(/\s+/g, ' ').slice(0, 160),
  );
  const beforeHintDrag = await source201.evaluate((element) => element.style.left);
  await source201.locator('.dbz-graph-status-tag').hover();
  await page.mouse.down();
  await page.mouse.move(300, 300, { steps: 8 });
  await page.mouse.up();
  await page.mouse.move(10, 10);
  const afterHintDrag = await source201.evaluate((element) => element.style.left);
  check('状态标签上按下拖动不会移动卡片', beforeHintDrag === afterHintDrag, `${beforeHintDrag} -> ${afterHintDrag}`);

  // 搜索会用定位平移视口；先「适应画布」把视图复位，避免节点落到缩放控件下面点不中
  await page.locator('.dbz-graph-controls button[title="适应画布"]').click();
  await page.waitForTimeout(300);

  // 「+」新增下级：父级固定且表单里根本不出现父级/Connect/集群输入控件
  await page.locator('.dbz-graph-node[data-node-key="1:401"] .dbz-graph-add-child').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const addConnectModal = visibleModal().last();
  await page.waitForTimeout(400);
  check(
    'broker 下加 Connect：表单没有 Kafka 集群输入控件',
    (await addConnectModal.locator('#kafkaBrokerId').count()) === 0 &&
      !(await addConnectModal.innerText()).includes('所属 Kafka 集群'),
  );
  await addConnectModal.locator('#name').fill('冒烟新增-Connect');
  await addConnectModal.locator('#url').fill('http://10.0.0.9:8083');
  await addConnectModal.locator('.ant-modal-footer .ant-btn-primary').click();
  await waitNoVisibleModal();
  const addConnectRequest = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/kafka-connect',
  );
  check(
    'broker 下新增 Connect 的请求体固定父级',
    addConnectRequest?.body?.kafkaBrokerId === '401' &&
      addConnectRequest?.body?.name === '冒烟新增-Connect',
    JSON.stringify(addConnectRequest?.body),
  );

  await page.locator('.dbz-graph-node[data-node-key="2:101"] .dbz-graph-add-child').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const addSourceModal = visibleModal().last();
  await page.waitForTimeout(400);
  check(
    'Connect 下加上游：表单没有 Connect 输入控件',
    (await addSourceModal.locator('#kafkaConnectId').count()) === 0 &&
      !(await addSourceModal.innerText()).includes('所属 Kafka Connect'),
  );
  await addSourceModal.locator('#name').fill('冒烟新增-上游');
  await addSourceModal.locator('#connectorName').fill('smoke-added-source');
  await addSourceModal.locator('[id="connector.class"]').fill('io.debezium.connector.mysql.MySqlConnector');
  await addSourceModal.locator('[id="topic.prefix"]').fill('dbzsmoke');
  await addSourceModal.locator('[id="database.hostname"]').fill('10.0.0.21');
  await addSourceModal.locator('[id="database.user"]').fill('dbz');
  await addSourceModal.locator('[id="database.dbname"]').fill('orders');
  await addSourceModal.locator('.ant-modal-footer .ant-btn-primary').click();
  await waitNoVisibleModal();
  const addSourceRequest = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/source-connector',
  );
  check(
    'Connect 下新增上游的请求体固定父级，不提交 Connect 选择',
    addSourceRequest?.body?.kafkaConnectId === '101' &&
      addSourceRequest?.body?.name === '冒烟新增-上游',
    JSON.stringify(addSourceRequest?.body),
  );

  await page.locator('.dbz-graph-node[data-node-key="3:201"] .dbz-graph-add-child').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const addSinkModal = visibleModal().last();
  await page.waitForTimeout(400);
  check(
    '上游下加下游：表单没有上游/Connect/集群输入控件',
    (await addSinkModal.locator('#sourceConnectorId').count()) === 0 &&
      (await addSinkModal.locator('#kafkaConnectId').count()) === 0 &&
      !(await addSinkModal.innerText()).includes('上游连接器'),
  );
  await addSinkModal.locator('#name').fill('冒烟新增-下游');
  await addSinkModal.locator('#connectorName').fill('smoke-added-sink');
  await addSinkModal.locator('[id="connector.class"]').fill('io.debezium.connector.jdbc.JdbcSinkConnector');
  await addSinkModal.locator('[id="topics"]').fill('dbz.orders');
  await addSinkModal.locator('[id="connection.url"]').fill('jdbc:postgresql://10.0.0.3:5432/dw');
  await addSinkModal.locator('[id="connection.username"]').fill('dbz');
  await addSinkModal.locator('.ant-modal-footer .ant-btn-primary').click();
  await waitNoVisibleModal();
  const addSinkRequest = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/sink-connector',
  );
  check(
    '上游下新增下游的请求体由上游推导 Connect',
    addSinkRequest?.body?.sourceConnectorId === '201' &&
      addSinkRequest?.body?.kafkaConnectId === '101' &&
      addSinkRequest?.body?.name === '冒烟新增-下游',
    JSON.stringify(addSinkRequest?.body),
  );

  // 属性检查器随选中节点切换：库内配置、统一状态、任务与异常都在这里
  await selectNode('1:401');
  await page.waitForSelector('.dbz-graph-inspector .dbz-inspector-panel');
  const brokerInspectorText = await page.locator('.dbz-graph-inspector').innerText();
  check(
    '选中 Kafka 集群后属性框展示库内配置与统一状态',
    brokerInspectorText.includes('订单消息集群') &&
      brokerInspectorText.includes('库内配置') &&
      brokerInspectorText.includes('状态') &&
      brokerInspectorText.includes('最后检查时间'),
    brokerInspectorText.slice(0, 120),
  );
  check('原来是抽屉的详情不再重复弹出', (await page.locator('.ant-drawer').count()) === 0);

  // 卡片上的编辑配置：请求体要带新名称
  await page.locator('.dbz-graph-node[data-node-key="1:401"] [data-node-action="edit"]').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const brokerModal = visibleModal().last();
  // 等 Modal 的 afterOpenChange 回填完成，否则刚 fill 的值会被 setFieldsValue 覆盖
  await page.waitForTimeout(500);
  await brokerModal.locator('#name').fill('订单消息集群-冒烟');
  await brokerModal.locator('.ant-modal-footer .ant-btn-primary').click();
  await waitNoVisibleModal();
  const editRequest = posted.findLast(
    (item) => item.method === 'PUT' && item.path === '/dbz/kafka-broker',
  );
  check(
    '卡片上的编辑配置调用修改接口',
    editRequest?.body?.id === '401' && editRequest?.body?.name === '订单消息集群-冒烟',
    JSON.stringify(editRequest?.body),
  );

  // 卡片上的集群检测：402 在另一个集群 tab，先切过去
  await page.locator('[data-cluster-tab="402"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 3, null, {
      timeout: 15000,
    })
    .catch(() => {});
  await page.locator('.dbz-graph-node[data-node-key="1:402"] [data-node-action="check"]').click();
  // 检测是异步请求，等提示真的出现再读，避免只看到上一次「修改成功」
  await page
    .waitForSelector('.ant-message-notice-content:has-text("集群检测通过")', { timeout: 5000 })
    .catch(() => {});
  const checkRequest = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/kafka-broker/check',
  );
  check(
    '卡片上的集群检测调用检测接口',
    checkRequest?.body?.id === '402',
    JSON.stringify(checkRequest?.body),
  );
  const brokerMessages = await page.locator('.ant-message-notice-content').allInnerTexts();
  check(
    '集群检测给出成功提示',
    brokerMessages.some((text) => text.includes('集群检测通过')),
    brokerMessages.join(' | '),
  );
  await page.locator('[data-cluster-tab="401"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 7, null, {
      timeout: 15000,
    })
    .catch(() => {});

  // C. Connect 健康检测：可达 / 版本 / worker
  await page.locator('.dbz-graph-node[data-node-key="2:101"] [data-node-action="check"]').click();
  await page.waitForTimeout(400);
  const healthMessage = await page
    .locator('.ant-message-notice-content:has-text("健康检测通过")')
    .last()
    .innerText();
  check(
    'Connect 健康检测结果包含版本与 worker',
    healthMessage.includes('v3.7.1') && healthMessage.includes('1 个 worker'),
    healthMessage,
  );

  // C. 连接器一致性检测：下游一致 → 成功；上游库内缺字段 → 失败并列出具体原因
  await page.locator('.dbz-graph-node[data-node-key="4:301"] [data-node-action="check"]').click();
  await page
    .waitForSelector('.ant-message-notice-content:has-text("一致性检测通过")', { timeout: 5000 })
    .catch(() => {});
  check(
    '下游一致性检测通过',
    (await page
      .locator('.ant-message-notice-content:has-text("一致性检测通过")')
      .count()) >= 1,
  );

  await page.locator('.dbz-graph-node[data-node-key="3:201"] [data-node-action="check"]').click();
  await page.waitForTimeout(600);
  const consistencyModal = visibleModal().last();
  await consistencyModal.waitFor({ state: 'visible' });
  const consistencyText = (await consistencyModal.innerText()).replace(/\s+/g, ' ');
  check(
    '上游一致性检测失败并列出具体差异',
    consistencyText.includes('一致性检测未通过') &&
      consistencyText.includes('database.hostname') &&
      consistencyText.includes('table.include.list'),
    consistencyText.slice(0, 240),
  );
  await consistencyModal.locator('.ant-modal-confirm-btns .ant-btn-primary').click();
  await consistencyModal.waitFor({ state: 'hidden' });

  // 卡片上的重启：确认后请求要打到 Connect 的 restart 接口
  await page.locator('.dbz-graph-node[data-node-key="3:201"] [data-node-action="restart"]').click();
  const restartConfirm = page.locator('.ant-modal-confirm');
  await restartConfirm.waitFor({ state: 'visible' });
  await restartConfirm.locator('.ant-btn-primary').click();
  await restartConfirm.waitFor({ state: 'hidden' });
  await page.waitForTimeout(400);
  const restartRequest = posted.find(
    (item) => item.method === 'POST' && item.path === '/dbz/kafka-connector/restart',
  );
  check(
    '卡片上的重启调用远端重启接口',
    restartRequest?.body?.connectorName === 'mysql-orders-source' &&
      restartRequest?.body?.kafkaConnectId === '101' &&
      restartRequest?.body?.includeTasks === true,
    JSON.stringify(restartRequest?.body),
  );

  // 选中连接器：配置 JSON、实时状态、任务与异常都在属性框里
  await selectNode('3:201');
  await page.waitForSelector('.dbz-graph-inspector .dbz-inspector-panel');
  const connectorInspector = page.locator('.dbz-graph-inspector');
  const connectorText = await connectorInspector.innerText();
  check(
    '属性框展示连接器的库内配置、单一状态、最后检查时间与任务异常',
    connectorText.includes('库内配置') &&
      connectorText.includes('状态') &&
      connectorText.includes('最后检查时间') &&
      connectorText.includes('失败原因') &&
      connectorText.includes('任务与异常') &&
      connectorText.includes('MySqlConnector'),
    connectorText.slice(0, 160),
  );
  check(
    '属性框任务表显示实时任务',
    (await connectorInspector.locator('tbody tr.ant-table-row').count()) === 2,
    `实际 ${await connectorInspector.locator('tbody tr.ant-table-row').count()}`,
  );
  await page.screenshot({ path: `${SHOTS}/01a-control-plane-inspector.png`, fullPage: false });

  // 切到 Connect：属性框内容跟着换
  await selectNode('2:101');
  await page.waitForSelector('.dbz-graph-inspector .dbz-inspector-panel');
  const connectInspectorText = await page.locator('.dbz-graph-inspector').innerText();
  check(
    '切换选中节点后属性框内容跟着变',
    connectInspectorText.includes('生产集群-华东') &&
      connectInspectorText.includes('http://10.0.0.11:8083') &&
      connectInspectorText.includes('健康探测') &&
      !connectInspectorText.includes('任务与异常'),
    connectInspectorText.slice(0, 160),
  );

  // 卡片上的启用/停用：单个 Switch，请求中 loading，失败回滚
  check(
    '每张卡片有且只有一个启用开关',
    (await page.locator('.dbz-graph-switch .ant-switch').count()) === 7,
    `实际 ${await page.locator('.dbz-graph-switch .ant-switch').count()}`,
  );
  const sink301 = page.locator('.dbz-graph-node[data-node-key="4:301"]');
  check(
    '启用开关随启停状态正确',
    (await sink301.locator('.dbz-graph-switch .ant-switch').getAttribute('aria-checked')) === 'true',
  );
  await sink301.locator('.dbz-graph-switch .ant-switch').click();
  await page.waitForTimeout(500);
  const toggleRequest = posted.findLast(
    (item) => item.method === 'PUT' && item.path === '/dbz/sink-connector',
  );
  check(
    '点击开关调用修改接口并置为停用',
    toggleRequest?.body?.id === '301' && toggleRequest?.body?.enabled === false,
    JSON.stringify(toggleRequest?.body),
  );
  // 开关是「请求 → 重新拉数据 → 回写卡片」，等卡片真的变成停用态再断言
  await page
    .waitForFunction(
      () =>
        document
          .querySelector('.dbz-graph-node[data-node-key="4:301"]')
          ?.classList.contains('is-disabled') === true,
      null,
      { timeout: 5000 },
    )
    .catch(() => {});
  const sinkDisabledClass = await sink301.evaluate((element) =>
    element.classList.contains('is-disabled'),
  );
  const sinkStatusText = (await sink301.locator('.dbz-graph-status-tag .ant-tag').innerText()).trim();
  check(
    '停用态整卡可辨但与异常态不同',
    sinkDisabledClass && sinkStatusText === '已停用',
    `is-disabled=${sinkDisabledClass} 状态=${sinkStatusText}`,
  );
  await sink301.locator('.dbz-graph-switch .ant-switch').click();
  await page.waitForTimeout(500);

  failNextSinkToggle = true;
  const sink303 = page.locator('.dbz-graph-node[data-node-key="4:303"]');
  await sink303.locator('.dbz-graph-switch .ant-switch').click();
  await page.waitForTimeout(500);
  check(
    '开关请求失败后回滚到服务端状态',
    (await sink303.locator('.dbz-graph-switch .ant-switch').getAttribute('aria-checked')) === 'true' &&
      !(await sink303.evaluate((element) => element.classList.contains('is-disabled'))),
  );
  check(
    '开关失败有提示',
    (await page.locator('.ant-message-error').count()) >= 1,
  );
  check(
    '失败态不使用停用态样式',
    (await sink303.locator('.dbz-graph-status-tag .ant-tag').innerText()).trim() === '失败',
  );

  // 卡片上的删除：先看影响，再输入当前登录用户口令；失败分「口令错误 / 频控 / 认证服务不可用」
  await page.locator('.dbz-graph-node[data-node-key="4:301"] [data-node-action="delete"]').click();
  const deleteModal = visibleModal().last();
  await deleteModal.waitFor({ state: 'visible' });
  const deleteText = (await deleteModal.innerText()).replace(/\s+/g, ' ');
  check(
    '删除提示说明受影响的归属关系',
    deleteText.includes('归属关系'),
    deleteText.slice(0, 160),
  );
  check(
    '删除提示不再列数据流与写入目标',
    !deleteText.includes('数据流') && !deleteText.includes('写入目标'),
    deleteText.slice(0, 160),
  );
  check(
    '删除必须输入当前登录用户口令',
    deleteText.includes('当前登录用户口令') &&
      (await deleteModal.locator('[data-delete-password]').count()) === 1,
  );
  await deleteModal.locator('.ant-modal-footer .ant-btn').first().click();
  await waitNoVisibleModal();
  check(
    '取消删除时不调用删除接口',
    !posted.some((item) => item.path === '/dbz/control-plane/node-delete'),
  );

  // 口令错误：弹窗保留并给出可区分的提示
  await page.locator('.dbz-graph-node[data-node-key="4:301"] [data-node-action="delete"]').click();
  const wrongPasswordModal = visibleModal().last();
  await wrongPasswordModal.waitFor({ state: 'visible' });
  await wrongPasswordModal.locator('[data-delete-password]').fill('wrong-password');
  failNextDelete = 'wrong';
  await wrongPasswordModal.locator('[data-delete-confirm]').click();
  await page.waitForTimeout(500);
  const wrongText = (await wrongPasswordModal.innerText()).replace(/\s+/g, ' ');
  check(
    '口令错误时提示口令错误且弹窗不关闭',
    (await wrongPasswordModal.isVisible()) && wrongText.includes('口令错误'),
    wrongText.slice(0, 200),
  );

  // 删除请求必须带登录令牌与真实用户ID，口令只以 64 位 SM3 摘要提交
  const deleteRequest = posted.find((item) => item.path === '/dbz/control-plane/node-delete');
  check(
    '删除请求带 Authorization 与真实用户 ID',
    deleteRequest?.headers.authorization === `Bearer ${seedSession.token}` &&
      deleteRequest?.headers['x-user-id'] === seedSession.userId,
    JSON.stringify({
      authorization: deleteRequest?.headers.authorization,
      userId: deleteRequest?.headers['x-user-id'],
    }),
  );
  check(
    '删除请求体只提交 64 位 SM3 摘要，不含明文口令',
    typeof deleteRequest?.body?.loginPswd === 'string' &&
      /^[0-9a-f]{64}$/.test(deleteRequest.body.loginPswd) &&
      !('password' in (deleteRequest?.body ?? {})),
    JSON.stringify(deleteRequest?.body),
  );

  // 频控：提示稍后重试，且说明默认窗口
  failNextDelete = 'rate';
  await wrongPasswordModal.locator('[data-delete-confirm]').click();
  await page.waitForTimeout(500);
  const rateText = (await wrongPasswordModal.innerText()).replace(/\s+/g, ' ');
  check(
    '频控时提示口令校验过于频繁并说明重试窗口',
    (await wrongPasswordModal.isVisible()) &&
      rateText.includes('口令校验过于频繁') &&
      rateText.includes('10 分钟'),
    rateText.slice(0, 220),
  );

  // 认证服务不可用：不能提示成口令错，也不允许删除
  failNextDelete = 'unavailable';
  await wrongPasswordModal.locator('[data-delete-confirm]').click();
  await page.waitForTimeout(500);
  const unavailableText = (await wrongPasswordModal.innerText()).replace(/\s+/g, ' ');
  check(
    '认证服务不可用时提示不可用且不说成口令错',
    (await wrongPasswordModal.isVisible()) &&
      unavailableText.includes('认证服务不可用') &&
      !unavailableText.includes('口令错误：'),
    unavailableText.slice(0, 200),
  );
  // 卡片 Tooltip 可能还挂在焦点上，先移开指针再点取消，避免盖住弹窗按钮
  await page.evaluate(() => {
    document.activeElement?.blur?.();
    document.querySelectorAll('.dbz-graph-node').forEach((node) => {
      node.dispatchEvent(new MouseEvent('mouseout', { bubbles: true, relatedTarget: document.body }));
    });
  });
  await page.mouse.move(24, 24);
  await page.waitForTimeout(200);
  await wrongPasswordModal.locator('.ant-modal-footer .ant-btn').first().click();
  await waitNoVisibleModal();

  // 点选归属连线 → 属性框说明只读；没有任何拖拽手柄，也不能删除
  const ownershipHit = page.locator(
    '[data-edge-id="own:connect:101:source:201"].dbz-graph-edge-hit',
  );
  // 卡片上可能挂着 antd Tooltip（点击后靠 focus 保持），会盖住连线中段。
  // 先失焦并给节点补一次 mouseout，让 Tooltip 收起
  await page.evaluate(() => {
    document.activeElement?.blur?.();
    document.querySelectorAll('.dbz-graph-node').forEach((node) => {
      node.dispatchEvent(new MouseEvent('mouseout', { bubbles: true, relatedTarget: document.body }));
    });
  });
  await page.mouse.move(24, 24);
  await page.waitForTimeout(500);
  // 用 pointerdown 选中连线：和真实点击走同一个 handler，且不受浮层遮挡影响
  await ownershipHit.dispatchEvent('pointerdown', { button: 0, bubbles: true });
  await page.waitForSelector('.dbz-graph-edge-line.is-selected', { timeout: 5000 });
  await page.waitForSelector('.dbz-graph-inspector .dbz-inspector-panel');
  const edgeInspectorText = await page.locator('.dbz-graph-inspector').innerText();
  check(
    '选中归属连线后属性框说明端点固定、折点可拖',
    edgeInspectorText.includes('归属连线') &&
      edgeInspectorText.includes('不能拖动') &&
      edgeInspectorText.includes('不能删除') &&
      edgeInspectorText.includes('折点'),
    edgeInspectorText.slice(0, 160),
  );
  check(
    '连线没有端点拖拽手柄或锁标',
    (await page.locator('.dbz-graph-edge-handle').count()) === 0 &&
      (await page.locator('.dbz-graph-edge-lock').count()) === 0,
  );
  check(
    '选中连线后才出现一个折点手柄',
    (await page.locator('.dbz-graph-bend-handle').count()) === 1,
  );
  await page.keyboard.press('Delete');
  await page.waitForTimeout(300);
  check(
    'Delete 快捷键不会删除连线，也不弹旧提示',
    (await page.locator('.ant-message-notice-content:has-text("连线不能删除")').count()) === 0 &&
      !posted.some((item) => item.method === 'DELETE'),
  );

  // D. 拖动折点：改走线并落库；端点仍不可拖
  const postedBeforeBend = posted.length;
  await page.evaluate(() => {
    document.activeElement?.blur?.();
    document.querySelectorAll('.dbz-graph-node').forEach((node) => {
      node.dispatchEvent(new MouseEvent('mouseout', { bubbles: true, relatedTarget: document.body }));
    });
  });
  await page.mouse.move(24, 24);
  await page.waitForTimeout(400);
  const bendBox = await page.locator('.dbz-graph-bend-handle').boundingBox();
  const bendCenter = {
    x: bendBox.x + bendBox.width / 2,
    y: bendBox.y + bendBox.height / 2,
  };
  const bendHitIsHandle = await page.evaluate((point) => {
    const hit = document.elementFromPoint(point.x, point.y);
    return Boolean(hit?.closest?.('.dbz-graph-bend-handle'));
  }, bendCenter);
  if (bendHitIsHandle) {
    await page.mouse.move(bendCenter.x, bendCenter.y);
    await page.mouse.down();
    await page.mouse.move(bendCenter.x, bendCenter.y + 44, { steps: 10 });
    await page.mouse.up();
  } else {
    // 浮层还挂着时退回合成 pointer 事件，走的是同一个拖折点 handler
    await page
      .locator('.dbz-graph-bend-handle')
      .dispatchEvent('pointerdown', { button: 0, clientY: bendCenter.y, bubbles: true });
    await page.evaluate((point) => {
      window.dispatchEvent(
        new PointerEvent('pointermove', { clientX: point.x, clientY: point.y + 44, bubbles: true }),
      );
      window.dispatchEvent(
        new PointerEvent('pointerup', { clientX: point.x, clientY: point.y + 44, bubbles: true }),
      );
    }, bendCenter);
  }
  await page.waitForTimeout(400);
  const bendPut = posted.findLast(
    (item) => item.method === 'PUT' && item.path === '/dbz/control-plane/edges',
  );
  check(
    '拖动折点保存走线',
    bendPut?.body?.edgeId === 'own:connect:101:source:201' &&
      typeof bendPut?.body?.midY === 'number',
    JSON.stringify(bendPut?.body),
  );
  check('拖动折点只产生折点写请求', posted.length - postedBeforeBend === 1, `${postedBeforeBend} -> ${posted.length}`);

  const postedBeforeEndpointDrag = posted.length;
  const ownershipStart = await ownershipHit.evaluate((element) => {
    const bbox = element.getBBox();
    const rect = element.getBoundingClientRect();
    const point = element.getPointAtLength(0);
    return {
      x: rect.left + (point.x - bbox.x) * (rect.width / bbox.width),
      y: rect.top + (point.y - bbox.y) * (rect.height / bbox.height),
    };
  });
  await page.mouse.move(ownershipStart.x, ownershipStart.y + 2);
  await page.mouse.down();
  await page.mouse.move(ownershipStart.x + 160, ownershipStart.y + 60, {
    steps: 10,
  });
  await page.mouse.up();
  await page.waitForTimeout(300);
  check(
    '拖动端点不产生写请求，也不改上下游',
    posted.length === postedBeforeEndpointDrag,
    `${postedBeforeEndpointDrag} -> ${posted.length}`,
  );

  // 拖动节点后自动保存布局；刷新后位置保持
  const brokerNode = page.locator('.dbz-graph-node[data-node-key="1:401"]');
  const brokerBox = await brokerNode.boundingBox();
  await page.mouse.move(brokerBox.x + brokerBox.width / 2, brokerBox.y + brokerBox.height / 2);
  await page.mouse.down();
  await page.mouse.move(
    brokerBox.x + brokerBox.width / 2 + 120,
    brokerBox.y + brokerBox.height / 2 + 80,
    { steps: 14 },
  );
  await page.mouse.up();
  await page.waitForTimeout(400);
  const layoutPut = posted.findLast(
    (item) =>
      item.method === 'PUT' &&
      item.path === '/dbz/control-plane/layout' &&
      item.body?.nodeType === 1 &&
      item.body?.nodeId === '401',
  );
  check(
    '拖动节点后自动保存布局',
    typeof layoutPut?.body?.x === 'number' && typeof layoutPut?.body?.y === 'number',
    JSON.stringify(layoutPut?.body),
  );

  await page.reload({ waitUntil: 'networkidle' });
  await page.waitForSelector('.dbz-graph-node[data-node-key="1:401"]');
  const persistedLeft = await page
    .locator('.dbz-graph-node[data-node-key="1:401"]')
    .evaluate((element) => parseFloat(element.style.left));
  check(
    '刷新后节点位置从库内布局恢复',
    Math.abs(persistedLeft - layoutPut.body.x) < 1,
    `节点 left=${persistedLeft}，保存值=${layoutPut.body.x}`,
  );

  // 刷新后折点走线仍按库内值
  const persistedPath = await page
    .locator('[data-edge-id="own:connect:101:source:201"].dbz-graph-edge-hit')
    .evaluate((element) => element.previousElementSibling?.getAttribute('d') ?? '');
  const persistedMidY = Number((persistedPath.match(/L[^,]+,([^ ]+)/) ?? [])[1]);
  check(
    '刷新后连线折点从库内走线恢复',
    Math.abs(persistedMidY - bendPut.body.midY) < 1,
    `走线 midY=${persistedMidY}，保存值=${bendPut.body.midY}`,
  );

  // B. 组件面板点击「Kafka 集群」：建根节点 → 打开表单 → 保存布局
  await page.locator('.dbz-component-item[data-component="broker"]').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const createModal = visibleModal().last();
  // 新增表单没有回填值可等，等 afterOpenChange 跑完（resetFields + 默认值）再填
  await page.waitForTimeout(500);
  await createModal.locator('#name').fill('冒烟新集群');
  await createModal.locator('#bootstrapServers').fill('10.0.9.9:9092');
  await createModal.locator('.ant-modal-footer .ant-btn-primary').click();
  await waitNoVisibleModal();
  await page.waitForSelector('.dbz-graph-node[data-node-key="1:499"]');
  const createdBrokerRequest = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/kafka-broker',
  );
  check(
    '组件面板点击 Kafka 集群调用创建接口',
    createdBrokerRequest?.body?.name === '冒烟新集群' &&
      createdBrokerRequest?.body?.bootstrapServers === '10.0.9.9:9092',
    JSON.stringify(createdBrokerRequest?.body),
  );
  const createLayout = posted.findLast(
    (item) =>
      item.method === 'PUT' &&
      item.path === '/dbz/control-plane/layout' &&
      item.body?.nodeType === 1 &&
      item.body?.nodeId === '499',
  );
  check(
    '新增节点后保存落位布局',
    typeof createLayout?.body?.x === 'number',
    JSON.stringify(createLayout?.body),
  );
  check(
    '新增节点出现在画布上',
    (await page.locator('.dbz-graph-node[data-node-key="1:499"]').count()) === 1,
  );
  check(
    '新增后顶栏节点数更新',
    (await page.locator('.dbz-canvas-topbar .ant-tag:has-text("4 个 Kafka 集群")').count()) === 1,
  );

  // 新建根节点的布局刷新后仍在：刷新后默认回到第一个集群，先切到新集群
  await page.reload({ waitUntil: 'networkidle' });
  await page.locator('[data-cluster-tab="499"]').click();
  await page.waitForSelector('.dbz-graph-node[data-node-key="1:499"]');
  const createdNodeLeft = await page
    .locator('.dbz-graph-node[data-node-key="1:499"]')
    .evaluate((element) => parseFloat(element.style.left));
  check(
    '新建根节点刷新后仍在且位置从布局恢复',
    Math.abs(createdNodeLeft - createLayout.body.x) < 1,
    `left=${createdNodeLeft}，保存值=${createLayout.body.x}`,
  );
  // 回到原集群，后面的用例继续按 401 的节点断言
  await page.locator('[data-cluster-tab="401"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 7, null, {
      timeout: 15000,
    })
    .catch(() => {});

  // 新增节点会写进 broker 夹具，后面的列表用例仍按原始 3 条断言，这里恢复夹具
  brokers.splice(
    brokers.findIndex((broker) => broker.id === '499'),
    1,
  );
  const createdLayoutIndex = layoutStore.findIndex((item) => item.nodeId === '499');
  if (createdLayoutIndex >= 0) {
    layoutStore.splice(createdLayoutIndex, 1);
  }

  // 自动刷新：默认 15 秒、可调整、持久化、手动刷新、后台暂停
  const refreshInput = page.locator('.dbz-canvas-refresh input');
  check('自动刷新默认 15 秒', (await refreshInput.inputValue()) === '15');
  await refreshInput.fill('30');
  await refreshInput.press('Tab');
  await page.waitForTimeout(200);
  const storedRefresh = await page.evaluate(() =>
    JSON.parse(window.localStorage.getItem('dbz.control-plane.refresh') ?? '{}'),
  );
  check(
    '调整后的秒数写入本地存储',
    storedRefresh.seconds === 30 && storedRefresh.enabled === true,
    JSON.stringify(storedRefresh),
  );
  await refreshInput.fill('1');
  await refreshInput.press('Tab');
  const refreshBeforeAuto = refreshCalls;
  await page.waitForTimeout(2600);
  check(
    '按调整后的秒数自动刷新并回写状态',
    refreshCalls - refreshBeforeAuto >= 2,
    `${refreshBeforeAuto} -> ${refreshCalls}`,
  );
  const refreshBeforeManual = refreshCalls;
  await page.locator('.dbz-canvas-topbar button:has-text("刷新")').click();
  await page.waitForTimeout(600);
  check(
    '手动刷新立即回写一次状态',
    refreshCalls - refreshBeforeManual >= 1,
    `${refreshBeforeManual} -> ${refreshCalls}`,
  );

  // 自动刷新不重置选中节点，也不打断正在编辑的表单
  await selectNode('1:401');
  await page.waitForSelector('.dbz-graph-inspector .dbz-inspector-panel');
  await page.locator('.dbz-graph-node[data-node-key="1:401"] [data-node-action="edit"]').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const autoRefreshModal = visibleModal().last();
  await page.waitForTimeout(400);
  await autoRefreshModal.locator('#name').fill('自动刷新中-未保存');
  await page.waitForTimeout(1600);
  const visibleModalCount = await page.locator('.ant-modal-wrap:visible').count();
  const visibleNameValues = await page
    .locator('.ant-modal-wrap:visible #name')
    .evaluateAll((elements) => elements.map((element) => element.value));
  check(
    '自动刷新不清空正在编辑的表单',
    visibleModalCount >= 1 && visibleNameValues.includes('自动刷新中-未保存'),
    `modal=${visibleModalCount} name=${visibleNameValues.join(',')}`,
  );
  check(
    '自动刷新不重置选中节点与属性框',
    (await page.locator('.dbz-graph-inspector').innerText()).includes('订单消息集群'),
  );
  await autoRefreshModal.locator('.ant-modal-close').click();
  const autoRefreshDiscardConfirm = page.locator('.ant-modal-confirm');
  if (await autoRefreshDiscardConfirm.count()) {
    await autoRefreshDiscardConfirm.waitFor({ state: 'visible' });
    await autoRefreshDiscardConfirm.locator('.ant-btn-primary').click();
  }
  await waitNoVisibleModal();

  // 页面不可见时暂停轮询，回来再恢复
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => 'hidden',
    });
    Object.defineProperty(document, 'hidden', { configurable: true, get: () => true });
    document.dispatchEvent(new Event('visibilitychange'));
  });
  const refreshBeforeHidden = refreshCalls;
  await page.waitForTimeout(2200);
  check(
    '页面不可见时暂停自动刷新',
    refreshCalls === refreshBeforeHidden,
    `${refreshBeforeHidden} -> ${refreshCalls}`,
  );
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => 'visible',
    });
    Object.defineProperty(document, 'hidden', { configurable: true, get: () => false });
    document.dispatchEvent(new Event('visibilitychange'));
  });
  await page.waitForTimeout(1500);
  check(
    '回到前台后恢复自动刷新',
    refreshCalls > refreshBeforeHidden,
    `${refreshBeforeHidden} -> ${refreshCalls}`,
  );
  await refreshInput.fill('15');
  await refreshInput.press('Tab');
  await page.waitForTimeout(200);
  const restoredRefresh = await page.evaluate(() =>
    JSON.parse(window.localStorage.getItem('dbz.control-plane.refresh') ?? '{}'),
  );
  check(
    '恢复 15 秒并持久化',
    restoredRefresh.seconds === 15 && (await refreshInput.inputValue()) === '15',
    JSON.stringify(restoredRefresh),
  );

  // 重置位点：下游提示按实际 auto.offset.reset，上游提示全量快照
  const sink301Reset = page.locator(
    '.dbz-graph-node[data-node-key="4:301"] [data-node-action="reset-offsets"]',
  );
  check(
    '下游只有一个「重置位点」操作',
    (await sink301Reset.count()) === 1 &&
      (await page.locator('.dbz-graph-node[data-node-key="4:301"] [data-node-action="replay"]').count()) ===
        0,
  );
  await sink301Reset.click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  let resetModal = visibleModal().last();
  const sinkResetText = (await resetModal.innerText()).replace(/\s+/g, ' ');
  check(
    '下游重置提示与实际 auto.offset.reset=earliest 一致',
    sinkResetText.includes('auto.offset.reset=earliest') &&
      sinkResetText.includes('重复写入') &&
      sinkResetText.includes('死信'),
    sinkResetText.slice(0, 180),
  );
  check('下游重置提示不出现「重新同步」', !sinkResetText.includes('重新同步'));
  const resumeCheckbox = resetModal.locator('.ant-checkbox-wrapper:has-text("清空后立即恢复运行") input');
  check('「清空后立即恢复运行」默认勾选', await resumeCheckbox.isChecked());
  const sinkConfirmCheckbox = resetModal.locator(
    '.ant-checkbox-wrapper:has-text("重复写入") input',
  );
  check('下游额外确认默认不勾选', !(await sinkConfirmCheckbox.isChecked()));
  check(
    '下游未勾额外确认时确认按钮禁用',
    await resetModal.locator('.ant-modal-footer .ant-btn-primary').isDisabled(),
  );
  await sinkConfirmCheckbox.check();
  check(
    '勾选额外确认后可执行',
    !(await resetModal.locator('.ant-modal-footer .ant-btn-primary').isDisabled()),
  );
  await resetModal.locator('.ant-modal-footer .ant-btn-primary').click();
  await page.waitForTimeout(500);
  resetModal = visibleModal().last();
  const resetRequest = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/control-plane/connector/reset-offsets',
  );
  check(
    '重置位点请求带连接器类型、id 与确认项',
    resetRequest?.body?.connectorType === 'sink' &&
      resetRequest?.body?.connectorId === '301' &&
      resetRequest?.body?.resume === true &&
      resetRequest?.body?.confirmReplay === true,
    JSON.stringify(resetRequest?.body),
  );
  const stepLabels = (await resetModal.locator('.dbz-reset-steps .ant-tag').allInnerTexts()).map(
    (item) => item.trim(),
  );
  check(
    '执行顺序为停 → 清 → 恢复',
    stepLabels.join('>') === '停止连接器>清空位点>恢复运行',
    stepLabels.join('>'),
  );
  await resetModal.locator('.ant-modal-footer .ant-btn').first().click();
  await waitNoVisibleModal();

  await page.locator('.dbz-graph-node[data-node-key="3:201"] [data-node-action="reset-offsets"]').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  let sourceResetModal = visibleModal().last();
  const sourceResetText = (await sourceResetModal.innerText()).replace(/\s+/g, ' ');
  check(
    '上游重置提示写明全量快照与重灌下游',
    sourceResetText.includes('snapshot.mode=when_needed') &&
      sourceResetText.includes('全量快照') &&
      sourceResetText.includes('下游会被重灌'),
    sourceResetText.slice(0, 180),
  );
  check(
    '上游「清空后立即恢复运行」默认勾选',
    await sourceResetModal
      .locator('.ant-checkbox-wrapper:has-text("清空后立即恢复运行") input')
      .isChecked(),
  );
  failNextReset = true;
  await sourceResetModal.locator('.ant-modal-footer .ant-btn-primary').click();
  await page.waitForTimeout(500);
  sourceResetModal = visibleModal().last();
  const failedResetText = (await sourceResetModal.innerText()).replace(/\s+/g, ' ');
  check(
    '重置失败时说明停在哪一步与当前状态',
    failedResetText.includes('清空位点') &&
      failedResetText.includes('第「清空位点」步失败') &&
      failedResetText.includes('当前连接器状态：已停止'),
    failedResetText.slice(0, 200),
  );
  await sourceResetModal.locator('.ant-modal-footer .ant-btn').first().click();
  await waitNoVisibleModal();

  // F. 上游「重新同步」：只在源连接器出现，选表后写增量信号
  const sourceResync = page.locator(
    '.dbz-graph-node[data-node-key="3:201"] [data-node-action="resync"]',
  );
  check('上游连接器有「重新同步」', (await sourceResync.count()) === 1);
  check(
    '下游连接器没有「重新同步」',
    (await page.locator('.dbz-graph-node--sink [data-node-action="resync"]').count()) === 0,
  );
  await sourceResync.click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const resyncModal = visibleModal().last();
  await page.waitForTimeout(600);
  const resyncText = (await resyncModal.innerText()).replace(/\s+/g, ' ');
  check(
    '重新同步弹窗说明增量快照、只重读选中表、不重置位点',
    resyncText.includes('重新同步') &&
      resyncText.includes('增量快照') &&
      resyncText.includes('只重读选中的表') &&
      resyncText.includes('不会重置位点') &&
      resyncText.includes('Kafka Connect 日志'),
    resyncText.slice(0, 260),
  );
  check(
    '重新同步默认全选该连接器管理的表',
    resyncText.includes('共 2 项，已选 2 项'),
    resyncText.slice(0, 200),
  );
  check(
    '重新同步不出现信号通道切换',
    (await resyncModal.locator('text=信号通道').count()) === 0,
  );
  await resyncModal.locator('.ant-modal-footer .ant-btn-primary').click();
  await page.waitForTimeout(600);
  const resyncRequest = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/source-connector/snapshot',
  );
  check(
    '重新同步请求为源库通道 + 增量快照 + 选中表',
    resyncRequest?.body?.id === '201' &&
      resyncRequest?.body?.snapshotType === 'incremental' &&
      resyncRequest?.body?.channel === 'source' &&
      Array.isArray(resyncRequest?.body?.dataCollections) &&
      resyncRequest?.body?.dataCollections.length === 2,
    JSON.stringify(resyncRequest?.body),
  );
  check(
    '重新同步成功后提示信号已写入、后续以 Connect 日志为准',
    (await page
      .locator('.ant-message-notice-content:has-text("信号已写入")')
      .count()) >= 1,
  );
  await waitNoVisibleModal();

  // G. Connect 导入导出：默认含凭据、脱敏选项、同名冲突三种处理与结果分类
  const sourcesBeforeTransfer = sources.map((item) => ({ ...item }));
  const sinksBeforeTransfer = sinks.map((item) => ({ ...item }));
  const connectNode101 = page.locator('.dbz-graph-node[data-node-key="2:101"]');
  check(
    'Connect 卡片有导出与导入入口',
    (await connectNode101.locator('[data-node-action="export-connectors"]').count()) === 1 &&
      (await connectNode101.locator('[data-node-action="import-connectors"]').count()) === 1,
  );

  // 默认含凭据导出
  await connectNode101.locator('[data-node-action="export-connectors"]').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const exportModal = visibleModal().last();
  check(
    '导出默认勾选含凭据',
    await exportModal
      .locator('.ant-radio-wrapper:has-text("含凭据（默认）") input')
      .isChecked(),
  );
  const [exportDownload] = await Promise.all([
    page.waitForEvent('download'),
    exportModal.locator('.ant-modal-footer .ant-btn-primary').click(),
  ]);
  const exportPath = join(SHOTS, 'connect-export.json');
  await exportDownload.saveAs(exportPath);
  const exportedFile = JSON.parse(readFileSync(exportPath, 'utf8'));
  const exportedSource = exportedFile.connectors.find((item) => item.direction === 'source');
  const exportedSink = exportedFile.connectors.find(
    (item) => item.connectorName === 'jdbc-orders-sink',
  );
  check(
    '导出文件名含 Connect 名称与时间戳',
    /^dbz-connect-.*-\d{8}-\d{6}\.json$/.test(await exportDownload.suggestedFilename()),
    await exportDownload.suggestedFilename(),
  );
  check(
    '默认导出包含 5 个连接器与完整凭据，并标注敏感字段',
    exportedFile.credentialMode === 'included' &&
      exportedFile.connectors.length === 5 &&
      exportedSource.connectorConfig.includes('dbz-secret') &&
      exportedSource.sensitiveConfigKeys.includes('database.password'),
    JSON.stringify(exportedFile.connectors.slice(0, 1)),
  );
  check(
    '导出下游带所属上游名称与 id',
    exportedSink?.upstream?.connectorName === 'mysql-orders-source' &&
      exportedSink?.upstream?.id === '201',
    JSON.stringify(exportedSink?.upstream),
  );
  await waitNoVisibleModal();

  // 脱敏导出：凭据字段替换成掩码
  await connectNode101.locator('[data-node-action="export-connectors"]').click();
  await page.waitForSelector('.ant-modal-wrap:visible');
  const redactedModal = visibleModal().last();
  await redactedModal.locator('.ant-radio-wrapper:has-text("脱敏") input').check();
  const [redactedDownload] = await Promise.all([
    page.waitForEvent('download'),
    redactedModal.locator('.ant-modal-footer .ant-btn-primary').click(),
  ]);
  const redactedPath = join(SHOTS, 'connect-export-redacted.json');
  await redactedDownload.saveAs(redactedPath);
  const redactedFile = JSON.parse(readFileSync(redactedPath, 'utf8'));
  const redactedSource = redactedFile.connectors.find((item) => item.direction === 'source');
  check(
    '脱敏导出把凭据字段替换为掩码',
    redactedFile.credentialMode === 'redacted' &&
      !redactedSource.connectorConfig.includes('dbz-secret') &&
      redactedSource.connectorConfig.includes('******') &&
      redactedSource.sensitiveConfigKeys.includes('database.password'),
    redactedSource.connectorConfig,
  );
  await waitNoVisibleModal();

  // 导入同一份文件到原 Connect：预览冲突，批量跳过 + 单条覆盖 + 单条保留
  const [importChooser] = await Promise.all([
    page.waitForEvent('filechooser'),
    connectNode101.locator('[data-node-action="import-connectors"]').click(),
  ]);
  await importChooser.setFiles(exportPath);
  await page.waitForSelector('.ant-modal-wrap:visible');
  const importModal = visibleModal().last();
  await page
    .waitForFunction(() => document.querySelectorAll('[data-import-row]').length === 5, null, {
      timeout: 15000,
    })
    .catch(() => {});
  const importPreviewText = (await importModal.innerText()).replace(/\s+/g, ' ');
  check(
    '导入预览列出 5 条连接器并标出同名冲突',
    (await page.locator('[data-import-row]').count()) === 5 &&
      importPreviewText.includes('同名冲突') &&
      importPreviewText.includes('已有纳管记录'),
    importPreviewText.slice(0, 240),
  );
  check(
    '导入提供全部覆盖/全部跳过/全部保留',
    (await importModal.locator('[data-import-bulk-overwrite]').count()) === 1 &&
      (await importModal.locator('[data-import-bulk-skip]').count()) === 1 &&
      (await importModal.locator('[data-import-bulk-keep]').count()) === 1,
  );
  await importModal.locator('[data-import-bulk-skip]').click();
  const sink301Row = page.locator('[data-import-row="sink:jdbc-orders-sink"]');
  await sink301Row.locator('.ant-select').click();
  await page
    .locator('.ant-select-dropdown:visible .ant-select-item-option')
    .filter({ hasText: '覆盖已有纳管记录' })
    .last()
    .click();
  check(
    '单条可切换为覆盖已有纳管记录',
    (await sink301Row.innerText()).includes('覆盖已有纳管记录'),
  );
  await page.waitForTimeout(300);
  const sink302Row = page.locator('[data-import-row="sink:tmp-disabled-sink"]');
  await sink302Row.locator('.ant-select').click();
  await page
    .locator('.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option')
    .filter({ hasText: '两者都保留' })
    .first()
    .click();
  check(
    '单条可切换为两者都保留',
    (await sink302Row.innerText()).includes('两者都保留'),
  );
  const postedBeforeImport = posted.length;
  await importModal.locator('[data-import-execute]').click();
  await page.waitForSelector('[data-import-result]', { timeout: 15000 });
  const importResultText = (await page.locator('[data-import-result]').innerText()).replace(
    /\s+/g,
    ' ',
  );
  check(
    '结果汇总区分覆盖并纳管与覆盖已有纳管记录',
    importResultText.includes('覆盖并纳管 0') &&
      importResultText.includes('覆盖已有纳管记录 1') &&
      importResultText.includes('跳过 3') &&
      importResultText.includes('改名保留 1') &&
      importResultText.includes('失败 0'),
    importResultText.slice(0, 240),
  );
  const importWrites = posted.slice(postedBeforeImport);
  const overwriteSinkRequest = importWrites.find(
    (item) =>
      item.method === 'PUT' &&
      item.path === '/dbz/sink-connector' &&
      item.body?.id === '301',
  );
  const keptSinkRequest = importWrites.find(
    (item) =>
      item.method === 'POST' &&
      item.path === '/dbz/sink-connector' &&
      item.body?.connectorName === 'tmp-disabled-sink-imported-1',
  );
  check(
    '覆盖已有纳管记录走修改接口且不新建',
    Boolean(overwriteSinkRequest) &&
      overwriteSinkRequest.body.connectorName === 'jdbc-orders-sink' &&
      overwriteSinkRequest.body.sourceConnectorId === '201',
    JSON.stringify(overwriteSinkRequest?.body),
  );
  check(
    '两者都保留用 -imported-1 名称新增并挂到原上游',
    Boolean(keptSinkRequest) && keptSinkRequest.body.sourceConnectorId === '201',
    JSON.stringify(keptSinkRequest?.body),
  );
  check(
    '跳过的连接器没有产生写请求',
    !importWrites.some(
      (item) => item.method === 'PUT' && item.path === '/dbz/source-connector',
    ),
  );
  await importModal.locator('.ant-modal-footer .ant-btn').click();
  await waitNoVisibleModal();

  // 未纳管同名连接器：两者都保留要改名并明确提示原连接器仍未纳管
  await page.locator('[data-cluster-tab="402"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 3, null, {
      timeout: 15000,
    })
    .catch(() => {});
  const manualImportPath = join(SHOTS, 'manual-import.json');
  writeFileSync(
    manualImportPath,
    JSON.stringify({
      format: 'dbz-connect-transfer',
      version: 1,
      exportedAt: '2026-11-07T00:00:00.000Z',
      credentialMode: 'included',
      connect: { id: '102', name: '灰度集群', url: 'http://10.0.0.12:8083', brokerName: '风控消息集群' },
      connectors: [
        {
          direction: 'source',
          name: '手工连接器副本',
          connectorName: 'manual-test-connector',
          connectorConfig:
            '{"connector.class":"io.debezium.connector.postgresql.PostgresConnector"}',
          remark: '',
          enabled: true,
          sensitiveConfigKeys: [],
        },
      ],
    }),
  );
  const [manualChooser] = await Promise.all([
    page.waitForEvent('filechooser'),
    page
      .locator(
        '.dbz-graph-node[data-node-key="2:102"] [data-node-action="import-connectors"]',
      )
      .click(),
  ]);
  await manualChooser.setFiles(manualImportPath);
  await page.waitForSelector('.ant-modal-wrap:visible');
  const manualModal = visibleModal().last();
  await page
    .waitForFunction(() => document.querySelectorAll('[data-import-row]').length === 1, null, {
      timeout: 15000,
    })
    .catch(() => {});
  const manualPreviewText = (await manualModal.innerText()).replace(/\s+/g, ' ');
  check(
    '集群上未纳管的同名连接器也判定为冲突',
    manualPreviewText.includes('同名冲突') && manualPreviewText.includes('集群上未纳管'),
    manualPreviewText.slice(0, 220),
  );
  await manualModal.locator('[data-import-bulk-keep]').click();
  const postedBeforeManual = posted.length;
  await manualModal.locator('[data-import-execute]').click();
  await page.waitForSelector('[data-import-result]', { timeout: 15000 });
  const manualResultText = (await page.locator('[data-import-result]').innerText()).replace(
    /\s+/g,
    ' ',
  );
  check(
    '未纳管同名保留后提示另有一条未纳管连接器',
    manualResultText.includes('改名保留 1') &&
      manualResultText.includes('manual-test-connector-imported-1') &&
      manualResultText.includes('未纳管的同名连接器'),
    manualResultText.slice(0, 240),
  );
  const manualCreate = posted
    .slice(postedBeforeManual)
    .find((item) => item.method === 'POST' && item.path === '/dbz/source-connector');
  check(
    '未纳管同名两者都保留按 -imported-1 新建',
    manualCreate?.body?.connectorName === 'manual-test-connector-imported-1',
    JSON.stringify(manualCreate?.body),
  );
  await manualModal.locator('.ant-modal-footer .ant-btn').click();
  await waitNoVisibleModal();
  await page.locator('[data-cluster-tab="401"]').click();
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 7, null, {
      timeout: 15000,
    })
    .catch(() => {});

  // 导入会写进夹具：恢复原始记录，后面的死信与列表用例继续按原数据断言
  sources.splice(0, sources.length, ...sourcesBeforeTransfer);
  sinks.splice(0, sinks.length, ...sinksBeforeTransfer);

  // 单集群时不显示 tab 栏；恢复多集群后 tab 栏重新出现
  singleClusterOnly = true;
  await page.goto(`${base}/control-plane`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.dbz-graph-node[data-node-key="1:401"]');
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 7, null, {
      timeout: 15000,
    })
    .catch(() => {});
  check(
    '单集群时不显示 tab 栏',
    (await page.locator('[data-cluster-tabs]').count()) === 0 &&
      (await page.locator('.dbz-graph-node').count()) === 7,
    `节点 ${await page.locator('.dbz-graph-node').count()}`,
  );
  singleClusterOnly = false;
  await page.goto(`${base}/control-plane`, { waitUntil: 'networkidle' });
  await page
    .waitForFunction(() => document.querySelectorAll('.dbz-graph-node').length === 7, null, {
      timeout: 15000,
    })
    .catch(() => {});
  check(
    '恢复多集群后 tab 栏重新显示',
    (await page.locator('[data-cluster-tab]').count()) === 3,
    `tab 数 ${await page.locator('[data-cluster-tab]').count()}`,
  );

  const topologyOverflow = await page.evaluate(
    () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
  );
  check('控制面页面无横向溢出', topologyOverflow <= 1, `溢出 ${topologyOverflow}px`);
  await page.screenshot({ path: `${SHOTS}/01a-control-plane.png`, fullPage: true });

  // 死信标签：不触发拖动，点击带连接器筛选进死信页
  const sink301Node = page.locator('.dbz-graph-node[data-node-key="4:301"]');
  const deadTag = sink301Node.locator('.dbz-graph-dead-tag');
  const sinkLeftBeforeDeadDrag = await sink301Node.evaluate((element) => element.style.left);
  const deadTagBox = await deadTag.boundingBox();
  await page.mouse.move(deadTagBox.x + deadTagBox.width / 2, deadTagBox.y + deadTagBox.height / 2);
  await page.mouse.down();
  await page.mouse.move(deadTagBox.x + 160, deadTagBox.y + 80, { steps: 10 });
  await page.mouse.up();
  await page.waitForTimeout(300);
  check(
    '死信标签上按下拖动不会移动卡片',
    (await sink301Node.evaluate((element) => element.style.left)) === sinkLeftBeforeDeadDrag,
  );
  check('拖动死信标签不会误跳转', page.url().includes('/control-plane'));

  await deadTag.click();
  await page.waitForFunction(() => window.location.href.includes('/dead-letter?'));
  check(
    '卡片「查看死信」跳转带连接器类型与 id 筛选',
    page.url().includes('connectorType=2') && page.url().includes('connectorId=301'),
    page.url(),
  );
  await page.waitForSelector('.ant-table-tbody tr.ant-table-row');
  check(
    '落地即按该连接器筛选',
    (await page.locator('.ant-table-tbody tr.ant-table-row').count()) === 3,
    `实际 ${await page.locator('.ant-table-tbody tr.ant-table-row').count()}`,
  );

  // 临时状态夹具用完移除，后续列表/死信用例仍按原始记录
  for (const id of ['302', '303', '304']) {
    const index = sinks.findIndex((sink) => sink.id === id);
    if (index >= 0) {
      sinks.splice(index, 1);
    }
  }

  /* 1c. 死信：深链带连接器筛选、报文详情、重投与废弃 */
  await page.goto(`${base}/dead-letter?connectorType=2&connectorId=301`, {
    waitUntil: 'networkidle',
  });
  await page.waitForSelector('.ant-table-tbody tr.ant-table-row');
  check(
    '死信列表按连接器深链筛选',
    (await page.locator('.ant-table-tbody tr.ant-table-row').count()) === 3,
    `实际 ${await page.locator('.ant-table-tbody tr.ant-table-row').count()}`,
  );
  await page
    .waitForSelector('.ant-select-selection-item:has-text("订单同步到数仓")', { timeout: 5000 })
    .catch(() => {});
  check(
    '死信筛选器回填连接器名称',
    (await page.locator('.ant-select-selection-item:has-text("订单同步到数仓")').count()) >= 1,
  );
  check(
    '死信状态与失败分类标签渲染',
    (await page.locator('.ant-tag:has-text("待处理")').count()) >= 1 &&
      (await page.locator('.ant-tag:has-text("已重投")').count()) >= 1 &&
      (await page.locator('.ant-tag:has-text("确定性失败")').count()) >= 1 &&
      (await page.locator('.ant-tag:has-text("传输类失败")').count()) >= 1 &&
      (await page.locator('.ant-tag:has-text("字段不合规")').count()) >= 1,
  );

  // H. 死信只针对下游连接器：查询不再出现上游类型，界面也没有上游类型选项
  check(
    '死信查询固定为下游连接器',
    getRequests
      .filter((item) => item.path === '/dbz/dead-letter/page')
      .every((item) => !item.query.includes('connectorType=1')),
  );
  check(
    '死信界面没有上游类型选项',
    !(await page.locator('.dbz-toolbar').innerText()).includes('上游') &&
      (await page.locator('.ant-select-dropdown .ant-select-item-option:has-text("上游连接器")').count()) === 0,
  );
  const deadLetterHeader = await page.locator('.ant-table-thead').innerText();
  check(
    '死信列表展示 topic、partition、offset',
    deadLetterHeader.includes('Topic') &&
      deadLetterHeader.includes('分区') &&
      deadLetterHeader.includes('Offset'),
    deadLetterHeader.replace(/\s+/g, ' '),
  );
  const deadLetterBody = await page.locator('.ant-table-tbody').innerText();
  check(
    '死信行展示原始消息的分区与 offset',
    deadLetterBody.includes('1001') &&
      deadLetterBody.includes('1002') &&
      deadLetterBody.includes('1003') &&
      deadLetterBody.includes('dbz.orders'),
  );

  // 失败分类筛选：请求带上 failureCategory
  const categorySelect = page
    .locator('.dbz-toolbar .ant-select')
    .filter({ hasText: '失败分类' })
    .first();
  const categoryResponse = page.waitForResponse(
    (response) =>
      response.url().includes('/dbz/dead-letter/page') &&
      response.url().includes('failureCategory=1'),
  );
  await categorySelect.click();
  await page
    .locator('.ant-select-dropdown:visible .ant-select-item-option:has-text("确定性失败")')
    .click();
  await categoryResponse.catch(() => {});
  const categoryRequest = getRequests
    .filter((item) => item.path === '/dbz/dead-letter/page')
    .at(-1);
  check(
    '按失败分类筛选',
    categoryRequest?.query.includes('failureCategory=1') &&
      categoryRequest?.query.includes('connectorType=2'),
    categoryRequest?.query,
  );

  // 失败原因关键字筛选
  const reasonResponse = page.waitForResponse(
    (response) =>
      response.url().includes('/dbz/dead-letter/page') &&
      response.url().includes('reason='),
  );
  await page.locator('.dbz-toolbar input[placeholder="失败原因关键字"]').fill('下游连接失败');
  await page.locator('.dbz-toolbar input[placeholder="失败原因关键字"]').press('Enter');
  await reasonResponse.catch(() => {});
  const reasonRequest = getRequests
    .filter((item) => item.path === '/dbz/dead-letter/page')
    .at(-1);
  check(
    '按失败原因关键字筛选',
    reasonRequest?.query.includes('reason=') &&
      decodeURIComponent(reasonRequest.query).includes('reason=下游连接失败'),
    reasonRequest?.query,
  );

  // 导出 CSV：当前筛选参数、完整表头与数据行
  const downloadPromise = page.waitForEvent('download');
  await page.locator('button:has-text("导出 CSV")').click();
  const download = await downloadPromise;
  const csv = readFileSync(await download.path(), 'utf8');
  check(
    '导出 CSV 文件名与表头完整',
    download.suggestedFilename().endsWith('.csv') &&
      csv.includes('死信ID') &&
      csv.includes('Topic') &&
      csv.includes('Partition') &&
      csv.includes('Offset') &&
      csv.includes('处理说明'),
    download.suggestedFilename(),
  );
  check(
    '导出 CSV 按当前筛选范围请求',
    getRequests
      .filter((item) => item.path === '/dbz/dead-letter/page')
      .at(-1)
      ?.query.includes('failureCategory=1'),
  );

  const listText = await page.locator('.ant-table-tbody').innerText();
  check(
    '死信列表显示给人看的失败原因',
    listText.includes('字段校验不通过，请核对并修改报文后重投') &&
      listText.includes('下游连接失败，重试多次后仍失败'),
    listText.slice(0, 120),
  );

  /* 901：确定性字段不合规 —— 改报文、保存待投、改后重投成功 */
  const row901 = page.locator('.ant-table-tbody tr.ant-table-row').nth(0);
  await row901.locator('button:has-text("详情")').click();
  await page.waitForSelector('.ant-drawer-title:has-text("死信详情")');
  const deadLetterDetail = await page.locator('.ant-drawer-body').innerText();
  check(
    '死信详情把失败原因与失败详情分开呈现，且失败详情在失败原因上方',
    deadLetterDetail.indexOf('失败详情') < deadLetterDetail.indexOf('失败原因') &&
      deadLetterDetail.includes('字段校验不通过，请核对并修改报文后重投') &&
      deadLetterDetail.includes('SO-20261107-1') &&
      deadLetterDetail.includes('99.5'),
    deadLetterDetail.slice(0, 200),
  );
  check(
    '失败详情以 hint 形式展示，可点开看全文、可复制',
    (await page
      .locator('.ant-drawer-body :text("查看失败详情全文（可复制）")')
      .count()) === 1 &&
      deadLetterDetail.includes('分区 / Offset') &&
      deadLetterDetail.includes('0 / 1001'),
  );
  await page.locator('.ant-drawer-body :text("查看失败详情全文（可复制）")').click();
  await page.waitForSelector('.ant-popover:visible');
  const failurePopover = page.locator('.ant-popover:visible').last();
  const popoverText = await failurePopover.innerText();
  check(
    '失败详情弹层可看全文且带复制按钮',
    popoverText.includes('HTTP 200 result=-2') &&
      popoverText.includes('FieldValidator.validate') &&
      (await failurePopover.locator('button:has-text("复制")').count()) === 1,
    popoverText.slice(0, 160),
  );
  // 再次点同一链接关闭弹层：Escape 会把整个 Drawer 一起关掉，后面就没法继续操作了
  await page.locator('.ant-drawer-body :text("查看失败详情全文（可复制）")').click();
  await page.locator('.ant-popover:visible').last().waitFor({ state: 'hidden' });
  check(
    '失败详情查看后仍可继续操作',
    (await page.locator('.ant-drawer-body button:has-text("保存待投")').count()) === 1,
  );
  check(
    '可改报文的死信展示编辑与并排原始报文',
    (await page.locator('.ant-drawer-body textarea.dbz-json-editor').count()) === 1 &&
      deadLetterDetail.includes('原始报文（永不覆盖）') &&
      deadLetterDetail.includes('已保存的待投报文'),
  );

  // 提交前 JSON 校验：非法 JSON 给出可读错误，且不发保存请求
  await page.locator('.ant-drawer-body textarea.dbz-json-editor').fill('{');
  await page.locator('.ant-drawer-body button:has-text("保存待投")').click();
  await page.waitForTimeout(300);
  check(
    '非法 JSON 给出可读错误且不提交',
    (await page.locator('.ant-drawer-body .ant-alert-error:has-text("不是合法 JSON")').count()) === 1 &&
      posted.filter((item) => item.path === '/dbz/dead-letter/edit-payload').length === 0,
  );

  // 改成合法报文并保存待投：原始报文保留，审计字段回显
  const editedPayload = '{"id":1001,"order_no":"SO-20261107-1","amount":88,"shipping_address":"上海市"}';
  await page.locator('.ant-drawer-body textarea.dbz-json-editor').fill(editedPayload);
  await page.locator('.ant-drawer-body button:has-text("保存待投")').click();
  await page.waitForTimeout(500);
  const savePendingRequest = posted.find(
    (item) => item.method === 'POST' && item.path === '/dbz/dead-letter/edit-payload',
  );
  check(
    '保存待投报文调用接口并携带修改内容',
    savePendingRequest?.body?.id === '901' && savePendingRequest?.body?.payload?.includes('"amount":88'),
    JSON.stringify(savePendingRequest?.body ?? null),
  );
  check(
    '待投报文保存成功并回显编辑人/时间',
    (await page.locator('.ant-message-notice-content:has-text("待投报文已保存")').count()) >= 1,
  );
  const afterSaveDetail = await page.locator('.ant-drawer-body').innerText();
  check(
    '保存待投后原始报文仍在且待投可查',
    afterSaveDetail.includes('99.5') &&
      afterSaveDetail.includes('上海市') &&
      afterSaveDetail.includes('编辑人 #1'),
    afterSaveDetail.slice(0, 200),
  );

  // 编辑后直接重投：请求带 payload；成功后原始报文仍保留
  await page.locator('.ant-drawer-body button:has-text("改后重投")').click();
  const editedRetryConfirm = page.locator('.ant-modal-confirm', { hasText: '用修改后的报文重投' });
  await editedRetryConfirm.waitFor();
  await editedRetryConfirm.locator('.ant-btn-primary').click();
  await page.waitForTimeout(700);
  const editedRetryRequest = posted.find(
    (item) => item.method === 'POST' && item.path === '/dbz/dead-letter/retry' && item.body?.payload,
  );
  check(
    '改后重投携带修改后的报文',
    editedRetryRequest?.body?.ids?.[0] === '901' &&
      editedRetryRequest?.body?.payload?.includes('"amount": 88'),
    JSON.stringify(editedRetryRequest?.body ?? null),
  );
  check(
    '改后重投成功给出提示',
    (await page.locator('.ant-message-notice-content:has-text("1 条重投成功")').count()) >= 1,
  );
  const afterRetryDetail = await page.locator('.ant-drawer-body').innerText();
  check(
    '改后重投成功后原始报文仍保留',
    afterRetryDetail.includes('最近一次重投成功') &&
      afterRetryDetail.includes('99.5') &&
      afterRetryDetail.includes('上海市'),
    afterRetryDetail.slice(0, 200),
  );
  await page.click('.ant-drawer-close');
  await page.waitForTimeout(300);

  // H. 已人工处理终态：标记（必填说明）→ 与已废弃区分 → 撤销
  await page
    .locator('.ant-table-tbody tr.ant-table-row')
    .first()
    .locator('button:has-text("已处理")')
    .click();
  await page.waitForSelector('.ant-modal:has-text("已人工处理")');
  const handledModal = visibleModal().last();
  await handledModal.locator('textarea').fill('已在源库补录缺失字段，确认无需重投');
  await handledModal.locator('.ant-modal-footer .ant-btn-primary').click();
  // 等列表回写完成再断言终态，避免只等到请求发出、表格还没刷新
  await page
    .waitForFunction(
      () =>
        Array.from(document.querySelectorAll('.ant-table-tbody tr.ant-table-row')).some((row) =>
          row.innerText.includes('已人工处理'),
        ),
      null,
      { timeout: 5000 },
    )
    .catch(() => {});
  const markHandledRequest = posted.find(
    (item) => item.method === 'POST' && item.path === '/dbz/dead-letter/mark-handled',
  );
  check(
    '标记已人工处理携带 id 与处理说明',
    markHandledRequest?.body?.ids?.[0] === '901' &&
      markHandledRequest?.body?.remark?.includes('已在源库补录缺失字段'),
    JSON.stringify(markHandledRequest?.body ?? null),
  );
  check(
    '标记后进入「已人工处理」终态且与已废弃区分',
    (await page.locator('.ant-tag:has-text("已人工处理")').count()) === 1 &&
      (await page.locator('.ant-tag:has-text("已人工处理")').count()) !==
        (await page.locator('.ant-tag:has-text("已废弃")').count()),
  );
  check(
    '标记后该行只能撤销处理，不再提供重投/废弃',
    (await page
      .locator('.ant-table-tbody tr.ant-table-row')
      .first()
      .locator('button:has-text("撤销处理")')
      .count()) === 1 &&
      (await page
        .locator('.ant-table-tbody tr.ant-table-row')
        .first()
        .locator('button:has-text("废弃")')
        .count()) === 0,
  );

  // 撤销人工处理：恢复待处理，清空处理人/说明
  await page
    .locator('.ant-table-tbody tr.ant-table-row')
    .first()
    .locator('button:has-text("撤销处理")')
    .click();
  const undoHandledConfirm = page.locator('.ant-modal-confirm', { hasText: '撤销 1 条' });
  await undoHandledConfirm.waitFor();
  await undoHandledConfirm.locator('.ant-btn-primary').click();
  await page
    .waitForFunction(
      () =>
        !Array.from(document.querySelectorAll('.ant-tag')).some((tag) =>
          tag.textContent?.includes('已人工处理'),
        ),
      null,
      { timeout: 5000 },
    )
    .catch(() => {});
  check(
    '撤销人工处理调用撤销接口',
    posted.some(
      (item) =>
        item.method === 'POST' &&
        item.path === '/dbz/dead-letter/unmark-handled' &&
        item.body?.ids?.[0] === '901',
    ),
  );
  check(
    '撤销后恢复待处理',
    (await page.locator('.ant-tag:has-text("已人工处理")').count()) === 0 &&
      (await page
        .locator('.ant-table-tbody tr.ant-table-row')
        .first()
        .locator('button:has-text("已处理")')
        .count()) === 1,
  );

  /* 903：改完仍失败 —— 原因回写到记录，原始报文不被待投覆盖 */
  const row903 = page
    .locator('.ant-table-tbody tr.ant-table-row', { hasText: '1003' })
    .first();
  await row903.locator('button:has-text("详情")').click();
  await page.waitForSelector('.ant-drawer-title:has-text("死信详情")');
  // 抽屉会先显示上一条详情再换成新的，等目标记录真正渲染出来再读
  await page
    .waitForFunction(() => document.querySelector('.ant-drawer-body')?.innerText.includes('1003'), null, {
      timeout: 5000,
    })
    .catch(() => {});
  const detail903 = await page.locator('.ant-drawer-body').innerText();
  check(
    '历史记录回填后原因清晰、原始明细仍在',
    detail903.includes('字段校验不通过，请核对并修改报文后重投') &&
      detail903.includes('原始报文（永不覆盖）') &&
      detail903.includes('"abc"') &&
      detail903.includes('已保存的待投报文') &&
      detail903.includes('88'),
    detail903.slice(0, 260),
  );
  // 失败详情入口在失败原因上方，点开才渲染全文（含历史报错）
  await page.locator('.ant-drawer-body a:has-text("查看失败详情全文")').click();
  await page.waitForSelector('.dbz-failure-detail-popover');
  const failureDetailPopover = await page.locator('.dbz-failure-detail-popover').innerText();
  check(
    '失败详情可展开全文并保留历史报错',
    failureDetailPopover.includes('历史记录原始报错') &&
      failureDetailPopover.includes('FIELD_INVALID'),
    failureDetailPopover.slice(0, 200),
  );
  // 再点链接关闭 Popover，不按 Escape，避免把抽屉一起关掉
  await page.locator('.ant-drawer-body a:has-text("查看失败详情全文")').click();
  await page.locator('.ant-drawer-body button:has-text("改后重投")').click();
  const failRetryConfirm = page.locator('.ant-modal-confirm', { hasText: '用修改后的报文重投' });
  await failRetryConfirm.waitFor();
  await failRetryConfirm.locator('.ant-btn-primary').click();
  await page.waitForTimeout(700);
  const failedRetryRequests = posted.filter(
    (item) => item.method === 'POST' && item.path === '/dbz/dead-letter/retry' && item.body?.payload,
  );
  const failedRetryRequest = failedRetryRequests.at(-1);
  check(
    '再次失败的回写请求带 id 与待投报文',
    failedRetryRequest?.body?.ids?.[0] === '903' &&
      failedRetryRequest?.body?.payload?.includes('"shipping_address": null'),
    JSON.stringify(failedRetryRequest?.body ?? null),
  );
  check(
    '再次失败原因回显到详情',
    (await page.locator('.ant-modal-confirm:has-text("1 条重投失败")').count()) === 1,
  );
  await page
    .locator('.ant-modal-confirm', { hasText: '1 条重投失败' })
    .locator('.ant-btn-primary')
    .click();
  await page.waitForTimeout(300);
  const failedDetail = await page.locator('.ant-drawer-body').innerText();
  check(
    '再次失败原因回写到记录且原始报文保留',
    failedDetail.includes('最近一次重投仍失败') &&
      failedDetail.includes('shipping_address 必填') &&
      failedDetail.includes('"abc"'),
    failedDetail.slice(0, 220),
  );

  // 不传报文 = 按原报文重投，行为与旧版一致
  await page.locator('.ant-drawer-extra button:has-text("按原报文重投")').click();
  const originalRetryConfirm = page.locator('.ant-modal-confirm', { hasText: '重投 1 条死信' });
  await originalRetryConfirm.waitFor();
  await originalRetryConfirm.locator('.ant-btn-primary').click();
  await page.waitForTimeout(700);
  const originalRetryRequest = posted.filter(
    (item) => item.method === 'POST' && item.path === '/dbz/dead-letter/retry',
  ).at(-1);
  check(
    '原报文重投不携带 payload',
    originalRetryRequest?.body?.ids?.[0] === '903' &&
      !originalRetryRequest?.body?.payload,
    JSON.stringify(originalRetryRequest?.body ?? null),
  );
  await page.click('.ant-drawer-close');
  await page.waitForTimeout(300);

  /* 902：传输类失败 —— 页面上没有改报文入口 */
  const row902 = page.locator('.ant-table-tbody tr.ant-table-row').nth(1);
  await row902.locator('button:has-text("详情")').click();
  await page.waitForSelector('.ant-drawer-title:has-text("死信详情")');
  const transportDetail = await page.locator('.ant-drawer-body').innerText();
  check(
    '传输类死信禁止编辑报文',
    transportDetail.includes('该记录不允许修改报文') &&
      (await page.locator('.ant-drawer-body textarea.dbz-json-editor').count()) === 0 &&
      (await page.locator('.ant-drawer-body button:has-text("保存待投")').count()) === 0 &&
      (await page.locator('.ant-drawer-body button:has-text("改后重投")').count()) === 0,
    transportDetail.slice(0, 180),
  );
  await page.click('.ant-drawer-close');
  await page.waitForTimeout(300);

  // 废弃：只改状态，不删除报文
  await page
    .locator('.ant-table-tbody tr.ant-table-row')
    .first()
    .locator('button:has-text("废弃")')
    .click();
  const discardConfirm = page.locator('.ant-modal-confirm', { hasText: '废弃 1 条死信' });
  await discardConfirm.waitFor();
  await discardConfirm.locator('.ant-btn-primary').click();
  await page.waitForTimeout(600);
  const discardRequest = posted.find(
    (item) => item.method === 'POST' && item.path === '/dbz/dead-letter/discard',
  );
  check(
    '废弃调用死信废弃接口',
    discardRequest?.body?.ids?.[0] === '901',
    JSON.stringify(discardRequest?.body),
  );

  const deadLetterOverflow = await page.evaluate(
    () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
  );
  check('死信页面无横向溢出', deadLetterOverflow <= 1, `溢出 ${deadLetterOverflow}px`);
  await page.screenshot({ path: `${SHOTS}/01c-dead-letter.png`, fullPage: true });

  /* 1d. 旧 CRUD 页面已删除：未知路由也回落到控制面，不再 404 */
  await page.goto(`${base}/not-exist`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.dbz-graph-node');
  check('未知路由回落到控制面', page.url().includes('/control-plane'), page.url());

  /* 1e. 登录：未登录跳转、SM3 登录、令牌失效回登录页、退出登录 */
  await page.evaluate(() => {
    localStorage.removeItem('dbz.auth.session');
    // 标记已进入认证用例，后续刷新不再自动注入夹具会话
    sessionStorage.setItem('dbz.smoke.auth-tested', '1');
  });
  await page.goto(`${base}/control-plane`, { waitUntil: 'networkidle' });
  await page.waitForURL('**/login**', { timeout: 5000 });
  check(
    '未登录访问业务页面跳登录页',
    page.url().includes('/login') &&
      (await page.locator('[data-login-page] .dbz-brand-logo').count()) === 1,
    page.url(),
  );

  // 登录失败：提示可读原因，且不写入会话
  failNextLogin = 'wrong';
  await page.locator('[data-login-account]').fill('smoke-user');
  await page.locator('[data-login-password]').fill('wrong-password');
  await page.locator('[data-login-submit]').click();
  await page.waitForSelector('[data-login-error]', { timeout: 5000 });
  const loginError = await page.locator('[data-login-error]').innerText();
  check('登录失败提示账号或口令不正确', loginError.includes('账号或口令不正确'), loginError);
  check(
    '登录失败不写入会话',
    (await page.evaluate(() => localStorage.getItem('dbz.auth.session'))) === null,
  );
  const wrongLoginRequest = loginRequests.at(-1);
  check(
    '登录请求提交 64 位 SM3 摘要，不含明文口令',
    typeof wrongLoginRequest?.body?.loginPswd === 'string' &&
      /^[0-9a-f]{64}$/.test(wrongLoginRequest.body.loginPswd) &&
      !('password' in (wrongLoginRequest?.body ?? {})),
    JSON.stringify(wrongLoginRequest?.body),
  );

  // 登录成功：保存令牌并回到业务页
  await page.locator('[data-login-password]').fill('right-password');
  await page.locator('[data-login-submit]').click();
  await page.waitForURL('**/control-plane', { timeout: 5000 });
  await page.waitForSelector('.dbz-graph-node', { timeout: 5000 });
  const savedSession = await page.evaluate(() =>
    JSON.parse(localStorage.getItem('dbz.auth.session') ?? 'null'),
  );
  check(
    '登录成功保存令牌并回到业务页',
    savedSession?.token === 'smoke-login-token' &&
      savedSession?.userId === '7' &&
      page.url().includes('/control-plane'),
    JSON.stringify(savedSession),
  );
  check(
    '后续业务请求携带登录令牌与真实用户 ID',
    getRequests.some(
      (item) =>
        item.path.startsWith('/dbz/') &&
        item.headers.authorization === 'Bearer smoke-login-token' &&
        item.headers['x-user-id'] === '7',
    ),
  );

  // 令牌失效（HTTP 401）：清会话回登录页
  failNext401 = true;
  ignore401Console = true;
  await page.goto(`${base}/control-plane`, { waitUntil: 'domcontentloaded' });
  await page.waitForURL('**/login**', { timeout: 5000 });
  check(
    '令牌失效返回 401 时清掉登录态并回登录页',
    page.url().includes('/login') &&
      (await page.evaluate(() => localStorage.getItem('dbz.auth.session'))) === null,
    page.url(),
  );
  ignore401Console = false;

  // 重新登录后走退出登录
  await page.locator('[data-login-account]').fill('smoke-user');
  await page.locator('[data-login-password]').fill('right-password');
  await page.locator('[data-login-submit]').click();
  await page.waitForURL('**/control-plane', { timeout: 5000 });
  await page.locator('[data-header-user]').click();
  await page.locator('.ant-dropdown-menu-item:has-text("退出登录")').click();
  await page.waitForURL('**/login**', { timeout: 5000 });
  check(
    '退出登录清会话并回登录页',
    page.url().includes('/login') &&
      (await page.evaluate(() => localStorage.getItem('dbz.auth.session'))) === null,
    page.url(),
  );
} finally {
  await browser.close();
  await server.close();
}

console.log('\n--- 控制台错误 ---');
console.log(consoleErrors.length ? consoleErrors.join('\n') : '(无)');
console.log('--- 页面异常 ---');
console.log(pageErrors.length ? pageErrors.join('\n') : '(无)');
console.log(`--- 截图 ---\n${SHOTS}`);

const failed = checks.filter((item) => !item.ok);
console.log(`\n${checks.length - failed.length}/${checks.length} 通过`);
process.exit(failed.length || pageErrors.length || consoleErrors.length ? 1 : 0);
