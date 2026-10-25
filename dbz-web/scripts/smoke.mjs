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
 * ```
 *
 * 截图输出到系统临时目录 `dbz-web-smoke/shots`，失败时按截图定位问题。
 */

import { mkdirSync } from 'node:fs';
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
    connectorConfig: '{"connector.class":"io.debezium.connector.mysql.MySqlConnector"}',
    runStatus: 1,
    remark: '',
    enabled: true,
    createrId: '1',
    createMs: '1759990000000',
    updaterId: '1',
    updateMs: '1760000100000',
  },
  {
    id: '202',
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
    connectorConfig: '{"connector.class":"io.debezium.connector.jdbc.JdbcSinkConnector"}',
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

const plugins = [
  { class: 'io.debezium.connector.mysql.MySqlConnector', type: 'source', version: '2.5.0.Final' },
  { class: 'io.debezium.connector.jdbc.JdbcSinkConnector', type: 'sink', version: '2.5.0.Final' },
];

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

const browser = await chromium.launch();
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
const page = await context.newPage();

const pageErrors = [];
const consoleErrors = [];
const posted = [];
/** 置 true 让下一次预检失败一次，用完自动复位：用来验证「预检失败能原地重试」 */
let failNextPreview = false;

page.on('pageerror', (error) => pageErrors.push(String(error)));
page.on('console', (message) => {
  if (message.type() === 'error') {
    consoleErrors.push(message.text());
  }
});

await page.route('**/dbz/**', async (route) => {
  const request = route.request();
  const url = new URL(request.url());
  const path = url.pathname;

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
    await route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    return;
  }

  if (path === '/dbz/kafka-broker/page') return route.fulfill({ json: pageOf(brokers) });
  if (path === '/dbz/kafka-broker/list') return route.fulfill({ json: ro(brokers) });
  if (path === '/dbz/kafka-connect/page') return route.fulfill({ json: pageOf(clusters) });
  if (path === '/dbz/kafka-connect/list') return route.fulfill({ json: ro(clusters) });
  if (path === '/dbz/source-connector/page') return route.fulfill({ json: pageOf(sources) });
  if (path === '/dbz/source-connector/list') return route.fulfill({ json: ro(sources) });
  if (path === '/dbz/sink-connector/page') return route.fulfill({ json: pageOf(sinks) });
  if (path === '/dbz/sink-connector/list') return route.fulfill({ json: ro(sinks) });
  if (path === '/dbz/kafka-connector/list') return route.fulfill({ json: ro(connectors) });
  if (path === '/dbz/kafka-connector/plugins') return route.fulfill({ json: ro(plugins) });
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
  /* 1. 总览：统计与异常清单 */
  await page.goto(`${base}/`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.ant-card-head-title:has-text("异常清单")');
  check('总览渲染异常清单', await page.locator('.ant-card-head-title:has-text("异常清单")').first().isVisible());
  const anomalyRows = await page.locator('tbody tr.ant-table-row').count();
  check('异常清单 4 行（消息集群/集群/上游/下游各 1）', anomalyRows === 4, `实际 ${anomalyRows}`);
  check('异常清单含消息集群', (await page.locator('tbody .ant-tag:has-text("消息集群")').count()) === 1);

  // 五张卡片：Kafka 集群 / Connect 集群 / 上游 / 下游 / 异常对象
  const statCards = page.locator('.dbz-stat-card');
  check('总览 5 张统计卡片', (await statCards.count()) === 5, `实际 ${await statCards.count()}`);
  const brokerCard = statCards.filter({ hasText: 'Kafka 集群' }).first();
  check(
    'Kafka 集群卡片统计到 3 台',
    (await brokerCard.locator('.dbz-stat-value').textContent()) === '3',
    String(await brokerCard.locator('.dbz-stat-value').textContent()),
  );
  check(
    '禁用中的集群计入禁用而不是异常',
    (await brokerCard.locator('.ant-tag:has-text("禁用 1")').count()) === 1 &&
      (await brokerCard.locator('.ant-tag:has-text("异常 1")').count()) === 1,
  );
  check(
    '链路速查提示没人用的 Kafka 集群',
    (await page.locator('text=其中 1 个还没有 worker 指向它').count()) > 0,
  );
  await page.screenshot({ path: `${SHOTS}/01-dashboard.png`, fullPage: true });

  /* 1b. Kafka 集群（broker）：列表、检测、SASL 字段联动 */
  await page.click('.ant-menu-item:has-text("Kafka 集群")');
  await page.waitForSelector('text=订单消息集群');
  const brokerRows = await page.locator('tbody tr.ant-table-row').count();
  check('Kafka 集群列表 3 行', brokerRows === 3, `实际 ${brokerRows}`);
  check('broker 地址原样展示', (await page.locator('text=10.0.0.21:9092,10.0.0.22:9092').count()) > 0);
  check('broker 状态标签渲染', (await page.locator('text=连接失败').count()) > 0);
  // 悬停看详情：状态是结论，原因写在 runMsg 里
  const brokerDetail = await hoverStatus(
    '风控消息集群',
    'SASL authentication failed',
    'Kafka 集群',
    '02b-broker-status.png',
  );
  check(
    'broker 详情带地址与认证方式',
    brokerDetail.includes('10.0.1.31:9092') && brokerDetail.includes('SCRAM-SHA-256'),
    brokerDetail.slice(0, 120),
  );
  await layoutCheck(page, 'Kafka 集群');
  await page.screenshot({ path: `${SHOTS}/02-kafka-broker.png`, fullPage: true });

  // 检测：真去拉一次 topic 列表，连不通也是 200（reachable=false）
  await page.locator('tbody tr.ant-table-row').first().locator('button:has-text("检测")').click();
  await page.waitForTimeout(600);
  const brokerCheck = posted.find(
    (item) => item.method === 'POST' && item.path === '/dbz/kafka-broker/check',
  );
  check('检测发出 POST 且带记录ID', brokerCheck?.body?.id === '401', JSON.stringify(brokerCheck?.body));
  check(
    '检测成功给出提示',
    (await page.locator('.ant-message-notice-content:has-text("连接正常")').count()) > 0,
  );

  // 表单联动：明文协议不出现机制/账号/密码，选了 SASL 才出现
  await page.click('button:has-text("新增集群")');
  await page.waitForSelector('.ant-modal-wrap', { state: 'visible' });
  await page.waitForTimeout(450);
  check('明文协议下无认证字段', (await page.locator('#saslPassword').count()) === 0);
  await page.locator('.ant-modal .ant-select-selector').first().click();
  await page.click('.ant-select-item-option:has-text("SASL_PLAINTEXT")');
  await page.waitForTimeout(300);
  check('切到 SASL 后出现认证字段', (await page.locator('#saslPassword').count()) === 1);
  // 机制不能停在「没有标签的 0」上：切到 SASL 时要给一个能连通的默认值
  const mechanism = await page
    .locator('.ant-modal .ant-select')
    .nth(1)
    .locator('.ant-select-selection-item')
    .textContent();
  check('切到 SASL 后机制有默认值', mechanism === 'SASL/PLAIN', String(mechanism));
  await page.screenshot({ path: `${SHOTS}/03b-broker-form.png` });
  await page.locator('.ant-modal-footer .ant-btn').first().click();
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });

  /* 2. 集群列表 */
  await page.click('.ant-menu-item:has-text("Kafka Connect")');
  await page.waitForSelector('text=生产集群-华东');
  const clusterRows = await page.locator('tbody tr.ant-table-row').count();
  check('集群列表 2 行', clusterRows === 2, `实际 ${clusterRows}`);
  check('集群状态标签渲染', (await page.locator('text=连接失败').count()) > 0);
  await hoverStatus('灰度集群', 'connection refused', 'Kafka Connect');

  const headerBox = await page.locator('.dbz-header-title').boundingBox();
  check(
    '顶栏标题完整可见',
    Boolean(headerBox) && headerBox.y >= 0 && headerBox.y + headerBox.height <= 60,
    JSON.stringify(headerBox),
  );
  await layoutCheck(page, '集群');
  await page.screenshot({ path: `${SHOTS}/02-kafka-connect.png`, fullPage: true });

  /* 2b. 启用列本身就是开关：关掉要确认，只提交 id + enabled */
  const enabledSwitch = page.locator('tbody tr.ant-table-row').first().getByRole('switch');
  check('启用列渲染为开关', (await enabledSwitch.getAttribute('aria-checked')) === 'true');
  await enabledSwitch.click();
  await page.waitForSelector('.ant-modal-confirm');
  await page.click('.ant-modal-confirm .ant-btn-primary');
  await page.waitForTimeout(400);
  const toggleCall = posted.find((item) => item.method === 'PUT' && item.path === '/dbz/kafka-connect');
  check(
    '禁用只提交 id 与 enabled',
    toggleCall?.body?.id === '101' && toggleCall?.body?.enabled === false && Object.keys(toggleCall.body).length === 2,
    JSON.stringify(toggleCall?.body),
  );

  /* 3. 新增：默认值、请求头、请求体、提交后关闭 */
  await page.click('button:has-text("新增集群")');
  await page.waitForSelector('.ant-modal-wrap', { state: 'visible' });
  await page.waitForTimeout(450);
  check(
    '新增弹窗不再有启用开关',
    (await page.locator('.ant-modal').getByRole('switch').count()) === 0,
  );

  await page.fill('#name', '冒烟测试集群');
  await page.fill('#url', 'http://127.0.0.1:8083');
  // Kafka 集群是必填项：这台 worker 上的连接器把变更投到哪个集群
  await page.locator('.ant-modal .ant-select-selector').first().click();
  await page.click('.ant-select-item-option:has-text("订单消息集群")');
  await page.waitForTimeout(200);
  await page.screenshot({ path: `${SHOTS}/03-modal.png` });
  await page.click('.ant-modal-footer .ant-btn-primary');
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });
  check('保存后弹窗关闭', !(await page.locator('.ant-modal-wrap').isVisible().catch(() => false)));

  const createCall = posted.find((item) => item.method === 'POST' && item.path === '/dbz/kafka-connect');
  check('新增集群发出 POST', Boolean(createCall));
  check('写接口带 X-User-Id', createCall?.headers['x-user-id'] === '1', String(createCall?.headers['x-user-id']));
  check(
    '新增集群带上所属 Kafka 集群',
    createCall?.body?.kafkaBrokerId === '401',
    JSON.stringify(createCall?.body),
  );
  check(
    '请求体为 camelCase 且已 trim',
    createCall?.body?.name === '冒烟测试集群' && createCall?.body?.url === 'http://127.0.0.1:8083',
    JSON.stringify(createCall?.body),
  );
  // 启用/禁用移到列表后，新增一律先建成启用
  check('新增请求体默认启用', createCall?.body?.enabled === true, JSON.stringify(createCall?.body));

  /* 3b. 编辑回填 */
  await page.locator('tbody tr.ant-table-row').first().locator('.ant-btn-link').first().click();
  await page.waitForSelector('.ant-modal-wrap', { state: 'visible' });
  await page.waitForTimeout(450);
  check('编辑表单回填名称', (await page.inputValue('#name')) === '生产集群-华东', await page.inputValue('#name'));
  check(
    '编辑弹窗不再有启用开关',
    (await page.locator('.ant-modal').getByRole('switch').count()) === 0,
  );
  await page.screenshot({ path: `${SHOTS}/09-edit.png` });
  await page.locator('.ant-modal-footer .ant-btn').first().click();
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });

  /* 4. 上游连接器 */
  await page.click('.ant-menu-item:has-text("上游连接器")');
  await page.waitForSelector('text=订单库增量');
  check('上游列表渲染', (await page.locator('tbody tr.ant-table-row').count()) === 2);
  check('上游「所属集群」翻译成名称', (await page.locator('text=生产集群-华东').count()) > 0);
  await hoverStatus('用户库增量', 'Access denied for user', '上游连接器', '04b-source-status.png');
  await layoutCheck(page, '上游');
  await page.screenshot({ path: `${SHOTS}/04-source.png`, fullPage: true });

  /* 4b. 配置编辑器：图形化与 JSON 是同一份数据，换库要真的换过来 */
  await page.click('button:has-text("新增上游连接器")');
  await page.waitForSelector('.ant-modal-wrap', { state: 'visible' });
  await page.waitForTimeout(450);
  check('默认进入图形化模式', (await visibleModal().locator('.ant-segmented-item-selected:has-text("图形化")').count()) === 1);
  check('上游图形化按组列出配置项', (await visibleModal().locator('.dbz-config-group').count()) === 6);

  await visibleModal().getByRole('button', { name: '套用 MySQL 模板' }).click();
  await page.waitForTimeout(200);
  check('套用模板后说明改了什么', (await visibleModal().locator('text=已套用 MySQL 模板').count()) === 1);
  // 认得出方言后，只显示这家库的捕获范围（按 label 的 for 定位，避免命中帮助文字里的同名字段）
  check(
    'MySQL 方言下只显示 MySQL 的捕获范围',
    (await visibleModal().locator('label[for="database.include.list"]').count()) === 1 &&
      (await visibleModal().locator('label[for="schema.include.list"]').count()) === 0,
  );
  check('MySQL 方言下不显示复制插件', (await visibleModal().locator('label[for="plugin.name"]').count()) === 0);

  // 先填一个 MySQL 专属键，再换 PostgreSQL：它必须跟着消失，否则就是无效参数
  await page.fill('[id="database.include.list"]', 'orders');
  await page.waitForTimeout(150);

  await visibleModal().getByRole('button', { name: '套用 PostgreSQL 模板' }).click();
  await page.waitForTimeout(250);
  check(
    '换库后方言字段跟着换',
    (await visibleModal().locator('label[for="schema.include.list"]').count()) === 1 &&
      (await visibleModal().locator('label[for="plugin.name"]').count()) === 1 &&
      (await visibleModal().locator('label[for="database.include.list"]').count()) === 0,
  );
  await page.screenshot({ path: `${SHOTS}/04b-config-editor.png` });

  // 枚举值光看名字猜不出行为（schema_only_recovery 尤其），下拉里每项都要带注释
  // 点外层 .ant-select：值已经选中时，选中文本那层会挡住 input，直接点 input 会超时
  await visibleModal().locator('.ant-select:has([id="snapshot.mode"])').click();
  await page.waitForTimeout(300);
  const modeOptions = page.locator('.ant-select-dropdown:visible .ant-select-item-option');
  const recoveryText = await page
    .locator('.ant-select-dropdown:visible .ant-select-item-option:has-text("schema_only_recovery")')
    .innerText();
  check('快照模式下拉 7 项齐全', (await modeOptions.count()) === 7, String(await modeOptions.count()));
  check('快照模式每项都带注释', recoveryText.includes('修复'), recoveryText);
  await page.screenshot({ path: `${SHOTS}/04b2-snapshot-mode.png` });
  await page.keyboard.press('Escape');
  await page.waitForTimeout(250);

  await visibleModal().locator('.ant-segmented-item:has-text("JSON")').click();
  await visibleModal().locator('.dbz-json-editor').waitFor();
  const configJson = JSON.parse(await visibleModal().locator('.dbz-json-editor').inputValue());
  check(
    '换库后连接器类改成 PostgreSQL',
    configJson['connector.class'] === 'io.debezium.connector.postgresql.PostgresConnector',
    String(configJson['connector.class']),
  );
  check('换库后端口跟着换', configJson['database.port'] === 5432, String(configJson['database.port']));
  check(
    '换库后补上复制插件与模式',
    configJson['plugin.name'] === 'pgoutput' && configJson['schema.include.list'] === 'public',
    JSON.stringify({ plugin: configJson['plugin.name'], schema: configJson['schema.include.list'] }),
  );
  check(
    '换库后清掉 MySQL 专属键',
    !('database.include.list' in configJson),
    JSON.stringify(Object.keys(configJson)),
  );
  await visibleModal().locator('.ant-segmented-item:has-text("图形化")').click();
  await page.waitForTimeout(150);
  await visibleModal().locator('.ant-modal-footer .ant-btn').first().click();
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });
  // 关掉的弹窗在退出动画期间还挂在 DOM 里，等它退干净再去点列表，否则后面按文本找元素会命中它
  await page.waitForTimeout(400);

  /* 4c. 重新同步：两条通道可选，切换会重新预检，提交带 channel */
  await page.locator('tbody tr.ant-table-row').first().locator('button:has-text("重新同步")').click();
  await visibleModal().locator('text=信号表').first().waitFor();
  check('预检默认走源库信号表', (await visibleModal().locator('text=orders.debezium_signal').count()) > 0);
  check(
    '两条通道都能用时给出单选',
    (await visibleModal().locator('.ant-radio-wrapper:has-text("源库信号表")').count()) === 1 &&
      (await visibleModal().locator('.ant-radio-wrapper:has-text("Kafka 信号 topic")').count()) === 1,
  );
  await visibleModal().locator('.ant-radio-wrapper:has-text("Kafka 信号 topic")').click();
  await page.waitForTimeout(500);
  check('切到 Kafka 通道后展示落点', (await visibleModal().locator('text=dbz-signal').count()) > 0);
  // 通道清单与本次选择无关：切过去之后两个选项得还在，否则切错了就退不回来。
  // 注意别用 `.ant-radio-wrapper` 总数判断 —— 下面「快照类型」也是单选组
  check(
    '切到 Kafka 后仍能切回源库信号表',
    (await visibleModal().locator('.ant-radio-wrapper:has-text("源库信号表")').count()) === 1 &&
      (await visibleModal().locator('.ant-radio-wrapper-checked:has-text("Kafka 信号 topic")').count()) === 1,
  );
  await page.screenshot({ path: `${SHOTS}/04c-snapshot.png` });
  await visibleModal().locator('.ant-modal-footer .ant-btn-primary').click();
  await page.waitForTimeout(600);
  const snapshotCall = posted.find(
    (item) => item.method === 'POST' && item.path === '/dbz/source-connector/snapshot',
  );
  check(
    '触发快照提交所选通道与表',
    snapshotCall?.body?.channel === 'kafka' && snapshotCall?.body?.id === '201',
    JSON.stringify(snapshotCall?.body),
  );
  // 提交成功后弹窗自己收起，等退出动画跑完再点下一行
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });
  await page.waitForTimeout(400);

  /* 4d. 预检失败：说清原因，并能原地重试（不用关掉弹窗重开） */
  failNextPreview = true;
  await page
    .locator('tbody tr.ant-table-row')
    .nth(1)
    .locator('button:has-text("重新同步")')
    .click();
  await visibleModal().locator('text=预检失败').waitFor();
  check(
    '预检失败显示原因',
    (await visibleModal().locator('text=Connection refused').count()) > 0,
  );
  check('预检失败给重试按钮', (await visibleModal().locator('button:has-text("重新预检")').count()) === 1);
  check(
    '预检失败时不能触发快照',
    await visibleModal().locator('.ant-modal-footer .ant-btn-primary').isDisabled(),
  );
  await visibleModal().locator('button:has-text("重新预检")').click();
  await visibleModal().locator('text=信号表').first().waitFor();
  check('重试后预检成功', (await visibleModal().locator('text=orders.debezium_signal').count()) > 0);
  check(
    '重试成功后可以触发快照',
    await visibleModal().locator('.ant-modal-footer .ant-btn-primary').isEnabled(),
  );
  await page.screenshot({ path: `${SHOTS}/04d-snapshot-retry.png` });
  await visibleModal().locator('.ant-modal-footer .ant-btn').first().click();
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });
  await page.waitForTimeout(400);

  /* 5. 下游连接器 */
  await page.click('.ant-menu-item:has-text("下游连接器")');
  await page.waitForSelector('text=订单同步到数仓');
  check('下游列表渲染', (await page.locator('tbody tr.ant-table-row').count()) === 1);
  await hoverStatus('订单同步到数仓', 'connection.url is required', '下游连接器');
  await layoutCheck(page, '下游');
  await page.screenshot({ path: `${SHOTS}/05-sink.png`, fullPage: true });

  /* 5b. 下游配置也走图形化：sink 的参数键与上游完全不同，分组要跟着换 */
  await page.click('button:has-text("新增下游连接器")');
  await page.waitForSelector('.ant-modal-wrap', { state: 'visible' });
  await page.waitForTimeout(450);
  check('下游默认图形化', (await visibleModal().locator('.ant-segmented-item-selected:has-text("图形化")').count()) === 1);
  check('下游图形化按组列出配置项', (await visibleModal().locator('.dbz-config-group').count()) === 5);
  check(
    '下游不出现上游的源库键',
    (await visibleModal().locator('label[for="database.include.list"]').count()) === 0 &&
      (await visibleModal().locator('label[for="signal.enabled.channels"]').count()) === 0,
  );
  check('下游字段是 sink 的键', (await visibleModal().locator('label[for="connection.url"]').count()) === 1);

  await visibleModal().getByRole('button', { name: '套用 JDBC Sink 模板', exact: true }).click();
  await page.waitForTimeout(200);
  await visibleModal().locator('.ant-segmented-item:has-text("JSON")').click();
  await visibleModal().locator('.dbz-json-editor').waitFor();
  const sinkJson = JSON.parse(await visibleModal().locator('.dbz-json-editor').inputValue());
  check(
    '下游模板写入 JDBC sink 与追加语义',
    sinkJson['connector.class'] === 'io.debezium.connector.jdbc.JdbcSinkConnector' &&
      sinkJson['insert.mode'] === 'insert',
    JSON.stringify({ class: sinkJson['connector.class'], mode: sinkJson['insert.mode'] }),
  );

  // 两个下游模板互为切换：换成 upsert 要真的改过来，并提醒补主键字段
  await visibleModal().locator('.ant-segmented-item:has-text("图形化")').click();
  await page.waitForTimeout(150);
  await visibleModal().getByRole('button', { name: '套用 JDBC Sink 模板（upsert）' }).click();
  await page.waitForTimeout(200);
  check(
    '切到 upsert 后提醒补主键字段',
    (await visibleModal().locator('text=但没填 primary.key.fields').count()) > 0,
  );
  await page.screenshot({ path: `${SHOTS}/05b-sink-editor.png` });
  await visibleModal().locator('.ant-segmented-item:has-text("JSON")').click();
  await visibleModal().locator('.dbz-json-editor').waitFor();
  const sinkUpsert = JSON.parse(await visibleModal().locator('.dbz-json-editor').inputValue());
  check(
    '切到 upsert 模板后写入模式改过来',
    sinkUpsert['insert.mode'] === 'upsert' && sinkUpsert['primary.key.mode'] === 'record_value',
    JSON.stringify({ mode: sinkUpsert['insert.mode'], keyMode: sinkUpsert['primary.key.mode'] }),
  );
  await visibleModal().locator('.ant-modal-footer .ant-btn').first().click();
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });

  /* 6. 远端连接器：列表、未纳管连接器、任务展开与重启 */
  await page.goto(`${base}/remote-connector?kafkaConnectId=101`, { waitUntil: 'networkidle' });
  await page.waitForSelector('text=mysql-orders-source');
  check('远端连接器列表渲染', (await page.locator('tbody tr.ant-table-row').count()) === 2);
  check('未纳管连接器可见', (await page.locator('text=manual-test-connector').count()) > 0);
  // 这一页的详情来自集群的 expand=status 响应，不需要额外请求
  const remoteDetail = await hoverStatus(
    'manual-test-connector',
    'Failed to read from the WAL',
    '远端连接器',
    '06b-remote-status.png',
  );
  check(
    '远端详情带 worker 与类名',
    remoteDetail.includes('10.0.0.12:8083') && remoteDetail.includes('PostgresConnector'),
    remoteDetail.slice(0, 160),
  );
  await page.screenshot({ path: `${SHOTS}/06-remote.png`, fullPage: true });

  await page.locator('tbody tr.ant-table-row').first().locator('.ant-table-row-expand-icon').click();
  await page.waitForSelector('text=10.0.0.11:8083');
  check('任务行展开', (await page.locator('text=10.0.0.11:8083').count()) > 0);
  await page.screenshot({ path: `${SHOTS}/07-tasks.png`, fullPage: true });

  await page.locator('.ant-table-expanded-row .ant-btn-link').first().click();
  await page.waitForSelector('.ant-popconfirm');
  await page.click('.ant-popconfirm .ant-btn-primary');
  await page.waitForTimeout(600);
  const taskRestart = posted.find((item) => item.path === '/dbz/kafka-connector/task/restart');
  check('重启任务发出 POST', Boolean(taskRestart), JSON.stringify(taskRestart?.body));

  /* 6b. 删除：只发 DELETE，定位参数走查询串 */
  await page.locator('tbody tr.ant-table-row').first().locator('button:has-text("删除")').click();
  // 上一步的重启确认框还留在 DOM 里（隐藏），这里按标题挑出删除那个
  const removePopconfirm = page.locator('.ant-popconfirm', { hasText: '从集群上删除' });
  await removePopconfirm.waitFor();
  await removePopconfirm.locator('.ant-btn-primary').click();
  await page.waitForTimeout(600);
  const removeCall = posted.find(
    (item) => item.method === 'DELETE' && item.path === '/dbz/kafka-connector',
  );
  const removeQuery = new URLSearchParams(removeCall?.query ?? '');
  check(
    '删除远端连接器发出 DELETE 且带集群与连接器名',
    removeQuery.get('kafkaConnectId') === '101' &&
      removeQuery.get('connectorName') === 'mysql-orders-source',
    String(removeCall?.query),
  );

  /* 6c. 停止：保留配置与位点，只发 PUT，连接器名走查询串 */
  await page.locator('tbody tr.ant-table-row').first().locator('button:has-text("停止")').click();
  const stopPopconfirm = page.locator('.ant-popconfirm', { hasText: '停止连接器' });
  await stopPopconfirm.waitFor();
  await stopPopconfirm.locator('.ant-btn-primary').click();
  await page.waitForTimeout(600);
  const stopCall = posted.find(
    (item) => item.method === 'PUT' && item.path === '/dbz/kafka-connector/stop',
  );
  const stopQuery = new URLSearchParams(stopCall?.query ?? '');
  check(
    '停止连接器发出 PUT 且带集群与连接器名',
    stopQuery.get('kafkaConnectId') === '101' &&
      stopQuery.get('connectorName') === 'mysql-orders-source',
    String(stopCall?.query),
  );

  /* 6d. 重置位点：先停再清，resume 由用户勾选 */
  await page.locator('tbody tr.ant-table-row').first().locator('button:has-text("重置位点")').click();
  await page.waitForSelector('.ant-modal:has-text("位点删了就没有了")');
  await page.locator('.ant-modal .ant-checkbox-wrapper').click();
  await page.waitForTimeout(150);
  await page.screenshot({ path: `${SHOTS}/07b-reset-offsets.png` });
  await page.click('.ant-modal-footer .ant-btn-primary');
  await page.waitForTimeout(600);
  const resetCall = posted.find(
    (item) => item.method === 'DELETE' && item.path === '/dbz/kafka-connector/offsets',
  );
  const resetQuery = new URLSearchParams(resetCall?.query ?? '');
  check(
    '重置位点发出 DELETE 且 resume=true',
    resetQuery.get('kafkaConnectId') === '101' &&
      resetQuery.get('connectorName') === 'mysql-orders-source' &&
      resetQuery.get('resume') === 'true',
    String(resetCall?.query),
  );

  /* 7. 插件 */
  await page.click('.ant-tabs-tab:has-text("插件")');
  await page.waitForSelector('text=2.5.0.Final');
  check('插件列表渲染', (await page.locator('text=2.5.0.Final').count()) === 2);
  await page.screenshot({ path: `${SHOTS}/08-plugins.png`, fullPage: true });

  /* 8. 未知路由回落总览 */
  await page.goto(`${base}/not-exist`, { waitUntil: 'networkidle' });
  check('未知路由回落总览', page.url().endsWith('/'));
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
