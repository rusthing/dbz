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
    connectorConfig:
      '{"connector.class":"io.debezium.connector.mysql.MySqlConnector","snapshot.mode":"when_needed"}',
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
const page = await context.newPage();

const pageErrors = [];
const consoleErrors = [];
const posted = [];
/** 控制面画布布局：PUT 落库、GET 读取，用来验证「刷新后位置保持」 */
const layoutStore = [];
let nextBrokerId = 499;
/** 置 true 让下一次预检失败一次，用完自动复位：用来验证「预检失败能原地重试」 */
let failNextPreview = false;
/** 控制面状态刷新次数：验证进入页面、手动刷新与自动刷新都真的回写了状态 */
let refreshCalls = 0;
/** 置 true 让下一次重置位点失败：验证弹窗说明停在哪一步 */
let failNextReset = false;
/** 置 true 让下一次启用开关失败：验证开关回滚 */
let failNextSinkToggle = false;

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
      return route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
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
  // 集群侧连接器配置：详情抽屉优先展示这一份
  if (path === '/dbz/kafka-connector/config') {
    return route.fulfill({
      json: ro({
        connectorName: url.searchParams.get('connectorName'),
        config: {
          'connector.class': 'io.debezium.connector.mysql.MySqlConnector',
          'database.hostname': '10.0.0.11',
          'table.include.list': 'dbz.orders',
        },
      }),
    });
  }
  if (path === '/dbz/dead-letter/page') return route.fulfill({ json: pageOf(deadLetters) });
  if (path === '/dbz/control-plane/status/refresh') {
    refreshCalls += 1;
    return route.fulfill({ json: ro(0) });
  }
  if (path === '/dbz/control-plane/layout') {
    return route.fulfill({ json: ro(layoutStore) });
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

  await page.click('.ant-menu-item:has-text("控制面视图")');
  await page.waitForSelector('.dbz-graph-node[data-node-key="1:401"]');
  check(
    '画布渲染 11 个节点（3 broker + 2 Connect + 2 上游 + 4 下游）',
    (await page.locator('.dbz-graph-node').count()) === 11,
    `实际 ${await page.locator('.dbz-graph-node').count()}`,
  );
  check(
    '画布只渲染归属连线（8 条）',
    (await page.locator('.dbz-graph-edge-hit').count()) === 8,
    `实际 ${await page.locator('.dbz-graph-edge-hit').count()}`,
  );
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
    '调色板已移除，建链只走节点上的「+」',
    (await page.locator('.dbz-palette-item').count()) === 0,
    `实际 ${await page.locator('.dbz-palette-item').count()}`,
  );
  check(
    '缩略图与缩放控件可见',
    (await page.locator('.dbz-graph-minimap').isVisible()) &&
      (await page.locator('.dbz-graph-controls').isVisible()),
  );
  const legendText = await page.locator('.dbz-graph-legend').innerText();
  check(
    '图例只保留只读归属线',
    legendText.includes('归属') && legendText.includes('只读') && !legendText.includes('数据流'),
    legendText,
  );
  check(
    '顶栏展示节点数',
    (await page.locator('.dbz-canvas-topbar .ant-tag:has-text("11 个节点")').count()) === 1,
  );
  check(
    '顶部汇总 Connect 健康可达数',
    (await page.locator('.dbz-canvas-topbar .ant-tag:has-text("Connect 可达 2/2")').count()) === 1,
  );
  check(
    '顶部汇总待处理死信总数',
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
  for (const label of ['运行中', '已停止', '仅纳管未部署', '失败', '不可达', '已停用']) {
    check(`统一状态「${label}」正确呈现`, statusLabels.includes(label), statusLabels.join(','));
  }
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
    addCounts.broker === 3 &&
      addCounts.connect === 2 &&
      addCounts.source === 2 &&
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

  // 卡片上的集群检测
  await page.locator('.dbz-graph-node[data-node-key="1:402"] [data-node-action="check"]').click();
  await page.waitForTimeout(400);
  const checkRequest = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/kafka-broker/check',
  );
  check(
    '卡片上的集群检测调用检测接口',
    checkRequest?.body?.id === '402',
    JSON.stringify(checkRequest?.body),
  );

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
    (await page.locator('.dbz-graph-switch .ant-switch').count()) === 11,
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
  check(
    '停用态整卡可辨但与异常态不同',
    (await sink301.evaluate((element) => element.classList.contains('is-disabled'))) &&
      (await sink301.locator('.dbz-graph-status-tag .ant-tag').innerText()).trim() === '已停用',
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

  // 卡片上的删除：提示只说明归属关系，不再列数据流与写入目标
  await page.locator('.dbz-graph-node[data-node-key="4:301"] [data-node-action="delete"]').click();
  const deleteConfirm = page.locator('.ant-modal-confirm');
  await deleteConfirm.waitFor({ state: 'visible' });
  const deleteText = (await deleteConfirm.innerText()).replace(/\s+/g, ' ');
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
  await deleteConfirm.locator('.ant-btn').first().click();
  await deleteConfirm.waitFor({ state: 'hidden' });
  check(
    '取消删除时不调用删除接口',
    !posted.some((item) => item.method === 'DELETE' && item.path === '/dbz/sink-connector/301'),
  );

  // 点选归属连线 → 属性框说明只读；没有任何拖拽手柄，也不能删除
  const ownershipHit = page.locator(
    '[data-edge-id="own:connect:101:source:201"].dbz-graph-edge-hit',
  );
  const ownershipBox = await ownershipHit.boundingBox();
  await page.mouse.click(
    ownershipBox.x + ownershipBox.width / 2,
    ownershipBox.y + ownershipBox.height / 2,
  );
  await page.waitForSelector('.dbz-graph-inspector .dbz-inspector-panel');
  const edgeInspectorText = await page.locator('.dbz-graph-inspector').innerText();
  check(
    '选中归属连线后属性框说明只读',
    edgeInspectorText.includes('归属连线') &&
      edgeInspectorText.includes('不能拖动') &&
      edgeInspectorText.includes('不能删除'),
    edgeInspectorText.slice(0, 160),
  );
  check(
    '连线没有任何拖拽手柄或锁标',
    (await page.locator('.dbz-graph-edge-handle').count()) === 0 &&
      (await page.locator('.dbz-graph-edge-lock').count()) === 0,
  );
  await page.keyboard.press('Delete');
  await page.waitForTimeout(300);
  check(
    'Delete 快捷键不会删除连线，也不弹旧提示',
    (await page.locator('.ant-message-notice-content:has-text("连线不能删除")').count()) === 0 &&
      !posted.some((item) => item.method === 'DELETE'),
  );

  // 在连线上拖动：只作展示，不产生任何写请求、也不改上游
  const postedBeforeEdgeDrag = posted.length;
  await page.mouse.move(ownershipBox.x + ownershipBox.width / 2, ownershipBox.y + ownershipBox.height / 2);
  await page.mouse.down();
  await page.mouse.move(ownershipBox.x + 180, ownershipBox.y + 90, { steps: 12 });
  await page.mouse.up();
  await page.waitForTimeout(300);
  check(
    '拖动连线不产生写请求，也不改上下游',
    posted.length === postedBeforeEdgeDrag,
    `${postedBeforeEdgeDrag} -> ${posted.length}`,
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

  // 顶栏「新增 Kafka 集群」：表单保存后落到画布并写入布局
  await page.locator('.dbz-canvas-topbar button:has-text("新增 Kafka 集群")').click();
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
    '顶栏新增 Kafka 集群调用创建接口',
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
    (await page.locator('.dbz-canvas-topbar .ant-tag:has-text("12 个节点")').count()) === 1,
  );

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

  check(
    '界面上不存在增量快照/重新同步入口',
    !(await page.locator('.dbz-graph-viewport').innerText()).includes('增量快照') &&
      !(await page.locator('.dbz-graph-viewport').innerText()).includes('重新同步'),
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
  check(
    '死信筛选器回填连接器名称',
    (await page.locator('.ant-select-selection-item:has-text("下游 · 订单同步到数仓")').count()) >= 1,
  );
  check(
    '死信状态与失败分类标签渲染',
    (await page.locator('.ant-tag:has-text("待处理")').count()) >= 1 &&
      (await page.locator('.ant-tag:has-text("已重投")').count()) >= 1 &&
      (await page.locator('.ant-tag:has-text("确定性失败")').count()) >= 1 &&
      (await page.locator('.ant-tag:has-text("传输类失败")').count()) >= 1 &&
      (await page.locator('.ant-tag:has-text("字段不合规")').count()) >= 1,
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
    '死信详情把失败原因与失败详情分开呈现',
    deadLetterDetail.includes('失败原因') &&
      deadLetterDetail.includes('失败详情') &&
      deadLetterDetail.includes('字段校验不通过，请核对并修改报文后重投') &&
      deadLetterDetail.includes('HTTP 200 result=-2') &&
      deadLetterDetail.includes('SO-20261107-1') &&
      deadLetterDetail.includes('99.5'),
    deadLetterDetail.slice(0, 200),
  );
  check(
    '失败详情可展开且可复制',
    (await page.locator('.ant-drawer-body .ant-typography-expand:has-text("展开完整详情")').count()) ===
      1 &&
      (await page.locator('.ant-drawer-body .ant-typography-copy').count()) === 1,
  );
  await page.locator('.ant-drawer-body .ant-typography-expand:has-text("展开完整详情")').click();
  await page.waitForTimeout(200);
  check(
    '失败详情展开后可继续操作',
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

  /* 903：改完仍失败 —— 原因回写到记录，原始报文不被待投覆盖 */
  const row903 = page.locator('.ant-table-tbody tr.ant-table-row').nth(2);
  await row903.locator('button:has-text("详情")').click();
  await page.waitForSelector('.ant-drawer-title:has-text("死信详情")');
  const detail903 = await page.locator('.ant-drawer-body').innerText();
  check(
    '历史记录回填后原因清晰、原始详情仍在',
    detail903.includes('字段校验不通过，请核对并修改报文后重投') &&
      detail903.includes('历史记录原始报错') &&
      detail903.includes('"abc"') &&
      detail903.includes('88'),
    detail903.slice(0, 220),
  );
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
  const brokerCheck = posted.findLast(
    (item) => item.method === 'POST' && item.path === '/dbz/kafka-broker/check',
  );
  check('检测发出 POST 且带记录ID', brokerCheck?.body?.id === '401', JSON.stringify(brokerCheck?.body));
  const checkMessage = page.locator('.ant-message-notice-content:has-text("连接正常")').first();
  let checkMessageText = '';
  try {
    await checkMessage.waitFor({ state: 'visible', timeout: 3000 });
    checkMessageText = await checkMessage.innerText();
  } catch {
    // 保留空文案，交给下面的断言报错
  }
  check(
    '检测成功给出提示（含耗时单位）',
    checkMessageText.includes('12 个 topic') && checkMessageText.includes('耗时 34 ms'),
    checkMessageText,
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
  await cancelFormModal({ expectDirty: true });

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
  await waitNoVisibleModal();
  check('保存后弹窗关闭', (await page.locator('.ant-modal-wrap:visible').count()) === 0);

  const createCall = posted.findLast((item) => item.method === 'POST' && item.path === '/dbz/kafka-connect');
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
  // 只回填、没改：取消不应打扰用户
  await cancelFormModal();

  /* 4. 上游连接器 */
  await page.click('.ant-menu-item:has-text("上游连接器")');
  await page.waitForSelector('text=订单库增量');
  check('上游列表渲染', (await page.locator('tbody tr.ant-table-row').count()) === 2);
  check('上游「所属集群」翻译成名称', (await page.locator('text=生产集群-华东').count()) > 0);
  const orderRow = page.locator('tbody tr.ant-table-row', { hasText: '订单库增量' });
  check(
    '空备注统一显示 --',
    (await orderRow.locator('td').nth(5).innerText()).trim() === '--',
    await orderRow.locator('td').nth(5).innerText(),
  );
  const userRow = page.locator('tbody tr.ant-table-row', { hasText: '用户库增量' });
  check(
    '19 位 ID 在列表里截断',
    (await userRow.innerText()).includes('#192837…6789'),
    await userRow.innerText(),
  );
  // 删除说明必须与 README / 服务端 before_del 一致：先注销、再删记录
  await orderRow.getByRole('button', { name: '删除' }).click();
  const sourceDeleteConfirm = page.locator('.ant-modal-confirm:visible');
  await sourceDeleteConfirm.waitFor({ state: 'visible', timeout: 2000 });
  check(
    '上游删除说明与 README 一致',
    (await sourceDeleteConfirm.innerText()).includes('先从集群注销'),
    await sourceDeleteConfirm.innerText(),
  );
  // antd 会在两个汉字之间插入空格（「取 消」），按 role 名字匹配不到，取第一个按钮
  await sourceDeleteConfirm.locator('.ant-modal-confirm-btns .ant-btn').first().click();
  await waitNoVisibleModal();
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

  // 必填参数没补齐就点保存：应该被 Form 拦下，而不是等集群部署失败
  const sourcePostsBefore = posted.filter(
    (item) => item.method === 'POST' && item.path === '/dbz/source-connector',
  ).length;
  await visibleModal().locator('.ant-modal-footer .ant-btn-primary').click();
  await page.waitForTimeout(300);
  const missingFieldErrors = await visibleModal().locator('.ant-form-item-explain-error').allInnerTexts();
  check(
    '缺少必填参数时保存被拦截',
    missingFieldErrors.some((text) => text.includes('还缺必填项')),
    missingFieldErrors.join(' | '),
  );
  check(
    '必填校验失败不发保存请求',
    posted.filter((item) => item.method === 'POST' && item.path === '/dbz/source-connector')
      .length === sourcePostsBefore,
  );

  // 非法 JSON 也必须拦在提交前；恢复合法文本后才能切回图形化
  const originalConfigJson = await visibleModal().locator('.dbz-json-editor').inputValue();
  await visibleModal().locator('.dbz-json-editor').fill('{"connector.class": ');
  await visibleModal().locator('.ant-modal-footer .ant-btn-primary').click();
  await page.waitForTimeout(300);
  const jsonErrors = await visibleModal().locator('.ant-form-item-explain-error').allInnerTexts();
  check(
    '非法 JSON 保存被拦截',
    jsonErrors.some((text) => text.includes('不是合法的 JSON')),
    jsonErrors.join(' | '),
  );
  await visibleModal().locator('.dbz-json-editor').fill(originalConfigJson);

  await visibleModal().locator('.ant-segmented-item:has-text("图形化")').click();
  await page.waitForTimeout(150);
  await cancelFormModal({ expectDirty: true });
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
  await waitNoVisibleModal();
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
  await visibleModal().last().locator('.ant-modal-footer .ant-btn').first().click();
  await waitNoVisibleModal();
  await page.waitForTimeout(400);
  await page.screenshot({ path: `${SHOTS}/04e-snapshot-cancel.png` });

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
  await cancelFormModal({ expectDirty: true });

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
  // Navigate 在渲染时同步发生，但给浏览器一次落地机会再断言 URL
  await page.waitForFunction(() => window.location.pathname === '/', undefined, { timeout: 3000 }).catch(() => null);
  check('未知路由回落总览', page.url().endsWith('/'), page.url());
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
