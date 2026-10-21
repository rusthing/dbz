/**
 * # 冒烟测试
 *
 * 用 Playwright 起一个真实的 Chromium，把 `/dbz/**` 全部拦下来喂夹具，因此**不需要 dbz-svr**、
 * 也不碰数据库，就能把每个页面真实渲染一遍，并校验：
 *
 * - 页面能渲染、表格行数与状态标签正确；
 * - 写接口带了 `X-User-Id`、请求体是 camelCase；
 * - 表单默认值、编辑回填、提交后关闭；
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
const clusters = [
  {
    id: '101',
    name: '生产集群-华东',
    url: 'http://10.0.0.11:8083',
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
    runStatus: 3,
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
    tasks: [],
  },
];

const plugins = [
  { class: 'io.debezium.connector.mysql.MySqlConnector', type: 'source', version: '2.5.0.Final' },
  { class: 'io.debezium.connector.jdbc.JdbcSinkConnector', type: 'sink', version: '2.5.0.Final' },
];

const pageOf = (list) => ro({ pageNum: 1, total: list.length, list });

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

page.on('pageerror', (error) => pageErrors.push(String(error)));
page.on('console', (message) => {
  if (message.type() === 'error') {
    consoleErrors.push(message.text());
  }
});

await page.route('**/dbz/**', async (route) => {
  const request = route.request();
  const path = new URL(request.url()).pathname;

  if (request.method() !== 'GET') {
    posted.push({ method: request.method(), path, headers: request.headers(), body: request.postDataJSON() });
    await route.fulfill({ json: { result: 1, msg: '成功', timestamp: Date.now() } });
    return;
  }

  if (path === '/dbz/kafka-connect/page') return route.fulfill({ json: pageOf(clusters) });
  if (path === '/dbz/kafka-connect/list') return route.fulfill({ json: ro(clusters) });
  if (path === '/dbz/source-connector/page') return route.fulfill({ json: pageOf(sources) });
  if (path === '/dbz/source-connector/list') return route.fulfill({ json: ro(sources) });
  if (path === '/dbz/sink-connector/page') return route.fulfill({ json: pageOf(sinks) });
  if (path === '/dbz/sink-connector/list') return route.fulfill({ json: ro(sinks) });
  if (path === '/dbz/kafka-connector/list') return route.fulfill({ json: ro(connectors) });
  if (path === '/dbz/kafka-connector/plugins') return route.fulfill({ json: ro(plugins) });
  return route.fulfill({ json: ro(null) });
});

try {
  /* 1. 总览：统计与异常清单 */
  await page.goto(`${base}/`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.ant-card-head-title:has-text("异常清单")');
  check('总览渲染异常清单', await page.locator('.ant-card-head-title:has-text("异常清单")').first().isVisible());
  const anomalyRows = await page.locator('tbody tr.ant-table-row').count();
  check('异常清单 3 行（集群/上游/下游各 1）', anomalyRows === 3, `实际 ${anomalyRows}`);
  await page.screenshot({ path: `${SHOTS}/01-dashboard.png`, fullPage: true });

  /* 2. 集群列表 */
  await page.click('.ant-menu-item:has-text("Kafka Connect")');
  await page.waitForSelector('text=生产集群-华东');
  const clusterRows = await page.locator('tbody tr.ant-table-row').count();
  check('集群列表 2 行', clusterRows === 2, `实际 ${clusterRows}`);
  check('集群状态标签渲染', (await page.locator('text=连接失败').count()) > 0);

  const headerBox = await page.locator('.dbz-header-title').boundingBox();
  check(
    '顶栏标题完整可见',
    Boolean(headerBox) && headerBox.y >= 0 && headerBox.y + headerBox.height <= 60,
    JSON.stringify(headerBox),
  );
  await layoutCheck(page, '集群');
  await page.screenshot({ path: `${SHOTS}/02-kafka-connect.png`, fullPage: true });

  /* 3. 新增：默认值、请求头、请求体、提交后关闭 */
  await page.click('button:has-text("新增集群")');
  await page.waitForSelector('.ant-modal-wrap', { state: 'visible' });
  await page.waitForTimeout(450);
  const switchChecked = await page.getByRole('switch').getAttribute('aria-checked');
  check('新增表单默认启用', switchChecked === 'true', String(switchChecked));

  await page.fill('#name', '冒烟测试集群');
  await page.fill('#url', 'http://127.0.0.1:8083');
  await page.screenshot({ path: `${SHOTS}/03-modal.png` });
  await page.click('.ant-modal-footer .ant-btn-primary');
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });
  check('保存后弹窗关闭', !(await page.locator('.ant-modal-wrap').isVisible().catch(() => false)));

  const createCall = posted.find((item) => item.method === 'POST' && item.path === '/dbz/kafka-connect');
  check('新增集群发出 POST', Boolean(createCall));
  check('写接口带 X-User-Id', createCall?.headers['x-user-id'] === '1', String(createCall?.headers['x-user-id']));
  check(
    '请求体为 camelCase 且已 trim',
    createCall?.body?.name === '冒烟测试集群' && createCall?.body?.url === 'http://127.0.0.1:8083',
    JSON.stringify(createCall?.body),
  );

  /* 3b. 编辑回填 */
  await page.locator('tbody tr.ant-table-row').first().locator('.ant-btn-link').first().click();
  await page.waitForSelector('.ant-modal-wrap', { state: 'visible' });
  await page.waitForTimeout(450);
  check('编辑表单回填名称', (await page.inputValue('#name')) === '生产集群-华东', await page.inputValue('#name'));
  check('编辑表单回填启用', (await page.getByRole('switch').getAttribute('aria-checked')) === 'true');
  await page.screenshot({ path: `${SHOTS}/09-edit.png` });
  await page.locator('.ant-modal-footer .ant-btn').first().click();
  await page.waitForSelector('.ant-modal-wrap', { state: 'hidden', timeout: 5000 });

  /* 4. 上游连接器 */
  await page.click('.ant-menu-item:has-text("上游连接器")');
  await page.waitForSelector('text=订单库增量');
  check('上游列表渲染', (await page.locator('tbody tr.ant-table-row').count()) === 2);
  check('上游「所属集群」翻译成名称', (await page.locator('text=生产集群-华东').count()) > 0);
  await layoutCheck(page, '上游');
  await page.screenshot({ path: `${SHOTS}/04-source.png`, fullPage: true });

  /* 5. 下游连接器 */
  await page.click('.ant-menu-item:has-text("下游连接器")');
  await page.waitForSelector('text=订单同步到数仓');
  check('下游列表渲染', (await page.locator('tbody tr.ant-table-row').count()) === 1);
  await layoutCheck(page, '下游');
  await page.screenshot({ path: `${SHOTS}/05-sink.png`, fullPage: true });

  /* 6. 远端连接器：列表、未纳管连接器、任务展开与重启 */
  await page.goto(`${base}/remote-connector?kafkaConnectId=101`, { waitUntil: 'networkidle' });
  await page.waitForSelector('text=mysql-orders-source');
  check('远端连接器列表渲染', (await page.locator('tbody tr.ant-table-row').count()) === 2);
  check('未纳管连接器可见', (await page.locator('text=manual-test-connector').count()) > 0);
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
