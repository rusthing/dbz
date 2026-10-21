# dbz-web

`dbz-svr` 的 Web 控制台：管理 Kafka Connect 集群、上游/下游连接器，并对集群上真实运行的连接器做运维。

## 技术栈

| 关注点 | 选型 |
| --- | --- |
| 构建 | Vite 6 |
| 框架 | React 18 + TypeScript 5（`strict` 全开） |
| 组件库 | Ant Design 5 |
| 路由 | React Router 6 |
| 时间 | dayjs |
| 状态 | 只用 React 自身的 `useState` / 自定义 Hook，不引入状态库 |

## 快速开始

```bash
cd dbz/dbz-web
npm install
npm run dev          # http://localhost:5174
```

开发态由 Vite 把 `/dbz/**` 反代到 `http://127.0.0.1:9006`（即 `dbz-svr.toml` 里的 `[web] port`），
因此**不需要后端开 CORS**。要连别的环境时改 `.env.development` 里的 `VITE_PROXY_TARGET`。

```bash
npm run typecheck    # 只做类型检查
npm run build        # 类型检查 + 产出 dist/
npm run preview      # 本地预览 dist/
npm run smoke        # 冒烟测试：真浏览器渲染每个页面并截图
```

## 冒烟测试

`scripts/smoke.mjs` 用 Playwright 起一个真实 Chromium，把 `/dbz/**` 全部拦下来喂夹具，
**不需要 dbz-svr、不碰数据库**就能把每个页面真实渲染一遍，并校验：

- 页面渲染、表格行数、状态标签、字典翻译；
- 写接口带了 `X-User-Id`、请求体是 camelCase 且已 trim；
- 表单默认值、编辑回填、提交后自动关闭；
- 布局体检：顶栏标题不被裁、表格不横向溢出、工具栏不换行。

```bash
npx playwright install chromium   # 首次，约 100MB
npm run smoke
```

脚本自己 `createServer()` 起 vite（默认 5199）并在结束时关闭，截图输出到系统临时目录
`dbz-web-smoke/shots`。控制台错误或页面异常都会让退出码非 0。

这套测试已经抓到过三类真问题，改样式/改表单时值得先跑一遍：

| 问题 | 现象 | 根因 |
| --- | --- | --- |
| 表单默认值不生效 | 新增集群时「启用」是关的 | `Modal` 子树懒挂载，`open` 变化当次 effect 里 `form` 未连接，`setFieldsValue` 静默失效 → 改用 `afterOpenChange` |
| 顶栏标题被裁 | 标题上半截看不见 | `.ant-layout-header` 自带 `line-height: 64px`，撑破了 60px 的头部 |
| 表格右列被压掉 | 「更新时间」只剩半截 | `scroll.x` 大于容器宽度时，右侧固定列会盖住前一列 |

## 目录

```
src/
├── api/                 # 与 dbz-api 的 DTO/VO 一一对应
│   ├── client.ts        # fetch 封装：拆 Ro<T> 信封、补 X-User-Id、统一抛 ApiError
│   ├── types.ts         # Ro / PageRx / VO / 请求体
│   ├── kafka-connect.ts # /dbz/kafka-connect
│   ├── source-connector.ts
│   ├── sink-connector.ts
│   └── kafka-connector.ts  # /dbz/kafka-connector（远端运维）
├── components/          # StatusTag / EnabledTag / JsonEditor
├── dic/                 # 三个状态枚举的整数判别式 → 文案/颜色
├── hooks/               # usePagedQuery / useAsync / useOptions
├── layouts/AppLayout.tsx
├── pages/
│   ├── Dashboard.tsx            # 总览 + 异常清单
│   ├── kafka-connect/           # 集群 CRUD
│   ├── source-connector/        # 上游 CRUD
│   ├── sink-connector/          # 下游 CRUD
│   └── remote-connector/        # 远端连接器/插件、重启
└── utils/format.ts
scripts/smoke.mjs        # 冒烟测试
```

## 接口约定

### 响应信封

所有接口返回 `Ro<T>`，`client.ts` 在 `result !== 1` 时直接抛 `ApiError`，业务代码只处理「拿到数据 / 捕获异常」：

```jsonc
{ "result": 1, "msg": "查询成功", "timestamp": 1730000000000, "extra": { } }
```

`result` 取值：`1` 成功 / `-1` 参数错误 / `-2` 警告 / `-3` 失败。

### 分页

统一走 `#[router(crud)]` 生成的 `/page` 接口：

| 参数 | 说明 |
| --- | --- |
| `_page` | 页码，**从 1 开始** |
| `_size` | 页大小，服务端默认 10 |
| `_keyword` | 多列模糊匹配（DAO 的 `LIKE_COLUMNS`） |
| `_orderBy` | 排序，`字段:desc` 逗号分隔。**必须用 snake_case**（`update_ms:desc`），底层是 `Expr::col` 直接拼列名 |
| 其余字段 | 与 QueryDto 同名，走等值匹配 |

返回 `extra: { pageNum, total, list }`。

### 写接口的请求头

`#[ctrl]` 生成的 `add` / `modify` / `save` 从 `X-User-Id` 读取当前用户写入审计字段，缺失直接报参数错误。
前端在 `client.ts` 里统一补上（值取 `VITE_USER_ID`，默认 `1`）；接入统一登录后改成从登录态读取即可。

### 字典

服务端 `#[dic]` 枚举序列化为**整数判别式**，前端靠 `src/dic` 翻译：

| 枚举 | 语义 | 落库 |
| --- | --- | --- |
| `KafkaConnectStatus` | 集群自身是否可达 | 是 |
| `ConnectorRunStatus` | 检查任务对连接器得出的结论 | 是 |
| `KafkaConnectorStatus` | 远端连接器此刻的实时状态 | 否（来自 Kafka Connect REST） |

改后端枚举时必须同步 `src/dic/index.ts`，否则页面显示「未知状态」。

## 页面与接口对照

| 页面 | 用到的接口 |
| --- | --- |
| 总览 | 三张表的 `/list`，在浏览器里算统计与异常 |
| Kafka Connect | `/dbz/kafka-connect` 的 `/page`、`POST`、`PUT`、`DELETE /{id}` |
| 上游连接器 | `/dbz/source-connector` 同上 |
| 下游连接器 | `/dbz/sink-connector` 同上 |
| 远端连接器 | `GET /dbz/kafka-connector/plugins`、`GET /list`、`POST /restart`、`POST /task/restart` |

## 已知限制

- **大整数精度**：`#[vo]` 生成的 `id` / `*_ms` 字段是裸 `u64`，雪花 ID 超过 `2^53` 后 `JSON.parse`
  会丢精度。前端全程把 ID 当不透明值传递（不做算术、不回显计算），但严格来说需要后端在 VO 上补
  `#[serde(with = "u64_serde")]`（序列化为字符串）才能彻底解决。改动前请先确认 `dbz-api-client` 的兼容性。
- **健康检查周期**：页面上的状态是「上一轮检查的结论」，不是实时探测结果；要看实时状态请用「远端连接器」。
- **删除与注销是两件事**：删除记录不会自动注销集群上的连接器，需要在「远端连接器」里单独处理
  （集群改地址/禁用时服务端会自动注销，见 `DbzKafkaConnectSvc::before_modify_hook`）。
