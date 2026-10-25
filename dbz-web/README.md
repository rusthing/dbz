# dbz-web

`dbz-svr` 的 Web 控制台：管理 Kafka Connect 集群、上游/下游连接器，并对集群上真实运行的连接器做运维。

## 技术栈

| 关注点 | 选型 |
| --- | --- |
| 包管理 | pnpm 12（`corepack pnpm`，锁文件是 `pnpm-lock.yaml`） |
| 构建 | Vite 6 |
| 框架 | React 18 + TypeScript 5（`strict` 全开） |
| 组件库 | Ant Design 5 |
| 路由 | React Router 6（`BrowserRouter`，history 模式） |
| 时间 | dayjs |
| 状态 | 只用 React 自身的 `useState` / 自定义 Hook，不引入状态库 |

仓库里**只认 pnpm**：`pnpm-lock.yaml` 是唯一锁文件，不要再生成 `package-lock.json` / `yarn.lock`。

## 快速开始

```bash
cd dbz/dbz-web
corepack pnpm install     # 首次；corepack 会按 packageManager 取到 pnpm 12
corepack pnpm dev         # http://localhost:5174
```

开发态由 Vite 把 `/dbz/**` 反代到 `http://127.0.0.1:9006`（即 `dbz-svr.toml` 里的 `[web] port`），
因此**不需要后端开 CORS**（框架只有配了 `[web.cors]` 才会放开跨域）。要连别的环境时改
`.env.development` 里的 `VITE_PROXY_TARGET`。

```bash
corepack pnpm typecheck    # 只做类型检查
corepack pnpm build        # 类型检查 + 产出 dist/
corepack pnpm preview      # 本地预览 dist/
corepack pnpm smoke        # 冒烟测试：真浏览器渲染每个页面并截图
```

## 构建与部署

### 产物

```bash
corepack pnpm install --frozen-lockfile
corepack pnpm build        # = tsc --noEmit && vite build
```

产物是 `dist/` 下的纯静态文件（`index.html` + `assets/*.js|css`），**`dbz-svr` 不托管前端**，
也没有把前端编进二进制，所以部署 = 「静态托管 + 网关反代」两件事，两者必须同源。

### 静态托管要求

| 要求 | 原因 |
| --- | --- |
| 未命中的路径回落 `index.html` | 路由是 `BrowserRouter`（history 模式），刷新 `/dbz-web/kafka-connect` 时服务器上没有这个文件 |
| `/dbz-web/assets/*` 长缓存 | 文件名带内容哈希，可 `max-age=31536000, immutable` |
| `index.html` 不缓存 | 它引用的是当次构建的哈希文件 |

### 部署拓扑（/dbz-web + /dbz-svr）

本次两个前缀挂在同一个域名端口下，彼此同源，所以**不用给 dbz-svr 配 `[web.cors]`**：

```
http://xxx:xxx/dbz-web/…       → dist/ 静态文件
http://xxx:xxx/dbz-svr/dbz/…   → dbz-svr:9006 的 /dbz/…
```

两个 `/dbz` 长得像，但不是一个东西，这里最容易写错：

| 片段 | 谁加的 | 含义 |
| --- | --- | --- |
| `/dbz-svr` | 网关 | 后端服务的路径前缀，代理时必须**剥掉** |
| `/dbz` | `dbz-svr` 自己 | `#[router]` 按结构体名生成（`DbzKafkaConnectRouter` → `/dbz/kafka-connect`），必须原样透传 |

所以 `VITE_API_BASE=/dbz-svr`，业务代码里仍是 `/dbz/kafka-connect`，最终请求
`/dbz-svr/dbz/kafka-connect/page`。若把 `VITE_API_BASE` 写成 `/dbz-svr/dbz`，就会变成
`/dbz-svr/dbz/dbz/...`，是最常见的 404。

### 前端要改的配置

`vite.config.ts` 的 `base`、`index.html` 的 favicon、`main.tsx` 的 `basename` 都已经跟着变量走，
**只需要改 `.env.production`**：

| 变量 | 本次取值 | 作用 |
| --- | --- | --- |
| `VITE_PUBLIC_BASE` | `/dbz-web` | 一次管三处：Vite `base`（资源走 `/dbz-web/assets/*`）、favicon（`%BASE_URL%`）、`BrowserRouter` 的 `basename`（路由变 `/dbz-web/kafka-connect`） |
| `VITE_API_BASE` | `/dbz-svr` | 接口前缀，拼在 `/dbz/...` 前面 |
| `VITE_USER_ID` | `1` | 写接口的 `X-User-Id` 占位 |

三个都是**编译期**变量，改完必须重新 `build`。另外 `pnpm preview` 只起静态服务、不带反代，
用它预览生产包时接口会 404，属正常现象，要连后端得走真网关。

### 网关配置（nginx）

```nginx
server {
    listen 80;
    server_name xxx;

    # 把 dist/ 的内容放到与 URL 前缀同名的目录，就能直接用 root：
    #   /srv/www/dbz-web/   ←   dist/* 的内容
    root /srv/www;

    # 1) 后端：末尾这个 "/" 是关键，它负责把 /dbz-svr 剥掉。
    #    漏掉末尾的 /，后端收到的是 /dbz-svr/dbz/...，不命中任何路由 → 404
    location /dbz-svr/ {
        proxy_pass http://127.0.0.1:9006/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # 2) 前端资源：文件名带哈希，长缓存
    location /dbz-web/assets/ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    # 3) 前端页面：未命中回落 index.html，index.html 本身不缓存
    location /dbz-web/ {
        try_files $uri $uri/ /dbz-web/index.html;
        add_header Cache-Control "no-cache";
    }

    # 不带末尾斜杠的入口重定向，否则页面里的相对路径会算错
    location = /dbz-web {
        return 301 /dbz-web/;
    }
}
```

两个坑：

- **别写成 `location /dbz`（不带末尾斜杠）**：它会同时匹配 `/dbz-web/` 和 `/dbz-svr/`，先命中的那条会把另一个吃掉。
- 写接口要 `X-User-Id`，网关默认会透传自定义请求头，别额外加过滤规则把它清掉。

### 部署后自检

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://xxx:xxx/dbz-web/                          # 200
curl -s http://xxx:xxx/dbz-svr/dbz/kafka-connect/list | head -c 200                        # 应是 JSON，不是 HTML
```

第二条如果返回 HTML（通常是 `index.html`），说明 `/dbz-svr/**` 被前端回落吃掉了。

| 现象 | 原因 |
| --- | --- |
| 白屏，控制台 `assets/*.js` 404 | `VITE_PUBLIC_BASE` 没设，资源还按根路径找 |
| 刷新深层路由 404 | 没配 SPA 回落，或回落到 `/index.html` 而不是 `/dbz-web/index.html` |
| 页面能开、接口全 404 | `VITE_API_BASE` 没设，或 `proxy_pass` 少了末尾 `/` |
| 接口返回 200 但内容是 HTML | `/dbz-svr/**` 被 SPA 回落吃掉 |
| 接口返回 400「参数错误」 | 网关把 `X-User-Id` 丢了 |
| 请求路径里出现 `/dbz/dbz/` | `VITE_API_BASE` 写成了 `/dbz-svr/dbz` |

## 冒烟测试

`scripts/smoke.mjs` 用 Playwright 起一个真实 Chromium，把 `/dbz/**` 全部拦下来喂夹具，
**不需要 dbz-svr、不碰数据库**就能把每个页面真实渲染一遍，并校验：

- 页面渲染、表格行数、状态标签、字典翻译、时间列格式化；
- **悬停状态弹出运行详情**：失败原因确实从 `runMsg` / trace 渲染出来了（漏传 `detail` 会 FAIL）；
- 写接口带了 `X-User-Id`、请求体是 camelCase 且已 trim；
- 表单默认值、编辑回填、提交后自动关闭；
- 布局体检：顶栏标题不被裁、表格不横向溢出、工具栏不换行。

```bash
./node_modules/.bin/playwright install chromium chromium-headless-shell   # 首次，约 100MB
corepack pnpm smoke
```

脚本自己 `createServer()` 起 vite（默认 5199）并在结束时关闭，截图输出到系统临时目录
`dbz-web-smoke/shots`。控制台错误或页面异常都会让退出码非 0。

夹具里的 ID 与时间戳**必须用字符串**（后端 `u64` 就长这样，见下文「ID 与时间戳」），
写成数字会让精度与 `dayjs` 解析这两类问题在冒烟里看不出来。

这套测试已经抓到过四类真问题，改样式/改表单时值得先跑一遍：

| 问题 | 现象 | 根因 |
| --- | --- | --- |
| 表单默认值不生效 | 新增集群时「启用」是关的 | `Modal` 子树懒挂载，`open` 变化当次 effect 里 `form` 未连接，`setFieldsValue` 静默失效 → 改用 `afterOpenChange` |
| 顶栏标题被裁 | 标题上半截看不见 | `.ant-layout-header` 自带 `line-height: 64px`，撑破了 60px 的头部 |
| 表格右列被压掉 | 「更新时间」只剩半截 | `scroll.x` 大于容器宽度时，右侧固定列会盖住前一列 |
| 更新时间显示成 1759 年 | 时间列全是 `1759-11-30` | `updateMs` 线上是字符串，`dayjs("1760000000000")` 按日期字符串解析 → `formatMs` 先 `Number()` |

## 目录

```
src/
├── api/                 # 与 dbz-api 的 DTO/VO 一一对应
│   ├── client.ts        # fetch 封装：拆 Ro<T> 信封、补 X-User-Id、统一抛 ApiError
│   ├── types.ts         # Ro / PageRx / VO / 请求体 / Id（u64 的线上形态）
│   ├── kafka-broker.ts  # /dbz/kafka-broker（消息集群 CRUD + 检测）
│   ├── kafka-connect.ts # /dbz/kafka-connect
│   ├── source-connector.ts
│   ├── sink-connector.ts
│   └── kafka-connector.ts  # /dbz/kafka-connector（远端运维）
├── components/          # StatusTag / EnabledSwitch / JsonEditor / ConnectorConfigEditor
├── dic/                 # 状态枚举的整数判别式 → 文案/颜色
├── hooks/               # usePagedQuery / useAsync / useOptions
├── layouts/AppLayout.tsx
├── pages/
│   ├── Dashboard.tsx            # 总览 + 异常清单
│   ├── kafka-broker/            # 消息集群 CRUD + 连通性检测
│   ├── kafka-connect/           # 集群 CRUD
│   ├── source-connector/        # 上游 CRUD + 重新同步
│   ├── sink-connector/          # 下游 CRUD
│   └── remote-connector/        # 远端连接器/插件、重启、停止/恢复、重置位点
└── utils/format.ts
scripts/smoke.mjs        # 冒烟测试
```

## 接口约定

### 响应信封

所有接口返回 `Ro<T>`，`client.ts` 在 `result !== 1` 时直接抛 `ApiError`，业务代码只处理「拿到数据 / 捕获异常」：

```jsonc
{ "result": 1, "msg": "查询成功", "timestamp": "1730000000000", "extra": { } }
```

`result` 取值：`1` 成功 / `-1` 参数错误 / `-2` 警告 / `-3` 失败。

### ID 与时间戳

框架把 `u64` 换成 `robotech::api::U64`，**序列化成字符串**：

```jsonc
{ "id": "1928374650123456789", "createrId": "1", "createMs": "1760000000000" }
```

- 19 位雪花 ID 超过 `2^53`，用字符串是为了躲开 `JSON.parse` 丢精度；`timestamp` / `*Ms` 同理。
- 前端统一用 `Id`（即 `string`）声明这类字段，**不做 `Number()`、不做算术**，只原样透传。
  毫秒时间戳在展示前可以安全地 `Number()`（约 `1.7e12`，远小于 `2^53`）。
- 反序列化只认字符串：把 ID 当 JSON 数字发回去，后端直接判参数错误。

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
| `KafkaBrokerStatus` | 消息集群（broker）是否可达 | 是 |
| `KafkaConnectStatus` | 集群自身是否可达 | 是 |
| `ConnectorRunStatus` | 检查任务对连接器得出的结论 | 是 |
| `KafkaConnectorStatus` | 远端连接器此刻的实时状态 | 否（来自 Kafka Connect REST） |

改后端枚举时必须同步 `src/dic/index.ts`，否则页面显示「未知状态」。

## 连接器配置：图形化与 JSON

上游、下游表单的配置区都是 `components/ConnectorConfigEditor.tsx`，**两种写法共享同一份 JSON 文本**：
图形化只是把它渲染成表单，改动立刻写回；图形化没列出的键切到 JSON 还在，不会丢。

| 方向 | 图形化分组 | 模板 |
| --- | --- | --- |
| 上游（`kind="source"`） | 基本信息 / 源库连接 / 捕获范围 / 信号 / 快照 / 序列化 | MySQL 模板、PostgreSQL 模板 |
| 下游（`kind="sink"`） | 基本信息 / 目标库连接 / 写入方式 / 序列化 / 容错 | JDBC Sink 模板、JDBC Sink 模板（upsert） |

模板的动作按意图分四类，**不是无脑「只补空键」**（早先那样写，从 MySQL 模板点 PostgreSQL 模板时
`connector.class` 一个都不改，部署到集群才发现还是 MySQL）：

| 类别 | 行为 | 例子 |
| --- | --- | --- |
| `overwrite` | 直接改写 | `connector.class`、`insert.mode` |
| `stale` | 直接删掉（另一家的专属键，留着是无效参数） | 换到 PostgreSQL 时删 `database.include.list` |
| `port` | 为空、或还是模板写的默认端口才改 | 3306 ⇄ 5432；自己填的 3307 不会被改回去 |
| `fill` | 只补空键 | 序列化 4 项、`snapshot.mode` |

捕获范围按方言只显示一份：`connector.class` 是 MySQL/MariaDB 时只出现 `database.include.list`，
是 PostgreSQL 时只出现 `schema.include.list` 与 `plugin.name`；认不出连接器类（自定义类）时全部显示，
不做猜测。

必填项之外还有一条「前后矛盾」提醒：下游选了 `insert.mode=upsert/update` 却把主键来源留成
`record_value` 又不填 `primary.key.fields` 时，表单会直接点出来——这个错原本要到部署阶段才暴露。

## 页面与接口对照

| 页面 | 用到的接口 |
| --- | --- |
| 总览 | 四张表的 `/list`（含 Kafka 集群），在浏览器里算统计与异常；异常清单里消息集群排在集群之前一起列 |
| Kafka 集群 | `/dbz/kafka-broker` 的 `/page`、`POST`、`PUT`、`DELETE /{id}` + `POST /check` |
| Kafka Connect | `/dbz/kafka-connect` 的 `/page`、`POST`、`PUT`、`DELETE /{id}` |
| 上游连接器 | `/dbz/source-connector` 同上 + `GET /snapshot/preview`、`POST /snapshot` |
| 下游连接器 | `/dbz/sink-connector` 同上 |
| 远端连接器 | `GET /dbz/kafka-connector/plugins`、`GET /list`、`POST /restart`、`POST /task/restart`、`PUT /stop`、`PUT /resume`、`DELETE /offsets`、`DELETE /dbz/kafka-connector` |

## 状态悬停：运行详情

集群、Kafka Connect、上游/下游连接器列表里的**状态标签可以悬停**（虚线下划 + 问号光标），
弹出的是「这个状态是怎么得出的」：

| 页面 | 弹层内容 | 失败原因来自 |
| --- | --- | --- |
| Kafka 集群 | 地址、认证方式（协议 + 机制 + 账号）、结论时间 | `dbz_kafka_broker.run_msg`（检测时写入） |
| Kafka Connect | 地址、结论时间 | `dbz_kafka_connect.run_msg`（健康检查写入） |
| 上游 / 下游连接器 | 连接器名、所属集群（下游还有上游）、结论时间 | `dbz_{source,sink}_connector.run_msg`（健康检查写入） |
| 远端连接器 | 类名、worker、任务清点、停止/暂停说明 | `GET /connectors?expand=status` 带回的 trace，不额外发请求 |

`run_msg` 只在**失败时**有值（成功时为空），内容取自真正的原因：探测连不上时是请求错误，
部署被拒时是 Kafka Connect 的响应体（会指出哪个参数不合法），状态异常时是异常 trace 的
首行（例如 `Access denied for user 'dbz'`）。写入前统一压成一行并截到 500 字符
（`dbz-svr/src/utils/error_msg_utils.rs`），所以不会因为 Java 栈太长把列写爆。

状态没变、原因也没变时不写库，`update_ms` 因此表示「进入当前状态/原因的时间」。

## 删除到底会不会注销连接器

会。**删除记录 = 先注销集群上的连接器，再删库**，两者由同一个接口完成，不存在「删了记录、
连接器还在跑」的中间态：

| 操作 | 服务端行为 |
| --- | --- |
| 删上游 / 下游连接器 | `before_del_by_id_hook` 先从集群上 `DELETE /connectors/{name}`，成功才删记录；注销失败则**删除失败**（fail-closed，宁可删不掉也不留幽灵连接器） |
| 删 Kafka Connect 集群 | 名下还有连接器时直接拒绝删除（先删记录，外键也是 `restrict`），所以走不到需要注销的分支 |
| 改集群地址 / 禁用集群 | `before_modify_hook` 先注销该集群上的全部连接器，再写库；禁用时注销失败会拦下整个请求 |
| 禁用单个连接器 | 同样先注销再写库，避免「库里已禁用、集群上还在跑」 |

两个例外，都是「取不到集群地址」这一种情况：记录所属集群的 `url` 为空或已被改走，服务端
查不到地址就跳过注销并记一条 warn（按「从未部署过」处理）。

所以「注销」这个动作在前端**没有独立入口**：要让连接器停掉但保留记录，用「禁用」
（`enabled = false`）；要彻底清理，直接删除。

「远端连接器」页是直接对集群下手的地方，除了重启与删除，还有两个**不改库**的动作：

| 操作 | Kafka Connect 接口 | 影响 |
| --- | --- | --- |
| 停止 / 恢复 | `PUT /connectors/{name}/stop`、`/resume` | 保留配置与位点，恢复后从原处继续；需要 Kafka 3.5+ |
| 重置位点 | 先 `stop` 再 `DELETE /connectors/{name}/offsets` | 位点清空即「从没跑过」，`snapshot.mode=initial` 时整表快照；**不可撤销** |

停在「已停止」的连接器在健康检查里会被记为异常并告警，但**不会被自动重启** ——
自动修复只处理 `FAILED`，人工动作不能被机器覆盖。要让 dbz 不再管这条连接器，
去上游/下游列表把启用开关关掉。

## 已知限制

- **健康检查周期**：页面上的状态是「上一轮检查的结论」，不是实时探测结果；要看实时状态请用「远端连接器」。
- **没有登录鉴权**：`X-User-Id` 由构建期变量写死（默认 `1`），审计字段在接入统一登录前都记成同一个人，
  只适用于内网/开发环境。
- **子路径靠变量切**：`VITE_PUBLIC_BASE` 一次驱动 `base` / favicon / `basename` 三处；变量和网关前缀不一致就会出现
  白屏或刷新 404（对照「部署后自检」那张表）。
