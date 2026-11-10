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
| `VITE_AUTH_BASE` | `/admrs-svr` | adm-rs 认证接口前缀，登录 / 登出走 `/admrs-svr/adm/auth/...` |

这些都是**编译期**变量，改完必须重新 `build`。另外 `pnpm preview` 只起静态服务、不带反代，
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
- 表单默认值、编辑回填、提交后自动关闭；**改过字段再取消会先弹「放弃修改」确认**；
- 重新同步弹窗：两条通道切换会重新预检、切过去还能切回来、预检失败能原地重试；
- 布局体检：顶栏标题不被裁、表格不横向溢出、工具栏不换行；空值统一显示 `--`。

```bash
./node_modules/.bin/playwright install chromium chromium-headless-shell   # 首次，约 100MB
corepack pnpm smoke
# 系统不支持 Playwright 自带浏览器时，复用已有 Chromium：
# SMOKE_CHROME=/path/to/chrome-headless-shell corepack pnpm smoke
```

脚本自己 `createServer()` 起 vite（默认 5199）并在结束时关闭，截图输出到系统临时目录
`dbz-web-smoke/shots`。控制台错误或页面异常都会让退出码非 0。

夹具里的 ID 与时间戳**必须用字符串**（后端 `u64` 就长这样，见下文「ID 与时间戳」），
写成数字会让精度与 `dayjs` 解析这两类问题在冒烟里看不出来。

这套测试已经抓到过五类真问题，改样式/改表单时值得先跑一遍：

| 问题 | 现象 | 根因 |
| --- | --- | --- |
| 表单默认值不生效 | 新增集群时「启用」是关的 | `Modal` 子树懒挂载，`open` 变化当次 effect 里 `form` 未连接，`setFieldsValue` 静默失效 → 改用 `afterOpenChange` |
| 顶栏标题被裁 | 标题上半截看不见 | `.ant-layout-header` 自带 `line-height: 64px`，撑破了 60px 的头部 |
| 表格右列被压掉 | 「更新时间」只剩半截 | `scroll.x` 大于容器宽度时，右侧固定列会盖住前一列 |
| 更新时间显示成 1759 年 | 时间列全是 `1759-11-30` | `updateMs` 线上是字符串，`dayjs("1760000000000")` 按日期字符串解析 → `formatMs` 先 `Number()` |
| 再同步通道切换后选项消失 | 切到 Kafka 就再也切不回源库 | 预检回传的 `channels` 跟着入参 `channel` 收窄成一条，前端靠它画单选 → 服务端改成始终回传全部可用通道 |

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
│   ├── kafka-connector.ts  # /dbz/kafka-connector（集群连接器/插件查询与远端运维）
│   └── dead-letter.ts      # /dbz/dead-letter（死信查询、重投、废弃）
├── components/          # StatusTag / EnabledSwitch / JsonEditor / ConnectorConfigEditor
├── dic/                 # 状态枚举的整数判别式 → 文案/颜色
├── hooks/               # usePagedQuery / useAsync / useOptions / useCancelConfirm（脏表单确认）
├── layouts/AppLayout.tsx
├── pages/
│   ├── Dashboard.tsx            # 总览 + 异常清单
│   ├── control-panel/      # 控制面板：broker → Connect → 连接器的实时卡片树
│   ├── dead-letter/             # 死信列表 + 报文详情 + 重投/废弃
│   ├── kafka-broker/            # 消息集群 CRUD + 连通性检测
│   ├── kafka-connect/           # 集群 CRUD
│   ├── source-connector/        # 上游 CRUD + 重新同步
│   ├── sink-connector/          # 下游 CRUD
│   └── kafka-connector/         # 集群连接器与插件查询（只读）
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

所有业务请求都带 `Authorization: Bearer <token>`（登录 adm-rs 后拿到）；`#[ctrl]` 生成的
`add` / `modify` / `save` 仍从 `X-User-Id` 读取当前用户写审计字段，因此 `client.ts` 同时补上
登录结果里的真实 `userId`。登录态存在 `localStorage` 的 `dbz.auth.session`，HTTP 401 或业务码
`ADM00005` 会清会话并回登录页。

### 字典

服务端 `#[dic]` 枚举序列化为**整数判别式**，前端靠 `src/dic` 翻译：

| 枚举 | 语义 | 落库 |
| --- | --- | --- |
| `KafkaBrokerStatus` | 消息集群（broker）是否可达 | 是 |
| `KafkaConnectStatus` | 集群自身是否可达 | 是 |
| `ConnectorRunStatus` | 检查任务对连接器得出的结论 | 是 |
| `KafkaConnectorStatus` | 集群连接器此刻的实时状态 | 否（来自 Kafka Connect REST） |
| `DeadLetterStatus` | 死信处理状态：待处理 / 已重投 / 已废弃 / 已人工处理 | 是 |
| `DeadLetterFailureCategory` | DLQ 异常分类：数据/格式类 / 投递类 / 其它（由异常类名推导） | 是 |
| `DeadLetterConnectorType` | 死信所属连接器方向；DLQ 只对 sink 有效，恒为下游 | 是 |

改后端枚举时必须同步 `src/dic/index.ts`，否则页面显示「未知状态」。

### 展示约定

这些规则收敛在 `src/utils/format.ts`，新增页面/列时直接复用，别各写一套：

| 场景 | 约定 | 入口 |
| --- | --- | --- |
| 空值 | 一律 `--`（`null`、`undefined`、空串、纯空白），不再混用 `-` | `orDash` / `EMPTY_TEXT` |
| 时间 | `YYYY-MM-DD HH:mm:ss`；无效或非正数时间戳显示 `--` | `formatMs` |
| 时长 | `< 1s` 用 `N ms`，`< 1min` 用 `N.N s`，更长用 `N 分 N 秒` | `formatDuration` |
| 长 ID | 头 6 位 + `…` + 尾 4 位，完整值放 Tooltip / `title`；仍禁止 `Number()` | `shortId` |
| 数字 | 表格数字列加 `.dbz-num`（等宽数字），统计卡同样；数量带单位（`个 topic`、`台`） | `formatDuration` + CSS |
| 状态色 | `success` 正常、`warning` 需关注/未分配/已停止、`error` 失败、`default` 中性、`blue` 仅用于标识认证方式等非健康语义 | `src/dic` |
| 长文本 | 单行 `ellipsis` + `title`；异常栈等宽、`pre-wrap`、限定高度内部滚动 | `.dbz-ellipsis` / `.dbz-status-reason` |
| 错误文案 | 服务端 `msg` 为主，已知编码补一句「怎么办」；HTTP 状态兜底中文提示 | `errorMessage` |

### 错误码到提示

`client.ts` 抛出的 `ApiError` 保留 `result` / `code` / `status` / `detail`。展示层按以下顺序补提示：
业务编码命中优先（`RTW00001` 唯一键、`RTW00002` 关联对象不存在、`RTW00003` 仍有关联记录），
其次按 HTTP 状态（401 / 403 / 404 / 409 / 5xx），都没有时原样展示服务端文案。

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

**保存前会校验，不只是提示**（`checkConnectorConfig`）：配置必须是 JSON 对象（数组/`null` 不接受）、
当前方言的必填项齐全、`database.port` 在 1–65535、`tasks.max` 为正整数。必填与数值不满足时
`Form.Item` 直接拦下保存，不再等集群部署失败才反馈。

还有一条「前后矛盾」提醒：下游选了 `insert.mode=upsert/update` 却把主键来源留成
`record_value` 又不填 `primary.key.fields` 时，表单会直接点出来——这个错原本要到部署阶段才暴露。

文字长度与库列长度对齐（前端不再允许提交必被截断的值）：

| 字段 | 上限 | 对应列 |
| --- | --- | --- |
| 名称 | 50 | `name varchar(50)` |
| 连接器名称 | 50 | `connector_name varchar(50)` |
| 备注 | 50 | `remark varchar(50)` |
| Connect 地址 | 100 | `url varchar(100)` |
| broker 地址 | 500 | `bootstrap_servers varchar(500)` |

`signal.kafka.bootstrap.servers` 可以留空：保存时服务端会用这颗连接器所在 Connect 关联集群的地址
补上，并写进配置（下次编辑能看到）。这个键在 Debezium 里必填、**没有默认值，也不会沿用 worker 的
地址**，留空老版本直接 NPE、整条连接器起不来。自己填过的地址不会被改；关联的 Connect 没有选集群时
保存会直接报错，而不是存下一条注定起不来的配置。

## 页面与接口对照

| 页面 | 用到的接口 |
| --- | --- |
| 总览 | 四张表的 `/list`（含 Kafka 集群），在浏览器里算统计与异常；异常清单里消息集群排在集群之前一起列 |
| 控制面板 | 四张表的 `/list` + 每台 Connect 的 `GET /dbz/kafka-connector/list`（实时连接器）、`GET /dbz/kafka-connector/plugins`（插件）、`GET /dbz/kafka-connect/health`（Connect 自身健康）；`POST /restart`；上游/下游 `PUT`（启用/停用）；`POST /kafka-broker/check`（Kafka 卡片检测）、`POST /kafka-connect/detect`（Connect 卡片检测）、`POST /control-plane/connector-config/detect`（上/下游卡片一致性检测）、`POST /control-plane/connector-config/sync`（不一致双向同步） |
| Kafka 集群 | `/dbz/kafka-broker` 的 `/page`、`POST`、`PUT`、`DELETE /{id}` + `POST /check` |
| Kafka Connect | `/dbz/kafka-connect` 的 `/page`、`POST`、`PUT`、`DELETE /{id}` |
| 上游连接器 | `/dbz/source-connector` 同上 + `GET /snapshot/preview`、`POST /snapshot` |
| 下游连接器 | `/dbz/sink-connector` 同上 |
| 集群连接器 | `GET /dbz/kafka-connector/list`（集群上全部连接器）、`GET /dbz/kafka-connector/plugins`（已安装插件） |
| 死信 | `POST /dbz/dead-letter/sync`（立即从 DLQ 同步）、`GET /page`（连接器/状态/topic/时间筛选）、`GET /pending-count`（卡片角标）、`POST /retry`、`POST /edit-payload`、`POST /discard`、`POST /mark-handled`、`POST /unmark-handled`；没有上报接口 |

## 控制面板

`/control-panel` 把四张表按 **Kafka 集群 → Connect → 上游/下游连接器** 铺成卡片树，
新增下级走节点底部的「+」，新建 Kafka 集群走顶栏入口，不再跳转独立的明细页。

- **状态口径**：连接器状态以每台 Connect 的 `GET /dbz/kafka-connector/list` 实时结果为主，
  库内 `run_status` 只作为「最近一次检查」的对照；两者不一致时节点标黄，顶部汇总。
- **Connect 自身健康**：`GET /dbz/kafka-connect/health` 返回可达性、版本、commit、
  Kafka 集群 ID、worker 数与连接器数；卡片上显示 `v3.x · N worker · M 连接器`，
  顶部汇总「x/y 可达」，不可达时带上原因。
- **不一致判定**：由后端按「库内 `connectorConfig` vs Connect 实际配置 + 远端是否存在」
  落库 `inconsistent` 标记，前端只读展示，运行状态不参与判断（任务重启、rebalance
  的抖动不该天天误报）。Connect 卡片的「检测」会遍历整台 Connect 并补录/禁用纳管记录；
  上游 / 下游卡片自己的「检测」只处理这一条记录，用同一套判定刷新标记与脱敏对比快照，
  所以点完卡片就能看到「不一致」标签、点开逐项对比并双向同步。
- **卡片检测**：四类卡片共用同一个结果弹窗，属性面板保留最近一次结论。连接器卡片的
  一致性检测走 `POST /dbz/control-plane/connector-config/detect`，后端回写
  `inconsistent` / `inconsistent_detail` 后前端刷新列表；「启停状态」只是弹窗里的补充
  诊断，不参与一致性判定。
- **图上动作**：详情抽屉（集群实时配置、实时任务、异常原因）、启用/停用、重启；未纳管的
  连接器不进入画布，统一到 `/kafka-connector` 页查看全量列表（该页入口已从左侧菜单移除，直接访问 URL 仍可用）。
- **配置来源**：详情抽屉展示库内 `connectorConfig`；Connect 上的实际配置由后端在
  `POST /control-plane/connector-config/detect` / `sync` 里直接读取并比对，前端不再单独拉
  `GET /dbz/kafka-connector/config`（该接口仍保留给运维排查，控制面板不依赖它）。
- **刷新**：刷新按钮会重新拉四张表并对每台 Connect 实时采集 `list` 与 `health`；单台 Connect
  15 秒不返回只影响它自己的实时状态，不会拖住整页。
- **死信角标**：同一轮还会拉 `GET /dbz/dead-letter/pending-count`，连接器卡片显示待处理条数，
  点击跳到 `/dead-letter?connectorType=…&connectorId=…`；顶部汇总待处理总数。
- **Kafka 集群**没有实时探测接口，节点状态仍是库内结论，可点「检测」调用
  `POST /dbz/kafka-broker/check` 当场刷新。
- **集群导出 / 导入 / 备份恢复**：顶栏「导出集群」按当前 tab 导出整颗集群，与集群卡片上
  的「备份」共用一个确认弹窗；「导入集群」与集群卡片上的「恢复」共用一套预览/执行流程。
  导出走 `GET /dbz/backup/export`（文件含 `sasl_password`，只提示不脱敏），导入与恢复走
  `POST /dbz/backup/restore`：`dryRun` 预览 → 选策略（仅补齐缺失 / 覆盖已存在）→
  二次确认 → 结果明细。导入用于目标环境还没有该集群时把集群、Connect 与连接器一次建出来，
  恢复用于往已有集群补记录；两者都只写库，不向远端注册或删除连接器，导入成功后自动切到
  新集群 tab。
- **配置比对口径**：只有一份，在后端（`config_mismatch` / `compare_detail_text`），
  整集群检测、单条连接器检测、双向同步后重算标记、对比快照都复用它。只比库内出现过的
  键（`name` 除外），对象键序无关、数字按数值比较，文本与数字能解析成同一数值时视为
  一致（库内 `"3306"` 与远端 `3306` 不算差异）；敏感项的值一律脱敏，只留「变了 / 没变」。
  前端不再自己镜像一份比较逻辑，避免「弹窗说一致、标签说不一致」。

其它后端缺口（前端不伪造数据）：

| 缺口 | 现状与影响 |
| --- | --- |
| 连接器指标 | 任务 lag、吞吐等 metrics 未暴露，图上只展示状态与任务清点 |
| 未纳管连接器的图内操作 | 图上仅列出名称并跳远端页；要先在上游/下游建记录才能启用/停用 |

## 死信与重试（Kafka Connect DLQ）

坏记录的**产生、重试、保全**全部交给 Kafka Connect，dbz 不再自建死信判定与重试队列。

### 为什么不用旧的自研方案

旧实现要求 magic 脚本在失败分支调 `POST /dbz/dead-letter/report` 写死信，再由
dbz 的后台 worker 按 `1s → 5s → 30s → 2m → 10m` 退避重投、10 次后转终态。依据是
「HttpSink 对 4xx/5xx 只记日志并照常提交偏移，Connect DLQ 兜不住」。这个结论有两个问题：

1. Confluent HTTP Sink 官方文档明确支持 DLQ，并且自带 `max.retries` /
   `retry.backoff.ms`；Kafka Connect 的 `errors.tolerance=all` +
   `errors.deadletterqueue.topic.name` + `errors.deadletterqueue.context.headers.enable`
   会把坏记录的原始字节连同源坐标、异常类名、消息与堆栈写进 DLQ topic。
2. 真正让 Connect 「看不见失败」的是业务契约：magic 用 HTTP 200 +
   `{"result":-2}` 表达业务失败。把判定外包给脚本的后果是 —— 脚本漏报、崩溃、
   网络断都会静默丢数据，而且 Kafka 里连原始报文都没留下。这**不是** DLQ 的能力
   缺陷，而是失败没有被表达出来。

`errors.tolerance=all` 会跳过错误继续消费，因此 DLQ 与 context headers 必须同时开，
否则同样等于丢数据。

### 现在的链路

```
下游接口返回非 2xx / 记录转换失败
  → Kafka Connect 按 errors.retry.* 与 max.retries 重试
  → 仍失败：原始记录 + __connect.errors.* 头写进 DLQ topic，正常提交偏移
  → dbz（jobx 健康检查计划 / 页面「同步 DLQ」）增量拉取，按四列坐标幂等落库
  → 死信页查询、改报文、重投、废弃、标记已处理
```

- **配置注入**：新增/修改下游连接器时，`dbz-svr` 把下列 key 合并进
  `connector_config` 再下发（库内保存的也是补齐后的配置，一致性检测不会误报）：
  `errors.tolerance=all`、`errors.deadletterqueue.topic.name`、
  `errors.deadletterqueue.topic.replication.factor`、
  `errors.deadletterqueue.context.headers.enable=true`、
  `errors.retry.timeout`、`errors.retry.delay.max.ms`、`errors.log.*`；
  HTTP Sink 另外补 `max.retries` / `retry.backoff.ms`。上游（source）连接器不注入
  —— Connect 的 DLQ 只适用于 sink。
- **事实来源**：`dbz_dead_letter` 只是页面索引 + 人工处置结论（状态、备注、处理人），
  唯一键是 `(connector_id, topic, partition, offset)` —— 带上连接器是因为同一源 topic
  可能被多条 sink 连接器订阅，同一条源记录在每条连接器上失败都是独立死信。
  DLQ 记录不可变，重复同步直接跳过，不会把「已人工处理」重置回「待处理」；
  删索引行不丢 Kafka 里的原始报文。DLQ 头里的连接器名反查不到本地记录时
  `connector_id=0`（未归属），这类记录能看不能重投。
- **重试**：由 Connect 负责，dbz 不再有重试队列表、退避 worker 与 `/report` 接口。
- **重投**：把报文写回**源 topic**，让 Connect 重新走一遍完整链路（转换器、SMT、
  offset 语义都与原始链路一致），不再绕过 Connect 直接 POST 下游 HTTP 地址。
  ⚠️ 源 topic 若被多条下游连接器订阅，它们都会再收到一次，重投前请确认。

### 前提与约束（部署时必须满足）

- 下游接口必须把**业务失败返回成非 2xx**，不能再用 HTTP 200 + `{"result":-2}`，
  否则 Connect 认为投递成功，任何 DLQ 都接不住。
- DLQ topic 不能被连接器的 `topics` / `topics.regex` 覆盖，否则 Connect 会拒绝下发配置，
  即便下发成功也会把死信再当业务消息消费一次。dbz 在补齐配置时会先拦一道：
  `topics` 精确命中、或 `topics.regex` 明显命中就报错，不会再打到 Connect 上。
- 安全集群里 Connect worker 需要有创建 DLQ topic 与写 DLQ 的权限；用 RBAC 时还要
  给 worker 配 `admin.*` 管理端参数。
- `replication-factor` 默认 3，单 broker 的开发环境要调小，否则 Connect 自动创建
  DLQ topic 会因副本数超过可用 broker 而失败。
- DLQ 上下文头来自 Apache Kafka 的 `DeadLetterQueueReporter`，实际 key 为
  `__connect.errors.topic` / `partition` / `offset` / `connector.name` /
  `exception.class.name` / `exception.message` / `exception.stacktrace`；dbz 同时兼容
  Confluent 文档里出现过的 `_connect.errors.*` 单下划线前缀。
- 重投只写回原始 key 与 value，**不复制 DLQ 记录头**：Connect 写 DLQ 时是在原记录头
  上追加 `__connect.errors.*` 上下文头，整包写回会污染源 topic。代价是原业务记录
  自带的消息头不会保留；若下游依赖消息头，需要在改报文时自行补齐。
- 改造不兼容旧数据：`20261110000000_dlq-redesign.sql` 会删掉 `dbz_dead_letter_retry`、
  清空 `dbz_dead_letter` 并重建唯一索引，重启后从 DLQ topic 重新同步。

### 配置（`[dbz.dead-letter]`，`dbz-svr.toml`）

| 配置项 | 默认值 | 作用 |
| --- | --- | --- |
| `enabled` | `true` | 是否注入 DLQ 配置并同步 DLQ 记录 |
| `topic` | `dbz-connect-dlq` | 全局 DLQ topic 名 |
| `replication-factor` | `3` | Connect 自动建 DLQ topic 的副本数 |
| `retry-timeout-ms` | `300000` | `errors.retry.timeout` |
| `retry-delay-max-ms` | `30000` | `errors.retry.delay.max.ms` |
| `http-max-retries` | `10` | HTTP Sink 的 `max.retries` |
| `http-retry-backoff-ms` | `3000` | HTTP Sink 的 `retry.backoff.ms` |
| `log-enable` / `log-include-messages` | `true` / `false` | `errors.log.*` |
| `sync-batch-size` | `200` | 每轮每分区最多同步条数 |
| `retention-days` | `30` | 页面索引保留天数（DLQ topic 由 Kafka retention 管） |
| `max-per-connector` | `50000` | 单连接器索引上限 |

### 页面字段口径

`failure_category` / `failure_subtype` 由 DLQ 头的异常类名与消息推导，只用于筛选、
一句话说明与「能否改报文」白名单，**不参与重试决策**。只有「数据/格式类 +
字段不合规」允许修改报文：原始 `payload` 永不覆盖，改后的报文写在
`pending_payload`，重投时序列化回源 topic。`failure_detail` 保留 DLQ 上下文原文
（异常类名、消息、堆栈、DLQ 坐标）。`retry_count` 恒为 0 —— Connect 的内部重试次数
不写进 DLQ，因此列表、详情与 CSV 导都不再展示「次数 / 首次失败 / 末次失败」，
只保留一个「Kafka 记录时间」（`first_seen_ms` 与 `last_seen_ms` 同值）。
`connector_type` 恒为下游（2）。

## 状态悬停：运行详情

集群、Kafka Connect、上游/下游连接器列表里的**状态标签可以悬停**（虚线下划 + 问号光标），
弹出的是「这个状态是怎么得出的」：

| 页面 | 弹层内容 | 失败原因来自 |
| --- | --- | --- |
| Kafka 集群 | 地址、认证方式（协议 + 机制 + 账号）、结论时间 | `dbz_kafka_broker.run_msg`（检测时写入） |
| Kafka Connect | 地址、结论时间 | `dbz_kafka_connect.run_msg`（健康检查写入） |
| 上游 / 下游连接器 | 连接器名、所属集群（下游还有上游）、结论时间 | `dbz_{source,sink}_connector.run_msg`（健康检查写入） |
| 集群连接器 | 类名、worker、任务清点、停止/暂停说明 | `GET /connectors?expand=status` 带回的 trace，不额外发请求 |

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

控制面板卡片是直接对集群下手的地方，除了重启与删除，还有两个**不改库**的动作：

| 操作 | Kafka Connect 接口 | 影响 |
| --- | --- | --- |
| 停止 / 恢复 | `PUT /connectors/{name}/stop`、`/resume` | 保留配置与位点，恢复后从原处继续；需要 Kafka 3.5+ |
| 重置位点 | 先 `stop` 再 `DELETE /connectors/{name}/offsets` | 位点清空即「从没跑过」，`snapshot.mode=initial` 时整表快照；**不可撤销** |

停在「已停止」的连接器在健康检查里会被记为异常并告警，但**不会被自动重启** ——
自动修复只处理 `FAILED`，人工动作不能被机器覆盖。要让 dbz 不再管这条连接器，
去上游/下游列表把启用开关关掉。

## 已知限制

- **健康检查周期**：列表页的状态是「上一轮检查的结论」，不是实时探测结果；「控制面板」和 `/kafka-connector`
  会实时读取 Kafka Connect，但要把库内 `run_status` 追平仍需等 jobx 回写（Kafka 集群可在控制面板手动检测）。
- **没有登录鉴权**：`X-User-Id` 由构建期变量写死（默认 `1`），审计字段在接入统一登录前都记成同一个人，
  只适用于内网/开发环境。
- **子路径靠变量切**：`VITE_PUBLIC_BASE` 一次驱动 `base` / favicon / `basename` 三处；变量和网关前缀不一致就会出现
  白屏或刷新 404（对照「部署后自检」那张表）。
- **列表暂不支持时间范围筛选**：`#[router(crud)]` 的 QueryDto 只支持字段等值匹配，`updateMs` 只能传精确值，
  所以搜索行没有「更新时间起止」控件。需要后端先在 QueryDto 增加区间参数（如 `updateMsFrom` / `updateMsTo`）
  并在 DAO 拼 `>=` / `<` 条件，前端再补控件；本次未改后端契约。
- **集群连接器为全量列表**：`GET /dbz/kafka-connector/list` 不带分页，集群上连接器很多时一次拉全量；
  需要后端加 `/page` 后再改前端。
