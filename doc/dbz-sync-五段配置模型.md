# dbz-sync 五段配置模型

> 前置决策：① 执行器是独立进程；② 一个 topic 可挂多个任务；③ 只做「匹配 / 映射 / 动作 / 判定 / 校验」五段，
> 超出部分写回 gatexrs。
>
> 本文回答一件事：**五段分开看，各自能抽象到什么程度、用什么形态配置、哪部分必须写死在代码里。**

---

## 0. 两条决策带出来的三个推论

**推论一：五段必须正交，否则图形化一定退化。**

现在三个脚本把分支埋在映射里（`if (table.equals("dtw_user_locator")) requestBody.bindTargetType = 1`），
把 op 分支埋在执行里（`if ("c" == op || "r" == op) ... else if ("u" == op) ...`），
把失败分类埋在校验里（`failWith` 里判断 `isTransientFailure`）。
只要"一个 topic 多个任务"成立，这些分支就都有更好的去处：**分支 = 多一个任务，条件 = 匹配层**。
拆干净之后：

- 映射是纯函数：一条记录 → 一个 body，无分支；
- 动作是查表：op → 一个 HTTP 调用，无分支；
- 判定是查表：一个响应 → 一个结论，无分支；
- 校验是查表：一个 body → 通过 / 不通过，无分支。

只有映射里那个「按 table 分变体」是真正需要决策的：**用多任务表达，还是用变体组表达？**
建议用多任务。`dtw_locator_bind` 拆成 `dtw_user_locator` / `dtw_car_locator` 两个任务，
各自 `match: $.source.table == "..."`，映射表就退化成 7 行纯改名 + 2 行常量。变体组是多余的抽象。

**推论二：执行器无状态，重试永远归 sink。**

执行器对 sink 只有一个契约：2xx = 全部命中任务都成功；4xx = 确定性失败；5xx/429/408 = 可重试。
执行器自己**不做重试、不排队、不阻塞**，一次请求一次处理。
所以判定层的可配置结论只有 `success / skip / fatal / retriable` 四个**输出枚举**，
`retriable` 的语义是"翻译成 503 交还给 sink"，而不是"我要重试 3 次"。
这样重试预算、退避、DLQ 全部还在 sink 的 `http.retry.*` 里，只有一处可调。

**推论三：一个 topic 多个任务 ⇒ 必须有幂等记账，否则 fanout 会重复写。**

一条记录命中 3 个任务，第 2 个失败 → 整条记录返回非 2xx → sink 重投 → 第 1 个任务再执行一遍。
所以执行器必须有 `(task_id, idempotency_key)` 的记账表，重投时直接返回上次结论。
key 由 sink 传（`X-Idempotency-Key`），执行器只负责**按任务维度**记账 —— 这也是"多任务"和"单任务"在存储上唯一的差别。

---

## 1. 配置分层：L0 / L1 / L2 / L3

先立一条判定标准，五段都按它落位：

| 层 | 形态 | 谁能改 | 适用 |
|---|---|---|---|
| **L0** 结构化表格 | 可编辑 grid，列有枚举约束 | 运维 / 实施 | 匹配、映射、动作、判定、校验的**主体** |
| **L1** 受限表达式 | JSONPath 取路径 + 固定操作符 + 命名变换函数链 | 实施 | 只出现在单元格里，不构成独立语法 |
| **L2** 脚本沙箱 | Rhai，无网络、无时钟、有指令数上限 | 开发，且需审批 | 跨记录聚合、多步依赖、循环 —— **现有 3 个脚本一个都不需要** |
| **L3** 写死在代码 | 编译期常量 | 只有发版能改 | 传输分类、幂等、并发、凭证、脱敏 |

**为什么把 L2 放到最后**：一旦编辑框里能写脚本，所有表格都会变成"给脚本传参数"的摆设，
图形化就白做了。L2 应该是**逃生门**而不是**入口** —— 默认关闭，开启需要说明"为什么五段表达不了"，
且脚本只能出现在单段位置上（比如某个映射行的 `value`），不能接管整条流程。

判断标准：如果一段逻辑需要**看第二条记录**才能算出来（去重、批量合并、累计），才允许 L2；
只要信息全在当前这条记录里，就一定能用 L0+L1 表达。

---

## 2. 逐段分析

### 2.1 匹配（Match）

**脚本现状**：隐式。一个 topic → 一条 magic-api 路径 → 一个脚本，没有匹配逻辑。
唯一的行级条件是 `dtw_region` 里被注释掉的 `type_dic_value == 54` 和
`dtw_locator_bind` 里 `table.equals(...)` 的变体分支 —— 两者都放错了地方。

**能配到多少**：100%。匹配条件永远是「字段路径 + 操作符 + 值」的合取，没有例外。

**配置形态**：有序规则表，priority 升序，**首个命中即停**（默认）。

```
[任务]  priority  topic                    条件（AND）
A       100       dtw.dtw_locator_bind      $.source.table == "dtw_user_locator"
B       110       dtw.dtw_locator_bind      $.source.table == "dtw_car_locator"
C       120       dtw.dtw_locator_bind      *（兜底，verdict=skip, code=no_match）
```

| 维度 | 约束 |
|---|---|
| 条件字段 | JSONPath，**从样例记录自动生成下拉**，不让人手写 |
| 操作符 | `eq / ne / in / not_in / exists / absent / starts_with / regex / gt / lt` |
| 组合 | 一层 AND；需要 OR 就加一行任务 |
| 优先级冲突 | 保存时检测「条件完全相同 / 一个包含另一个」并告警 |
| 未命中 | 由 topic 级 `default_verdict` 决定：`skip`（默认）或 `fatal(no_match)` |

**不能配的部分**：topic 到任务的索引结构。`topic` 必须提成关系列并建索引，
不能让每个请求都去扫 jsonb —— 这是表设计问题，不是配置问题。

**GUI 控件**：任务列表（拖拽排序）+ 条件行表格 + 顶部「用样例记录试匹配」显示命中哪个 / 为什么没命中。

---

### 2.2 映射（Mapping）

**脚本现状**：三段代码里信息量最大、也最机械的一段。
`dtw_region` 11 个字段：6 个纯改名、1 个常量、1 个变换（`normalizeRegionName` 去「电子围栏区」后缀）、3 个直传。
`dtw_person` 10 个字段：4 个套 `cleanString`（去 `\x00`）、1 个 `to_bool`、其余改名。
还有一条全局语义：`rawMap.each((k,v) => v != null && (body[k] = v))` —— null 值整行省略。

**能配到多少**：95%。剩下的 5% 不是"需要写代码"，而是"需要限定函数库"。

**配置形态**：一张表，每行一个目标字段。

| 目标字段 | 来源 | 变换链 | 空值 | 失败策略 |
|---|---|---|---|---|
| `id` | `$.after.id` | — | 省略 | fatal |
| `name` | `$.after.name` | `strip_suffix("电子围栏区")` → `trim` | 省略 | — |
| `geom` | `$.after.polygon_location.wkb` | `wkb_to_wkt` | — | **fatal(WKB_INVALID)** |
| `tags` | `const "｜电子围栏｜"` | — | — | — |
| `enabled` | `$.after.is_enable` | `to_bool_01` | 省略 | — |
| `code` | `$.after.locator_id` | — | 省略 | fatal |

关键设计点：

1. **变换是封闭注册表，不是表达式语言**。每个函数有签名、有单测、出现在下拉里：
   `trim / strip_nul / strip_suffix(s) / strip_prefix(s) / truncate(n) / to_bool_01 / to_ms /
    wkb_to_wkt / upper / lower / concat(sep, ...) / default(v) / map_enum{k:v}`。
   要加新函数 = 改代码发版，不走配置。这是**故意**的：变换是数据正确性的最后一道闸，
   让它在配置里自由演化，等于把脏数据责任推给实施人员。
2. **空值省略是全局开关**（`omit_null: true`），不逐行配。脚本里这段是全局 `each`，配置里也应该全局。
3. **变换失败策略挂在映射行上**，不进校验段。`wkb_to_wkt` 抛异常 → 该行 `on_error: fatal(WKB_INVALID)`。
   这样校验段就不用去 try/catch 变换，两段彻底解耦。
4. **`normalizeRegionName` 的"清空后保留原名"是复合语义**，用两个字段表达：
   `fn: strip_suffix(...) → trim`，加 `on_empty: keep_original` + 一条 warn 日志。
   不要为它单独造一个 `strip_suffix_or_keep` 函数 —— 那是把一个配置问题写成代码。

**不能配的部分**：字段类型转换的正确性。`to_bool_01` 到底是 `1→true` 还是 `"1"→true`，
`to_ms` 接受秒还是毫秒（后者必须过 `normalize_ms`），这些是代码里定死的，配置只能选函数名。

**GUI 控件**：字段映射 grid；来源用「样例记录树选择器」；变换链是**可拖拽的标签串**（点 + 号从下拉加一个）；
右侧常驻「映射结果 JSON」实时预览。

---

### 2.3 动作（Action）

**脚本现状**：`if ("c" == op || "r" == op) ... else if ("u" == op) ... else if ("d" == op)`。
三个脚本的表各不相同，且**不一致本身就是 bug 源**：

| 脚本 | c / r | u | d |
|---|---|---|---|
| `dtw_region` | POST `/upsert` | POST `/sync` | DELETE `?id=` |
| `dtw_locator_bind` | POST ``（base） | POST `/upsert` | DELETE `?id=` |
| `dtw_person` | POST ``（base） | POST `/upsert` | DELETE `?id=` |

**能配到多少**：100%。就是一张 op × 调用 的 4 行矩阵。

**配置形态**：

| op | method | 路径 | body | query | 额外 header |
|---|---|---|---|---|---|
| `c` | POST | `/upsert` | mapped | — | `X-User-Id ← $.after.updator_id` |
| `r` | POST | `/upsert` | mapped | — | 同上 |
| `u` | POST | `/sync` | mapped | — | 同上 |
| `d` | DELETE | `` | none | `id ← $.before.id` | `X-User-Id ← $.before.updator_id` |

- **路径不是明文 URL**，是「目标服务引用 + 相对路径」。`target: { service: "dtw-rs-svr", base: "/dtw/region" }`，
  host 由服务发现/环境变量给。三个脚本里硬编码 `http://gatexrs:80/...` 是必须消掉的。
- **body 只有三态**：`mapped`（用映射结果）/ `none` / `raw`（原样透传记录）。“要不要发数组”这类
  由 sink 决定，不在这一段。
- **header 值也是字段路径**，带 `default` 兜底（`X-User-Id` 取不到时 `"0"`）。
- **`r`（快照读）必须显式存在**，不能靠"没写就当 c"。三个脚本里 `r` 和 `c` 走同一分支是巧合，
  新下游如果区分不了快照重放，配置里就得能单独给 `r` 指定不同路径。

**顺带修掉的真 bug**：`dtw_region` 和 `dtw_locator_bind` 都写 `after.UPDATOR_ID`（大写），
源列名是小写 `updator_id` → 恒取不到 → **`X-User-Id` 恒为 `"0"`，审计人一直是 0**。
`dtw_person` 因为源列真的叫 `UPDATOR_ID` 所以恰好正确。
换到字段路径 + 样例记录树选择器之后，这类大小写猜错在编辑期就会被"路径不存在"拦下来。

**不能配的部分**：并发度、超时、连接复用、是否复用长连接 —— 服务级配置；凭证（`X-User-Id` 之外的真 token）走 secret 引用。

**GUI 控件**：4 行矩阵；每行一个「试发」按钮，显示将要发出的完整请求（含 headers）。

---

### 2.4 判定（Verdict）

**这是五段里唯一需要仔细切的一刀。**

**脚本现状**：三套字符串 `indexOf` 混在一起，语义边界是糊的：

| 函数 | 判断依据 | 真实语义 |
|---|---|---|
| `isTransientFailure` | `indexOf("500 ")`、`"timed out"`、`"Connection"`… | **传输层**（应该是 status code / io error kind） |
| `isBusinessFailure` | `"RTW00002"`、`"extra":{` + `"result":-` | **业务层**（应该是 `$.extra.code`） |
| `isAlreadyExists` | `"RTW00001"` | 业务层，但结论是 `skip` |
| `skipCategoryOfDuplicate` | `"key name: code"` 等 5 种 | 业务层，**只产出分类标签** |
| `skipCategoryOfValidation` | `"身份证号格式不正确"` 等 6 种 | 业务层，**只产出分类标签** |
| `isFieldIssue` | `"参数校验错误"`、`"长度"`… 且排除 `"不存在"` | 业务层，**只产出分类标签** |

**切法**：分三层，只有中间一层进配置。

| 层 | 判定 | 归谁 | 理由 |
|---|---|---|---|
| 传输层 | 2xx → 进入业务判定；408/429/5xx/超时/DNS → `retriable`；其余 4xx/3xx → `fatal` | **代码，写死** | 这是 Kafka Connect 的契约，配错 = 毒消息无限重投或该重试的直接丢 |
| 业务层 | 响应 body 的结构化断言 → `success / skip / fatal / retriable` | **配置** | 下游各服务语义不同，只能逐服务配 |
| 分类层 | 命中的业务规则 → 一个 `code` 标签（`duplicate_code` / `invalid_id_card`…） | **配置**，附在同一条规则上 | 用于死信归类与前端"能否改报文重投" |

`isTransientFailure` 整个函数**删掉**，不需要配置化。脚本里它之所以存在，是因为 magic-api
拿不到 HTTP status（`http.connect()` 失败只给一个 message 字符串），只能靠 `indexOf("500 ")` 猜。
执行器用 reqwest，`err.is_timeout()` / `err.is_connect()` / `status.as_u16()` 都是现成的枚举，
猜是错误的来源，不是需求。

`isFieldIssue` 那条「排除『不存在』」的逻辑也是同一个病根：靠中文散文反推语义。
换成 `$.extra.code` 之后，"外键缺失"就是 `RTW00002`，不需要判断句子里有没有"不存在"。

**配置形态**：有序规则表，**第一条命中即停**。

| # | 作用域 | 断言（AND） | 结论 | code |
|---|---|---|---|---|
| 1 | body | `$.extra.code eq "RTW00001"` | skip | `duplicate` |
| 2 | body | `$.extra.code eq "RTW00002"` | fatal | `fk_missing` |
| 3 | body | `$.result lt 0` | fatal | `business_error` |
| 4 | body | `$.msg contains "身份证号格式不正确"` | skip | `invalid_id_card` |
| 5 | status | `in [200, 201]` | success | — |
| — | 兜底 | — | fatal | `unclassified` |

- 作用域只有三个：`status` / `header` / `body`。body 用 JSONPath。
- **`contains` / `regex` 允许存在，但只用于"分类"，不用于流程控制**。
  也就是说：语义性判断必须落到结构化字段（`$.extra.code`）；
  下游只给了中文散文时，才退化到 `contains`，且这类规则的 `code` 必须填。
  规则编辑器里对 `contains/regex` 规则标黄提示"建议上游补错误码"。
- 兜底规则不可删。没有兜底 = 响应不认识时静默成功，这是最危险的默认值。
- **`retriable` 可以配，但含义固定**：翻译成 503 还给 sink。执行器自身不重试。

**不能配的部分**：单条规则的超时、退避、最大重试次数（全在 sink 的 `http.retry.*`）；
"什么算 2xx"（固定 200–299）。

**GUI 控件**：规则表 + 拖拽排序；每行右侧「用响应样例试判定」，显示命中的规则序号与结论；
底部固定一行"兜底"，灰显不可删。

---

### 2.5 校验（Guards）

**脚本现状**：散落在三处，且混了两种完全不同的东西。

| 位置 | 逻辑 | 真实类别 |
|---|---|---|
| `dtw_region` | `after.polygon_location.wkb` 为空 → `failWith` | **值缺失断言** |
| `dtw_region` | `wkbToWkt` 抛异常 → `failWith` | **变换失败**（不是校验） |
| `dtw_person` | `name` 为空 → `failWith("empty_name")` | **值缺失断言** |
| `dtw_person` | `name.length() > 100` → 截断 | **映射变换**（`truncate(100)`，不是校验） |
| 三者共通 | `op != "d"` 时 `after` 可能是 `{}` → 后面 NPE 被平台吞成 HTTP 200 | **前置断言**（现在是隐藏 bug） |

**切法**：把"变换失败"还给映射段（`on_error`），把"截断"还给映射段（`truncate`），
校验段只留**对已映射 body 的断言**。这样校验段小到只有一张表：

| # | 适用 op | 断言（AND） | 结论 | code |
|---|---|---|---|---|
| 1 | c, r, u | `$.after` exists | fatal | `EMPTY_RECORD` |
| 2 | c, r, u | `$.polygon_location.wkb` present | fatal | `POLYGON_MISSING` |
| 3 | c, r, u | `name` present 且 `trim != ""` | fatal | `EMPTY_NAME` |

注意第 1 条：这是三个脚本都缺的、且真的出过问题的断言（空 `after` 导致 NPE 被吞成 200，
记录当成功消费掉）。**NPE 变 200 是这套脚本最隐蔽的损失路径**。

**能配到多少**：100%。校验永远是「body 上的标量断言」，没有任何需要代码的地方。

**不能配的部分**：断言之外的东西一律不算校验。比如"这条记录该不该同步"是**匹配**，
"这个字段要不要带"是**映射**。校验只回答一个问题：**已经组装好的 body 能不能发**。

**GUI 控件**：规则表（同判定段，只是作用域固定为 `mapped body`）；每条能给一句人话消息模板
（`区域 {id} 的几何为空`），直接进死信 `detail`，省掉现在 `failWith(id, "polygon_location.wkb is empty")` 的手拼。

---

## 3. 五段汇总

| 段 | 输入 → 输出 | 配置形态 | 可图形化 | 需要表达式 | 需要脚本 | 写死 |
|---|---|---|---|---|---|---|
| 匹配 | 记录 → 任务集合 | 有序规则表 | **是** | 否（路径+操作符+值） | 否 | topic 索引 |
| 映射 | 记录 → body | 字段映射 grid + 变换链 | **是** | 仅 `${path}` 取值 | 否 | 变换注册表 |
| 动作 | op → HTTP 调用 | 4 行矩阵 | **是** | 路径模板 | 否 | 无（全可配） |
| 判定 | 响应 → 结论+code | 有序规则表（三作用域） | **是** | 断言 DSL | 否 | 传输层分类 |
| 校验 | body → 通过/拒绝 | 规则表（单作用域） | **是** | 断言 DSL | 否 | 无（全可配） |

**"需要表达式"一律指"单元格里的 JSONPath + 固定操作符"，不是可编程语言。**
没有任何一段需要 `if / for / try`。这点值得强调：现有 710 行脚本，
去掉 130 行重复的公共函数和一半的日志，真正有信息量的部分全部落在这五张表里。

**L2 脚本的准入条件**（写进文档，默认不开放）：
只有在出现「需要看第二条记录才能算」的需求时才允许，例如
批量合并（多行聚合成一次请求）、跨记录去重、按累计量触发。
届时脚本也只能挂在某一段的单个位置（如 `mapping[i].value`），
不能接管整条流程 —— 否则五段模型直接失效。

---

## 4. 一个完整的翻译样例（`dtw_region`）

展示五段拆完之后配置长什么样。

```yaml
id: dtw_region
topic: dtw.dtw_region
priority: 100
enabled: true

target:
  service: dtw-rs-svr
  base: /dtw/region

match:
  - field: $.op
    op: in
    value: [c, r, u, d]

mapping:
  omit_null: true
  rows:
    - { to: id,        from: $.after.id }
    - { to: name,      from: $.after.name,
        fn: [strip_suffix("电子围栏区"), trim], on_empty: keep_original }
    - { to: geom,      from: $.after.polygon_location.wkb,
        fn: [wkb_to_wkt], on_error: { verdict: fatal, code: WKB_INVALID } }
    - { to: tags,      const: "|电子围栏|" }
    - { to: remark,    from: $.after.remark }
    - { to: enabled,   from: $.after.is_enable }
    - { to: createMs,  from: $.after.create_timestamp }
    - { to: createrId, from: $.after.creator_id }
    - { to: updateMs,  from: $.after.update_timestamp }
    - { to: updaterId, from: $.after.updator_id }     # 修掉原脚本大写取不到的问题

actions:
  - { op: c, method: POST,   path: /upsert, body: mapped,
      headers: [{ name: X-User-Id, from: $.after.updator_id, default: "0" }] }
  - { op: r, method: POST,   path: /upsert, body: mapped,
      headers: [{ name: X-User-Id, from: $.after.updator_id, default: "0" }] }
  - { op: u, method: POST,   path: /sync,   body: mapped,
      headers: [{ name: X-User-Id, from: $.after.updator_id, default: "0" }] }
  - { op: d, method: DELETE, path: "",      body: none,
      query:   [{ name: id, from: $.before.id }],
      headers: [{ name: X-User-Id, from: $.before.updator_id, default: "0" }] }

guards:
  - when: { field: $.op, op: ne, value: d }
    assert:
      - { field: $.after, op: exists }
      - { field: $.after.polygon_location.wkb, op: present }
    else: { verdict: fatal, code: POLYGON_MISSING,
            message: "区域 {after.id} 的几何为空" }

outcomes:
  - { scope: body, assert: [{ field: $.extra.code, op: eq, value: RTW00001 }],
      verdict: skip,  code: duplicate }
  - { scope: body, assert: [{ field: $.extra.code, op: eq, value: RTW00002 }],
      verdict: fatal, code: fk_missing }
  - { scope: body, assert: [{ field: $.result, op: lt, value: 0 }],
      verdict: fatal, code: business_error }
  - { scope: status, assert: [{ op: in, value: [200, 201] }], verdict: success }   # 兜底
```

对比原脚本：199 行 → 约 55 行配置，且 `isTransientFailure` / `setHttpStatus` /
`requestBody` 手工合并 / 大小写猜测全部消失。语义变化是有意的两处：
`X-User-Id` 不再恒为 `"0"`；空 `after` 不再静默成功。

`dtw_locator_bind` 按决策②拆成两个任务，映射段各少一个分支：

```yaml
# 任务 A
match: [{ field: $.source.table, op: eq, value: dtw_user_locator }]
mapping.rows:
  - { to: bindTargetType, const: 1 }
  - { to: bindTargetId,   from: $.after.user_id }

# 任务 B
match: [{ field: $.source.table, op: eq, value: dtw_car_locator }]
mapping.rows:
  - { to: bindTargetType, const: 2 }
  - { to: bindTargetId,   from: $.after.car_id }
```

两条任务优先级的顺序即语义（谁先命中谁执行），不需要在映射里写 `if`。

---

## 5. 明确不进配置的清单

| 项 | 为什么 |
|---|---|
| 传输层失败分类（2xx/408/429/5xx/超时/DNS） | Kafka Connect 契约；配错 = 毒消息无限重投，或该重试的被当确定性失败丢掉 |
| 是否重试、重试次数、退避 | 归 sink 的 `http.retry.*`；执行器只输出状态码。两处可调 = 两处都会调错 |
| 幂等键生成与记账 | 配错会重复写下游且无法察觉；必须由 sink 传 key + 执行器按任务记账 |
| 变换函数实现 | 数据正确性最后闸门；只能通过发版增删，不能配置演化 |
| 并发度 / 超时 / 连接池 | 服务级资源参数，不是任务级语义 |
| 凭证 | 走 secret 引用，不进任务配置明文 |
| 日志内容与脱敏 | 代码；配置只提供 `code` 和消息模板 |

---

## 6. 存储、发布、试跑

**存储**：`dbz_sync_task` 一表一任务。`topic` / `priority` / `enabled` / `version` 提成关系列并建
`(topic, priority)` 索引；五段配置整体存 `spec jsonb`。
**匹配条件必须提列**（或至少单独建 `dbz_sync_match` 子表），否则每个请求都要全表反序列化 jsonb。

**发布**：保存 = 生成新的不可变 `version` 快照；执行器持有 `arc_swap<Arc<Snapshot>>`，
按 `version` 号增量拉取。控制台与执行器**不共享 DB 连接池**，只共享一个只读快照接口
（这也正是决策①"独立进程"的意义：执行器重启不拖累控制台，控制台改配置不需要执行器重连）。

**两级校验**：
- 编辑期：schema 校验 → 引用完整性（服务是否存在、路径是否在映射里出现过）→ **fixture 试跑**；
- 加载期：单任务配置非法只禁用该任务并告警，绝不让整份快照加载失败（一个坏任务不能停掉整条链路）。

**试跑（dry-run）是图形化能成立的前提**。每次编辑，用保存的 fixture（从死信里挑一条真实记录）
走完五段，右侧并排显示：

```
命中任务 dtw_region（priority 100）
→ mapped body  { id: 123, name: "一号围栏", geom: "POLYGON(...)", tags: "|电子围栏|", ... }
→ 请求          POST http://dtw-rs-svr/dtw/region/upsert  X-User-Id: 10086
→ 判定          body $.extra.code=RTW00002 → fatal / fk_missing
```

没有这个东西，运维改配置就是在赌；有这个东西，才敢把 710 行脚本交给表格。

---

## 7. 分期

| 阶段 | 内容 | 验收 |
|---|---|---|
| P0 | 五段 spec 的 Rust 类型 + 3 个脚本翻译成 YAML + 影子比对（new body / 请求序列 vs magic-api 逐字节） | 3 条 fixture 下请求体与 URL 完全一致 |
| P1 | 执行器（独立 binary）：匹配 → 映射 → 动作 → 判定 → 校验 + `/sync/{topic}` + 幂等记账 + dry-run 端点 | 单测覆盖五段各自的正/负样例；fixture 回放通过 |
| P2 | 管控面：任务 CRUD、五个编辑器、试跑面板、发布/回滚 | 实施能在不看文档的情况下改出一致的请求 |
| P3 | 灰度：一个 topic 先双写（执行器只比对不发），再切流量，magic-api 保留 2 周 | 死信量与分类分布无异常跳变 |
