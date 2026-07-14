# S5 Ticker Events：schema review、日期质量规则与发布证据

## 1. 范围与硬边界

S5 只把 Massive `ticker_events` Bronze 转成可审计的身份事件证据，不生成永久
`asset_id`、ticker 有效区间或回测股票池。本阶段发布的两张表都固定
`backtest_identity_eligible=false`；S4 Assets、S5 Ticker Events 和 S6 Overview 的跨源身份
解析仍由 S7 单独完成。

本次父表的正式请求粒度由
`manifests/plans/ticker_events/identifiers.txt` 的 15,173 个 Composite FIGI 定义；子表的事件粒度来自
11,471 个成功 Bronze gzip 中的 13,088 个 raw events。100 个 audit-only pilot identifier 及其
16 个成功响应、84 个 404 全部排除，不能混入正式 Silver 计数。S5 不请求 Massive、不修改
Bronze，也不启动 S6。

## 2. 正式 source profile

| 项目 | 结果 |
| --- | ---: |
| 正式 identifier receipt | 15,173 个唯一 Composite FIGI |
| receipt SHA-256 | `c0386e3a19c5fadb5a976052ebc964e72836b3b60644e842a740d8e6dcdfd312` |
| 成功请求 | 11,471 |
| 稳定 HTTP 404 | 3,702 |
| 成功响应内 raw events | 13,088 |
| 合法 ticker-change events | 12,895 |
| 空 target placeholder | 193 |
| 空 target 响应内仍需保留的合法 sibling events | 262 |
| 响应 FIGI 与请求 FIGI mismatch | 0 |
| 完整请求的 CIK 缺失 | 2,527 |
| 完整请求的 CIK 非空 | 8,944 |

Bronze gzip、stored/raw checksum、JSON、manifest row count 和 request-to-response FIGI 均已通过
全量验证。正式成功请求每个只有一页；语义候选事件键没有重复。3,702 个 404 是 endpoint 对该
identifier 的稳定终态，不是损坏文件，也不能解释为每日股票池缺失。

为避免 Silver store 把 404 `failed` manifest 当作可消费数据，S5 先生成一个不可变、确定性的
coverage receipt schema v2。它逐条绑定 15,173 个正式请求的 manifest path、SHA、identifier 和
terminal outcome，并逐文件绑定 11,471 个成功 gzip artifact；pilot 不在 receipt 中。

转换使用两个不同粒度的 `SourceInventory`：父表的 `control_manifest` inventory 直接以
`identifiers.txt` 作为 15,173 行权威载体；子表的 Bronze inventory 直接以 11,471 个 gzip 作为
13,088 个 event occurrence 的载体。两个 inventory 必须绑定同一份 coverage receipt v2 和同一 Git
commit。因此父表的 response manifest/page 虽不是其直接 row-count carrier，仍通过 receipt v2 被完整、
闭合地绑定，读取时会再次校验原始 manifest、gzip 和响应身份。

## 3. 两张输出表

### `identity/ticker_event_request_status`

父表 grain 是每个正式 Composite FIGI 请求一行，共 15,173 行。它保存 identifier、request 和
manifest lineage、`complete_timeline/not_found_404` outcome、响应身份字段、事件计数与
capture/final-observed
时间。404 作为正常 coverage outcome 保存，不进入 quarantine。

- contract ID：`5890117915e8ffc585c2faa1b9f4a9909a75f068bdad50a5e6bd64f78cf1df02`
- candidate/resource SHA-256：
  `e2bb30eb23171f0b1b02b9ad085e2b5099499585b6ac00bea30ea84882239982`
- Arrow schema digest：`8c80dbd8c56508c1c03a5a7a8ecd08c2de9e6afefdf7d4edef027c9c0bea7c88`
- 30 列、27 个 QA rules；主键 `source_request_id`，按 `source_observed_date` 分区。

### `identity/ticker_change_event`

子表 grain 是每个正式成功响应中的一个合法 ticker-change occurrence，共 12,895 行。它保留 raw
date、parsed date、date-quality 分类、target ticker、原始响应身份和逐 occurrence lineage。同一
FIGI、同一日期出现多个不同 ticker 时全部保留，不选择 winner；ticker 被多个 FIGI 重用也不在
S5 合并。

- contract ID：`48a46dfd810b95137125b336917c23343da2aace5a6a71d99129b4d10f2e59b1`
- candidate/resource SHA-256：
  `8d79a5fcab1dbe849c3cbcb9bda8c564226d24de44554a4a5f317a71e368febb`
- Arrow schema digest：`b643a7381e3fd704800aa703aa6c621173b951c40c80ce806ae491f578715fb2`
- 31 列、36 个 QA rules；主键 `source_record_id`，按 `source_capture_date` 分区。

子表每行必须引用父表中 `complete` 的 `source_request_id`。父表按响应保存 raw/accepted/
quarantined event counts，使 13,088 = 12,895 + 193 可以逐请求重算。

虽然最初处理计划把 quarantine 写成第三张业务表，当前实现遵循统一 Silver 证据模型：193 个异常
occurrence 写入每个 build 的标准 `quarantine-record.parquet`，不再建立可发布的第三个业务
workflow。这更方便复用统一审批、issue ID 和发布 gate；与原计划的差异在此明确记录。

## 4. 已批准的日期质量方案

原始 `date` 字符串与可解析的 `event_date` 同时保留，不静默改写：

- `1969-12-31` 共 766 条，标记为 provider sentinel / unknown effective date，不能直接形成有效
  区间；
- `2003-09-10` 共 1,334 条，标记为 provider lower-bound ambiguous，等待 S7 用 Assets/Overview
  交叉佐证；
- 周末日期共 481 条（其中 480 条为 `2023-11-18`），原样保留，不平移到交易日；
- 1 条 `2026-07-10` 晚于下载计划标签 `2026-07-09`，但早于实际 capture。ticker-events endpoint
  请求只发送 identifier 与 event types，计划的 start/end 只是本地审计标签，因此该条不是未来
  泄露，也不删除；
- 193 条 `2023-11-18` 空 target occurrence 作为 High quarantine，只有该 occurrence 被排除，
  同响应的合法 sibling 保留。

用户于 2026-07-14 明确批准这套方案，并说明研究不依赖这些极早日期。这个批准只允许按上述精确
分类接受已知 warning/quarantine；任何计数、日期类别或身份 mismatch 超出 profile 都必须 fail
closed，不能套用该批准。

## 5. 已知身份诊断与 S7 边界

全历史诊断还包括：1,244 个 FIGI 有多个合法 ticker、430 个 exact ticker 映射到多个 FIGI、2 个
FIGI 在同一天出现多个不同 target ticker，以及 4,298 条事件早于 S4 Assets 的 2016-07-11
覆盖窗口。这些都是身份解析证据，不是 S5 内可安全自动修复的问题。

因此：

1. S5 不推导 prior ticker；endpoint row 只有 target ticker；
2. S5 不用事件顺序生成 `[valid_from, valid_to)`；日期 sentinel、下界截断和同日多事件会使这种
   推导过早；
3. S5 不用 ticker、CIK 或 FIGI 单字段生成永久 identity；
4. 上述诊断以 Medium warning 保留，并由本次精确授权逐项 waiver；
5. S7 必须把 S4/S5/S6 作为证据源重新协调，并对冲突输出人工 review 清单。

## 6. Preview / FullRun 策略

正式父表 inventory 只有一个 197,249-byte identifier receipt；子表 inventory 由 11,471 个小 gzip
页面组成。因此 bounded preview 直接覆盖完整正式双 inventory，而不是抽样后再扩大范围。页面样例
仍限制为最多 100 条，但 data、QA、row funnel 和 quarantine 都是 full formal scope。
PreviewMetadata 对两个 inventory 均明确记录 `projection_multiplier=1.0`，所以 Full build 只能在同一精确
inventory、coverage receipt、代码 commit 和参数下重算，不需要额外 FullRunPlan。

两张表的 workflow 锁步推进。只有父子计数、主键、外键、QA 和 quarantine 全部匹配已批准画像，才
允许批准 full run；发布时先发布 request-status 父表，重新读取并验证后，才发布 event 子表。

## 7. 发布证据

### 7.1 冻结运行身份

- 转换代码 commit：`cd1667028cc6709f2836dcde44dbbf06c3f13170`
- coverage receipt v2：
  `manifests/silver/source-coverage/ticker_events/coverage-f4c3237e681b7710db23edcb5d639f4092b532affbe8e89ebebd718d1f013f52.json`
- coverage receipt file SHA-256：
  `fb116e2fd6a84e4c14a87dc02cb1894e73d91d571df25d6276fe3c5072314cfd`
- source profile SHA-256：`d78bb6564a883ac7f60dc5b5e5f32836b9df9ec56d8d8c3c7c2f253b45822117`
- 正式 identifier receipt SHA-256：
  `c0386e3a19c5fadb5a976052ebc964e72836b3b60644e842a740d8e6dcdfd312`
- fixed cases：父表 `current_reference_snapshot`；子表 `current_reference_snapshot`、
  `ticker_change`、`ticker_reuse`；两个 preview 的 `projection_multiplier=1.0`。

本次运行是纯离线转换：未请求 Massive，2026-07-14 12:45 UTC 之后 Bronze 无新文件；S6
`ticker_overview_safe` 正式 build 目录不存在。

### 7.2 双 source inventory

| 目标表 | source layer | inventory ID | inventory manifest SHA-256 | 直接权威载体 |
| --- | --- | --- | --- | --- |
| `ticker_event_request_status` | `control_manifest` | `b5dfe802713c83e4c19e894af3363efd44e259b2b84e0a88dadb24d02f29b654` | `a8d8685949e492a0002d43ddb325d846080aeb02f30a2ce4276ea63b6d1f747f` | `identifiers.txt`：1 个文件，15,173 行 |
| `ticker_change_event` | `bronze` | `a8648be94aaa4f35811b54f6823f7fd19dba317ea5018cd778a0a9e888a7fe0f` | `43cdc67ca99b9f75b98e11c112da30dadfb910fdd55fd0ef55bbd1549c1f1b55` | 11,471 个 gzip，13,088 个 raw events |

两份 inventory manifest 都位于 `manifests/silver/source-inventories/ticker_events/`，并同时绑定上述
coverage receipt v2 及 commit。这保留了父表的完整 request/outcome 证据，又不会用只含成功
event 的 Bronze artifact row count 代替 15,173 个正式请求总数。

### 7.3 Lifecycle 与不可变发布

| 项目 | `ticker_event_request_status` | `ticker_change_event` |
| --- | --- | --- |
| workflow ID | `ac52e6bacf5f9cd35a0bb6e3249b32dd3c40dd120122e7dc67e6dcde09c757ed` | `6ef9c67915dd40f488349779fd3eb9a1043659a70b279315ac3d02c6f3411dbd` |
| final state | `published`, sequence 9 | `published`, sequence 9 |
| preview build ID | `32574ceaa916be2d9103d1347cea12dad4636a74d7b7c1af0a69396ee765fd8e` | `2f106b2af6cda80e67a496c7ecdd47e44ddeb2de090c3d6cf6348f536dc086cd` |
| preview manifest SHA-256 | `cf854c63852b13fe857a84b96c82d545d426b82d232833893e1d3ba7103f7c9a` | `7cafb71096a8a6be046137e1d325d7ba5d7288cd3ca9799fe02c03d269c86825` |
| full build ID | `7ff845634148274b61c2f515cb66cb9e94f8bb8a5e1abe47316343eaa9f22ca1` | `7753688e3d4f19658ca5657b2dc5ccb9bf4c4b229b3c58dc68b255d5999735d2` |
| full manifest SHA-256 | `e759aaaee11f3c99cf2a3576699291bf7f0e944c7017697ce3fbffe23013be5d` | `392a2bab8bf7112050476059d16fe95930a55175bba4eaf051e0e919895462ae` |
| publish approval ID | `6fb6849d9487a3e3b9d379a1fd753202342c5de76f38ecfa578b140ff1692bcc` | `bc146eeab79b1ec44a7ada7c6cb50af9c3977d9097105f55e74ba0112b5eeddd` |
| release ID | `afc63db6850fb50295daa8e6e499c52fe1c16b8290b7932b08aea67531ff98eb` | `18a7eb3dd6805b94151f5b6ce0167c19dbeb328f45bec7c2f806dac42b8a6350` |
| release manifest SHA-256 | `29a8c5dbe1de1fbdc819a8e8a08f998967cde2ea19c3bb56e94b34bdea9fdb11` | `34cff4cdacbdace305f5ee541c101112a5a7f7fb4e572a3c2405509cf178ba50` |
| published event SHA-256 | `f03921d461e5154a0d2099ae32c264d80c35c0f91d9a9cb8fbb9778242c8dfbd` | `4ae8ac11ce1303bbbe04a5ea85150f9d522693d19d694d2e81fe5c696439c2dc` |
| DATA SHA-256 | `fcadbe3b04d54a1c9d6c6e97649a024283b35abce0355ae9dda5f8dfad439614` | `105086359fbe28d1f69681ab875f4c508b5e2aa9a11a9209bf6fc9927b9692ea` |

发布 DATA 路径分别为：

- `silver/schema=v1/identity/ticker_event_request_status/build_id=7ff845634148274b61c2f515cb66cb9e94f8bb8a5e1abe47316343eaa9f22ca1/data/source_observed_date=2026-07-11/part-00000.parquet`
- `silver/schema=v1/identity/ticker_change_event/build_id=7753688e3d4f19658ca5657b2dc5ccb9bf4c4b229b3c58dc68b255d5999735d2/data/source_capture_date=2026-07-11/part-00000.parquet`

### 7.4 Row funnel、QA 与 quarantine

| 表 | row funnel | QA | quarantine | 分区 |
| --- | --- | --- | --- | --- |
| `ticker_event_request_status` | 15,173 输入 → 15,173 接受/OUT；11,471 complete + 3,702 404 | 24 passed + 3 approved Medium warnings | 0 | `source_observed_date=2026-07-11` |
| `ticker_change_event` | 13,088 输入 → 12,895 接受/OUT + 193 quarantine | 25 passed + 11 approved Medium warnings | 193 个精确 High issue IDs，均为空 target | `source_capture_date=2026-07-11` |

父表的 3 个 waiver 精确对应：3,702 个 `not_found_404`、2,527 个 complete response CIK 缺失、
100 个 pilot manifest 排除。子表的 11 个 waiver 精确对应：193 个空 target、2,527 个 response
CIK 缺失、766 个 `1969-12-31`、1,334 个 `2003-09-10`、480 个 `2023-11-18` cluster、481 个周末
event、2 个同 FIGI/同日多 ticker group、430 个 ticker 重用多 FIGI group、1,244 个 FIGI 多 ticker group、
4,298 个 S4 覆盖窗口前事件和 1 个 request end label 后事件。

发布后从 Parquet 重读验证：父表 `source_request_id` 15,173/15,173 唯一，子表
`source_record_id` 12,895/12,895 唯一，父子外键缺失为 0，子表空 `effective_ticker` 为 0，两表
`backtest_identity_eligible=true` 均为 0 行。发布文件的行数与 SHA 与 release manifest 完全一致。

### 7.5 资源与最终硬停

从首个 preview build 开始到子表发布的 manifest 时间窗口为约 5 分 40 秒。运行期间实时观测到的
RSS 为 489,228 KiB（约 478 MiB），低于 2 GiB 上限；这是观测值，不写成未采集的精确峰值。
发布后数据盘仍为 78G used / 109G available / 42%（`df -h` 取整精度）。

`PublishedSilverReader` 已通过两个 release ID 重放 workflow trust chain，并重验 approval、full build
manifest、release manifest 和 DATA artifact。**这是 S5 发布当时的硬停记录。S6 后来已独立发布；
当前状态以 `silver-processing-plan.md` 与 `silver-s7-identity-resolution-schema-review.md` 为准。**
