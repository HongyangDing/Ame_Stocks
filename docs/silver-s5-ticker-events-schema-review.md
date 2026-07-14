# S5 Ticker Events：schema review、日期质量规则与发布证据

## 1. 范围与硬边界

S5 只把 Massive `ticker_events` Bronze 转成可审计的身份事件证据，不生成永久
`asset_id`、ticker 有效区间或回测股票池。本阶段发布的两张表都固定
`backtest_identity_eligible=false`；S4 Assets、S5 Ticker Events 和 S6 Overview 的跨源身份
解析仍由 S7 单独完成。

本次正式输入只来自
`manifests/plans/ticker_events/identifiers.txt`。100 个 audit-only pilot identifier 及其 16 个成功
响应、84 个 404 全部排除，不能混入正式 Silver 计数。S5 不请求 Massive、不修改 Bronze，也不
启动 S6。

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
`passed_with_warnings` coverage receipt。它逐条绑定 15,173 个正式请求的 manifest path、SHA、
identifier 和 terminal outcome，并逐文件绑定 11,471 个成功 gzip artifact；pilot 不在 receipt
中。后续 SourceInventory 只接受这份 receipt 作为 upstream lineage，并在读取时再次校验原始
manifest、gzip 和响应身份。

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

正式源只有约 2.53 MiB、15,173 个 manifest 和 11,471 个小 gzip 页面；因此 bounded preview
直接覆盖完整正式 inventory，而不是抽样后再扩大范围。页面样例仍限制为最多 100 条，但 data、QA、
row funnel 和 quarantine 都是 full formal scope。PreviewMetadata 明确记录
`projection_multiplier=1.0`，所以 Full build 只能在同一 source digest、同一代码 commit 和同一
参数下重算，不需要额外 FullRunPlan。

两张表的 workflow 锁步推进。只有父子计数、主键、外键、QA 和 quarantine 全部匹配已批准画像，才
允许批准 full run；发布时先发布 request-status 父表，重新读取并验证后，才发布 event 子表。

## 7. 发布证据

本节在远程 full run 完成后记录 workflow、build、release、文件 SHA、实际资源使用和最终计数。
