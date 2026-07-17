# S7 全市场 Composite FIGI reference 前置控制计划

## 1. 状态与本次边界

本文定义 S7 cross-market identity 进入全量运行前必须完成的三个独立审批门。当前只允许实现并生成
**Gate A 的 exact Plan 与 approval Request**；不生成 approval，不执行 inventory，不调用 OpenFIGI，
不运行远端 S7 数据任务，也不进入 adjudication、S7 Full 或 Publish。

现有 detector preview 的 19 个 case、89 个 suspected rows、全部 observed FIGI lineage，以及已经固化的
source-attested external evidence 继续有效且不得重写。但这 19 个 case 尚不是最终 adjudication；本计划也不
为它们生成 adjudication plan。

## 2. 为什么当前 18 个 Composite evidence 不能直接成为全市场 reference

现有 cross-market evidence manifest 固定了 9 个 Share Class group 中 18 个 Composite FIGI relationship：
每组一个 canonical US Composite 和一个 observed foreign Composite。它足以支持这 9 个 ticker 的人工结论、
固定 fixture 和 candidate schema review，但不具备以下全市场性质：

1. 它由 detector 命中的 9 个 ticker 反向抽样，不是从完整 S4 Composite domain 建立的 denominator。
2. 它没有证明 S4 十年记录中的每一个 distinct Composite 都已尝试查询和分类。
3. “不在这 18 行里”只表示未被本次 evidence 覆盖，不能推断为 US、clean 或不存在 cross-market 风险。
4. bounce detector 只能看到短期 `A→B→A`；长期保持同一个 foreign Composite 的 US-locale 序列可能完全
   没有 bounce，仍会污染 canonical research identity。
5. OpenFIGI 返回可能是 no mapping、ambiguous、多个 security、Share Class 冲突或暂时不可用；这些状态必须
   作为 attempted result 固化，不能被成功映射的 18 行掩盖。

因此，当前 evidence 只能作为未来 full reference 的 seed 和回归 fixture。只有先确定完整 inventory，再对
inventory 做 100% attempted coverage，才有资格声称全序列 market-consistency scan 使用了全市场
reference。

## 3. 三门控制流

```text
Gate A: full S4 Composite inventory
    ↓ inventory release reviewed and approved
Gate B: 100% attempted composite_market_reference_release
    ↓ coverage and unknown dispositions reviewed and approved
Gate C: full S4 market-consistency scan
    ↓ findings reviewed; only then may a separate adjudication gate be proposed
S7 Full / Publish
```

任一门只授权本门的不可变产物，不向下一门传递隐式执行权限。Gate A、B、C 均需独立
`plan → request → literal approval → execute once → awaiting_review → review/publish` 控制链；后续门的
Plan 只能绑定前一门已批准的 release ID、manifest SHA、row count 和 content digest。

### 3.1 Gate A — full S4 Composite inventory

目的：从完整、已发布的 S4 parents 枚举 provider observed Composite FIGI domain，并冻结一个可审计
denominator；不做 US/foreign 分类，不生成 canonical target，不应用 override。

主输入是 lossless `asset_observation_daily`；`universe_source_daily` 用于核对 selected membership 和
完整 session/ticker projection。执行时必须 streaming 扫描，不把 138M parent rows 一次性放入内存。
每个 non-null Composite 至少聚合：

- exact observed `composite_figi`；
- observed `share_class_figi` 集合及冲突状态；
- 首末 session、active/inactive row counts、session/ticker/provider-locale/market/MIC counts；
- parent table、release、partition、source-record lineage digest；
- null、malformed、Share Class conflict 等不能进入 identifier inventory 的独立 reason counts 和 bounded
  examples。

Gate A 输出不得包含 `market_class=us/non_us`、canonical FIGI、eligibility 或 adjudication disposition。
distinct inventory 的实际 row count 与 digest 在批准执行并 review 前均为 unknown；不得用现有 profile 的
近似或不同 grain distinct count替代。唯一预先冻结的 output cardinality hard cap 是 **100,000**，超过即
fail closed。

#### Exact source binding 与 caps

| 项目 | Exact value |
| --- | ---: |
| session range | `2016-07-11` 至 `2026-07-09` |
| sessions | 2,513 |
| `asset_observation_daily` release ID | `26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c` |
| `asset_observation_daily` manifest SHA-256 | `f5fb26e75f44382caddf980e8fdf88a77903465b55bfd367f8d9029852848084` |
| asset rows / artifacts / stored bytes cap | 69,381,182 / 2,513 / 8,248,987,847 |
| `universe_source_daily` release ID | `c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614` |
| `universe_source_daily` manifest SHA-256 | `6b2c6ca1b612c4c38ddc8e359c1402c177a4f19b0295604d42b78bcd5804596d` |
| universe rows / artifacts / stored bytes cap | 69,376,329 / 2,513 / 7,661,290,322 |
| total scanned rows cap | 138,757,511 |
| total source artifacts cap | 5,026 |
| total source stored bytes cap | 15,910,278,169 |
| distinct inventory rows hard cap | 100,000 |
| distinct Composite / Share Class pairs hard cap | 250,000 |
| output bytes hard cap | 268,435,456 bytes (256 MiB) |
| temporary bytes hard cap | 4,294,967,296 bytes (4 GiB) |
| process RSS hard cap | 2,147,483,648 bytes (2 GiB) |
| streaming batch size | 65,536 rows |
| worker count | 1 |
| wall-clock hard cap | 14,400 seconds (4 hours) |
| S4 release-set ID | `f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d` |
| S4 release-set manifest SHA-256 | `937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13` |

这些是 metadata preflight 与 runtime hard caps，不是估算值。任一实际 parent rows、artifact count、bytes、
date coverage、release/manifest digest 不相等，或 RSS/output cardinality 超限，都必须在写 candidate
inventory 前失败。Gate A 只读 parents，不改 S4 release，不访问网络。

当前 checkpoint 只生成绑定上述 exact values 的 Gate A Plan 和 Request，状态停在等待 literal approval；
不得创建 approval/authorization、调用 runner、生成 inventory candidate/release 或变更远端数据。

### 3.2 Gate B — 100% attempted `composite_market_reference_release`

目的：对 Gate A release 的每一个 valid distinct Composite 做一次可审计的 market-classification attempt，
并发布完整 denominator，而不是只发布成功映射。

最低输出 grain 为 `inventory_release_id + observed_composite_figi + reference_version`。每行必须保存：

- attempt status：`known_us`、`known_non_us`、`no_mapping`、`ambiguous`、`share_class_conflict`、
  `source_unavailable` 或等价的显式封闭枚举；
- OpenFIGI 原始 request/response bytes、path、SHA、HTTP metadata、capture time、publication/availability；
- 返回的 FIGI、Share Class、exchange/market sector/security type 和用于分类的 exact evidence projection；
- inventory parent digest、reference build/release ID、classification rule version 和 row-level lineage；
- 当前 18 个 relationship 所绑定 external manifest 的 seed/replay 结果；TNXP no-self-row、CR nullable
  non-target 等 API 例外仍以原始 evidence 为准，不补造 response row。

`attempted_coverage = attempted_inventory_keys / inventory_keys` 必须精确为 100%。attempted 不等于
resolved：no mapping、ambiguous、conflict 和 unavailable 均保留为 unknown disposition，不能删除或标为
clean。Gate B 的 API batch/request/bytes/重试/resource caps 只能在 Gate A actual inventory count/digest
review 后冻结，本文不提前猜测，也不授权调用 API。

### 3.3 Gate C — full market-consistency scan

目的：把已批准的 full reference 投影到完整 S4 序列，发现所有 US-locale cross-market observations，
不再依赖 bounce。

扫描 grain 至少覆盖每个 selected `session_date, ticker` 的 exact observed Composite、Share Class、locale、
market、primary exchange、active 状态和 source lineage。检测器必须：

1. 报告 `locale=us` 且 reference 为 `known_non_us` 的全部行，按 reason、ticker、Share Class、Composite、
   MIC 和连续日期段聚合，同时提供 bounded examples；
2. 发现没有任何 FIGI 跳变、但长期保持 foreign Composite 的序列；XNAS/XNYS 可加强 reason，但不能成为
   唯一判断依据；
3. 正确识别 `foreign→US→foreign` inverse case：中间 US observation 保持 direct/eligible，不得标为
   genuine transition；
4. 对 unknown reference fail closed：membership row 保留，canonical/alias 为 null，
   `backtest_identity_eligible=false`，但不推断 inactive/delisted，也不发出强制平仓信号；
5. 输出 findings 供后续 group-level/cross-market adjudication review，不自动生成或批准 override。

Gate C 至少冻结以下 QA：

- `us_locale_non_us_composite_figi_rows`：High，带 reason counts、完整 numerator 和 bounded examples；
- `unapproved_cross_market_composite_eligible_rows = 0`：Critical；
- `inverse_bounce_misclassified_as_genuine_transition_rows = 0`：Critical；
- `reference_inventory_unattempted_rows = 0`：Critical；
- `unknown_reference_backtest_eligible_rows = 0`：Critical；
- `identity_quality_membership_mutation_rows = 0`：Critical；
- `identity_quality_forced_liquidation_signal_rows = 0`：Critical。

## 4. Unknown 必须 fail closed

unknown 包括未尝试、source unavailable、no mapping、ambiguous、Share Class conflict、无有效 external
availability 或 reference binding 不完整。规则是：

- 未尝试是 Gate B release blocker，不能通过 warning waiver 变成 100% attempted；
- 已尝试但 unresolved 可以作为显式 unknown 保留并进入人工 review，但不能获得 canonical alias 或
  backtest eligibility；
- unknown 不等于 foreign，也不等于退市、inactive 或不可交易；它只阻断 identity-dependent backtest；
- 后续补证只能追加新 reference/adjudication version，并按 availability 生效，不能改写旧 release。

## 5. Foreign locale 隔离

Composite market classification 可以作为全局 identifier fact 被 reference，但 canonical override 不是全局
替换。任何 cross-market override 必须精确绑定 provider、`locale=us`、ticker、Share Class、observed
foreign Composite、canonical US Composite、有效日期/版本、source release、external evidence、approval 和
availability。

因此，同一个 foreign Composite 出现在真实 foreign provider locale/market 时仍是合法 observed/canonical
identity；US-scoped override 不得泄漏过去。测试必须同时包含：

- US locale：`US→foreign→US`；
- US locale：`foreign→US→foreign` inverse；
- US locale 且 XNAS/XNYS：长期稳定 foreign Composite；
- foreign locale：同一 foreign Composite 保持 direct，零 override、零 contamination finding。

## 6. 当前 resolver 的 production blocker

当前 resolver 对 reference 中不存在或 unavailable 的 observed Composite 可产生 `not_classified`，而 direct
decision 的 eligibility 仍可能只由 ordinary active/conflict gate 决定。这意味着 unknown 有机会继续
eligible，是进入 production 的明确 blocker。

**Gate A 不修改 resolver，也不解决这个 blocker。** Inventory 只建立 denominator，不能被解释成“所有
identifier 已分类”或“unknown 已安全处理”。在 Gate C 或任何 S7 Full/Publish 前，resolver/schema 必须
实现并测试：unknown canonical/alias 为 null、`backtest_identity_eligible=false`、membership 不变、无
identity-quality forced liquidation；相关 Critical QA 必须为零。

## 7. 本 checkpoint 的允许与禁止动作

允许：

- 更新 Gate A 的本地代码设计、固定测试和 schema proposal；
- 生成一份绑定第 3.1 节 exact source/caps 的 immutable Gate A Plan；
- 生成与该 Plan ID/SHA、resource caps digest 一致的 approval Request；
- 展示 Plan ID/SHA、Request event ID/SHA 和 caps digest 后停下等待用户 literal approval。

禁止：

- 执行 Gate A inventory 或读取远端 parent data；
- 调用 OpenFIGI、SEC 或 issuer 网络资源；
- 生成 Gate B/C plan、reference release、full-sequence findings 或 adjudication plan；
- 改写 19 个 preview cases、89 个 rows、raw observed FIGI 或任何 external evidence bytes；
- 进入 S7 Full、Publish，或把当前 19 个 case 当作最终 adjudication。
