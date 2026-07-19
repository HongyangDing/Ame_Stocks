# S7 exact-group 全历史事实审查：冻结的 review-only schema package

## 1. 当前结论与授权边界

Directional raw preview 已证明 SOR、XZO、ANABV 三案的观测方向，但 11 个抽样 session 不能证明
provider 映射在完整 S4 历史中的连续范围。下一步因此是 **exact-group full-history review**：物理上检查完整
S4 release，逻辑上只保留三个已批准的精确 group。

本 repository package 冻结合同、scope、事实语义、纯内存 fixture，以及后续可能执行的 fail-closed
control/runner 字节；**代码存在不等于任务已经获批**。当前没有任何本 package 的 Preparation Approval、
manifest-only Approval 或 Execution Approval，也没有读取本地或远程 Parquet。它同样不能生成 external
evidence、adjudication、registry、canonical identity、asset transition、tradability、Full 或 Publish 产物。

## 2. 精确 scope 与完整物理范围

逻辑 scope 固定为：

| Provider / market / locale | Ticker | exact observed Composite FIGI |
| --- | --- | --- |
| `massive / stocks / us` | `SOR` | `BBG000KMY6N2` |
| `massive / stocks / us` | `XZO` | `BBG01XL8FHT0` |
| `massive / stocks / us` | `ANABV` | `BBG021DMXXT2` |

固定 scope digest：

`b2c88ba3ce02ae0618206da35cf535c03b3dfdbca67edd0474a96165cbae28f2`

`review_group_id` 使用 control-plane scope 的五字段 canonical payload
`{locale, market, observed_composite_figi, provider, ticker}` 计算 stable digest：

- SOR：`844d92c0d58dabe60608cc2b37e6c69ea007308a4dc69fca07c9c86756a66335`；
- XZO：`31611a44b1102e0622c5ee9d720da591987b44f82fe9e35e7cb3cc14b68c770e`；
- ANABV：`20b28d71fcce26779c50d1d17cd6472fbbd9406b750f4d8e83d09a06c0887718`。

物理输入必须是当前冻结的完整 S4 release，而不是按猜测日期截取：

- XNYS：`2016-07-11` 至 `2026-07-09`，2,513 个 sessions；
- `asset_observation_daily` + `universe_source_daily`：5,026 个日分区；
- 物理源行数：138,757,511；
- 物理源 bytes：15,910,278,169；
- S4 release-set ID：`f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d`；
- release-set manifest SHA-256：
  `937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13`。

逻辑过滤字段只有 provider、market、locale、ticker、observed Composite FIGI。**Share Class FIGI 禁止参与
过滤**；它正是本次需要完整观察的变化字段。不能因 Share Class、active、selected-parent 或未来 canonical
判断删除任何 exact-group Asset version。

## 3. Slot grain 与 lossless evidence

Table：`identity_exact_group_history_review_slot`

Grain：每个 `review_group_id, session_date` 一行，但仅输出该 session 实际存在至少一个 exact-group
`asset_observation_daily` row 的 session。没有 exact-group Asset evidence 的日期不制造空 slot，也不填成
inactive。

同一 session 的所有 matching Asset versions 必须：

1. 使用 ProviderRowAttestation v2 保留 full-row snapshot 和物理 locator；
2. 全部进入 per-group immutable evidence manifest；
3. 以 canonical JSON 的 attestation ID 数组在 slot 中对账；
4. 不按多数、最近、selected version 或 Share Class 去重；
5. 将 Share Class、CIK、MIC、type、provider-active 的 distinct observed sets 原样报告。

固定合同标识：

| 项目 | 值 |
| --- | --- |
| Contract ID | `cdf406e869c06c2942588a043f6e50dd429f1d6a8818d05e4d01a75fb8a92765` |
| Arrow schema digest | `3ba74162c4903cef843496acc49d47198b1cc09f0206158b0ae065da38415400` |
| Candidate/resource SHA-256 | `ae957aeb2b61e7970eadcf2e963b7ae48ff2be6f4582901f1b9d26c7ff31b80c` |
| QA semantics digest | `837d03c92707590d505a5ea683760eb1448073a213abf07af4a6501ad263ce49` |

Candidate 与 packaged resource 必须 byte-for-byte 相同。

## 4. Universe selected-parent reconciliation

`asset_observation_daily` 是 lossless authority；`universe_source_daily` 只提供同 ticker/session 的 membership
与 selected-parent 对账。Universe lookup 不使用 Composite 或 Share Class 过滤，允许 0 或 1 行：

- 0 行：slot 仍因 exact-group Asset evidence 存在而保留，标记
  `membership_status=absent_source_membership`；
- 1 行：`selected_source_record_id` 必须在同 ticker/session 的完整 Asset rows 中唯一命中 parent，并通过
  S4 projection reconciliation；
- selected parent 可能不是本 exact group 的 Composite。此时 parent attestation 与 observed fields 仍保留，
  `selected_parent_matches_exact_group=false`，作为 High/review 事实；
- nonselected exact-group versions 全部保留，不因 universe selection 被丢弃。

`active_on_date` 与 provider-active 仅是 source membership/observation 事实，不是最终交易资格。Identity quality
不得改变 membership，也不能产生 forced-liquidation signal。

## 5. Exact observed runs 只表示观测事实

对每个 review group，将实际出现 exact-group Asset evidence 的 sessions 按冻结 XNYS calendar 排序。只有前后
两个 observed sessions 在 XNYS 上相邻时才属于同一 maximal run；任何缺口都必须断开新 run，不能插值。

Slot 保存 previous observed session、XNYS adjacency、run ID/ordinal、run 内 ordinal、observed start/end/count，
以及 group first/last/count summaries。所有行固定：

```text
observed_interval_state = exact_full_release_observed_runs_only
registry_evaluation_state = not_evaluated
```

Observed run 的 start/end 只表示“源数据中实际观察到的首尾 session”。即使完整 release 中连续存在，也不能
直接成为 `effective_from/effective_to`，不能证明 release 之外的边界，也不能自动形成 provider override。
固定 observed-run semantics digest：

`70dfc56002b731b9ddde53c0febaf5b1d75bc1b316387d3f6850ec3cb96f259e`

## 6. QA surface

33 个 Critical gate 固定覆盖：

- 三组 exact scope、inventory/directional-preview upstream bindings、full S4 release/calendar/artifact/count；
- no scope leakage、所有 exact-group Asset versions 无遗漏、每个输出 session 必有 Asset evidence；
- Share Class 绝不参与过滤；
- universe 唯一性、selected parent 唯一命中和完整 projection；
- ProviderRowAttestation v2、物理 replay、无 orphan/duplicate、observed row 无 mutation；
- XNYS run segmentation、run metadata/group summaries 精确；
- membership 无 mutation、identity quality 不产生 forced liquidation；
- observed-only state，无 interval inference、registry resolution、canonical/adjudication/transition/tradability；
- 所有 capability markers 为 false；PK、sort、immutable readback、resource caps。

12 个 High/review 指标固定报告 reason counts 与 bounded examples：

- nonselected exact-group versions；
- exact-group Asset-only sessions；
- selected parent 为其他 Composite；
- same-session identity variants；
- Share Class、CIK、MIC、type、active 的 observed change edges；
- exact observed run gap edges、多 run groups；
- first/last observed session 触及 S4 release boundary 的 censored groups。

High 非零不会触发静默修正。它只进入 review，不能转化为 canonical 或 registry decision。

## 7. Capability boundary 与后续顺序

合同、Preparation Plan 与 manifest-only Plan 在人工批准前的所有能力开关均为 false，包括 exact-group
execution、external evidence、override interval、canonical identity、adjudication、transition、tradability、
registry materialization、Full 和 publication。仓库中的 future-executable runner 只有在下面三段不可变控制链
依次完成后才可能获得一次性 source-read 能力：

1. Preparation Plan/Request 冻结 clean Git commit/tree、完整 runtime/verification file pins、合同、三组 scope、
   upstream lineage 与资源上限，停在第一份 exact literal；
2. Preparation literal 单独批准后，生成 manifest-only Plan/Request；第二份 exact literal 批准后只读取 7 份
   固定 JSON 并对 5,026 个 Parquet 路径执行 `lstat`，内容读取必须为 0 bytes；
3. manifest-only run 生成 raw-16、inventory-8 与 normalized-10 三个独立投影并验证
   `normalize(raw) == execution pins`，再生成 Execution Plan/Request，停在第三份 exact literal；只有第三次
   批准才允许一次性完整扫描并停在 `awaiting_review`。

三段控制共同绑定：

1. 本 schema 的 Contract ID、schema digest、candidate SHA；
2. exact inventory candidate/completion；
3. 已完成的 directional raw-preview candidate/completion；
4. 完整 S4 release 的 5,026 个 source refs，且沿用 raw/normalized digest 分域校验；
5. 固定 XNYS calendar、资源上限、clean Git commit/tree 与每个 executable/test file pin。

执行器必须先持久化 immutable execution intent，随后才可打开、读取或哈希任何 Parquet；完成态重试只校验
control/candidate/output bytes，不再触碰 S4 source。预期 candidate 只包含：

```text
data/review-slots.parquet
evidence/review_group_id=<id>/manifest.json  # 恰好三份
review/group-sequences.json
qa/qa.json
examples/review-anomalies.json
manifest.json
```

真正执行后也只能生成 review candidate/completion 并停在 `awaiting_review`。Review 完成后才可以独立固化
SEC/issuer/OpenFIGI evidence；再之后才设计 `provider_composite_override`、`share_class_adjudication` 与
`asset_transition` 的 schemas 和 decision plan。本 package 本身不提前实现任何上述步骤。
