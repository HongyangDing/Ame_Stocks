# S7 Identity Resolution：可审计的 mismatch / contamination 处理方案

## 1. 结论与当前硬停点

发布前复核提出的问题成立：格式合法、当日唯一的 provider-observed Composite FIGI 仍可能是短期
provider 污染。若同一 ticker 出现 `A → B → A`，旧方案会把 B 直接提升为强身份，既可能让错误资产
进入回测，也无法在保留原始 B 的同时把研究身份映射回 A。

因此旧四份 S7 proposal 和本次复核前的中间候选均已撤回。本次只更新：

1. S7 source profile 的设计含义；
2. 一份独立 `identity_adjudication` registry contract；
3. `asset_master`、`ticker_alias`、`issuer_master`、`universe_daily` 四份 cutoff-bound contracts；
4. schema-level fixed vectors、hashes 和 QA tests。

没有实现或运行 S7 transform、fixture、preview、FullRunPlan、PublishPlan 或任何远端 S7 数据任务；没有
写入远端 staging/Silver，也没有修改 Bronze 或已发布 S1–S6 release。

## 2. 固定 Massive 证据没有改变

S7 仍固定读取六个已发布市场证据表，共 7,542 个 DATA artifacts、138,825,855 行、
15,944,020,220 bytes：

| 输入表 | Release ID | 行数 |
| --- | --- | ---: |
| `asset_observation_daily` | `26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c` | 69,381,182 |
| `asset_observation_version` | `b422fd05df859b33587b8ece80d078247dd972d01d272710ef49c3529b0e54be` | 9,706 |
| `universe_source_daily` | `c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614` | 69,376,329 |
| `ticker_event_request_status` | `afc63db6850fb50295daa8e6e499c52fe1c16b8290b7932b08aea67531ff98eb` | 15,173 |
| `ticker_change_event` | `18a7eb3dd6805b94151f5b6ce0167c19dbeb328f45bec7c2f806dac42b8a6350` | 12,895 |
| `ticker_overview_safe` | `8715f90d0e01f990e9738b9266edfeb2830a76d59a00ae4fb7490d9f077092a5` | 30,570 |

六表 binding ID 仍为
`49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64`；不会寻找“最新”
release。事实 profile 位于
[`silver/source-profiles/identity-resolution-s7-2026-07-14.json`](silver/source-profiles/identity-resolution-s7-2026-07-14.json)，
文件 SHA-256 为 `b35e7df2ceb136b7717b0c8faf36e01e83599f3425dae3e05dc76901b083f2d0`，
deterministic fact/design digest 为
`42141c3998e3ae3270b9fdf4994363edb06a3c7adb7eed6b26a161264593c04d`。

本次没有运行全历史 bounce detector，因此没有声称 suspected case 的真实数量、比例或 disposition。

## 3. 新的身份分层

S7 不再把 provider observation 和 research identity 混为一列：

| 层 | 永久保留内容 | 用途 |
| --- | --- | --- |
| observed | provider ticker、Composite FIGI、Share-class FIGI、CIK、source row lineage | 忠实记录原始输入和 mismatch |
| canonical | research Composite FIGI、asset、Share-class FIGI/ID、CIK/issuer | 只有独立佐证且无冲突时供研究使用 |
| adjudication | case、版本、证据、审批、availability、observed→canonical 映射 | 只解决一个有界 episode 的身份关系 |

FIGI 裁决不能自动解决 Share-class、CIK、重复 membership、source-lineage 或其他关系冲突。即使
observed→canonical FIGI 已确认，普通 eligibility gate 未通过时仍然无 alias、不可回测。

## 4. 闭合的控制链

```text
six exact S4/S5/S6 releases
        │
        ▼
deterministic bounce detector
        │
        ▼
content-addressed identity-case candidate manifest
        │
        ├── optional immutable external-evidence manifest
        ▼
decision-plan logical payload → exact plan bytes/SHA
        │
        ▼
row-specific approval receipt
        │
        ▼
protected append-only identity_adjudication release
        │
        ▼
six releases + exact case manifest + exact registry release + cutoff
        │
        ▼
one resolution graph → four coordinated derived tables
```

`identity_adjudication` 先独立发布，四张派生表才消费 exact registry release。未批准 decision plan
不是派生表输入。四表同时消费 exact `identity_case_candidate_manifest`，因为尚无 registry row 的 pending
case 也必须保留并 fail closed。四表仍作为一个 coordinated sibling group 构建和原子发布。

本版本不新增长期 `identity_anomaly_case` Silver 表。case manifest 是内容寻址控制 artifact；如果以后确实
需要跨 release 查询完整 case 历史，再单独提案提升为业务表。

## 5. Bounce detector 只发现，不裁决

`s7_provider_figi_bounce_detector_v1` 的 case 是 exact case-sensitive ticker 在全局 S4 source-session
spine 上的三个 maximal runs：外侧均为 A，中间为不同且有效的 B，B 最长 20 个 XNYS sessions。

以下任一情况都会打断 run，不能跨越后拼成 A/B/A：

- ticker 缺席；
- membership inactive；
- 全局 source-session gap；
- observed Composite FIGI 为 null、malformed 或发生其他变化。

每个 case ID 精确绑定六表 release binding、detector version、ticker、outer A / middle B、两侧 boundary
source-record IDs、B episode bounds 和完整 sorted-unique source-record-set digest。candidate manifest 必须
报告 1、2–5、6–20 session bands、S5/S6/层级 corroboration reason counts、case availability 和 bounded
examples。

必须看到右侧 A 才知道 A/B/A 成立，所以 `identity_case_available_session` 不得回填到 B episode。给定
cutoff：

- cutoff 早于 case availability 时，不能使用未来模式；B 仍按当时普通 direct-observation 规则处理；
- case 已可知但没有可用、已发布裁决时，B 为 `pending_unresolved`；
- 裁决和 registry release 均已可用时，才应用相应 terminal decision。

多数值、最长 run、前后值、最近值、输入顺序、收益、因子或回测表现都不能自动产生修正。

## 6. Massive 之外的网络证据

Massive 无法确认的 case 可以使用外部来源，优先官方原始材料，例如监管申报、交易所/FINRA 通知和
公司公告；第三方 identifier reference 只能作为较低等级 corroboration。

外部证据不能只是一个可变 URL。`identity_external_evidence_manifest` 必须保存：

- source authority class 和规范化 URL；
- 原始页面/API/PDF 的不可变 path、bytes 和 SHA-256；
- source publication/filing time 与 retrieval time；
- 精确支持的字段或关系 assertion；
- 按绑定 XNYS calendar 计算的 availability。

外部 evidence count、manifest ID/SHA 和 `evidence_refs` 必须可重算。其 availability 不早于来源公布时间
与实际固化时间两者；外部证据仍只进入人工 review，不能自动覆盖 provider observation 或绕过审批。
本次没有联网抓取任何外部证据。

## 7. Registry disposition 与 append-only revision

一个已发现 case 在 resolution graph 中可能处于以下状态：

| disposition | registry row | canonical | alias / eligibility |
| --- | --- | --- | --- |
| `pending_unresolved` | 无可用版本 | null | 无 alias，ineligible |
| `confirmed_genuine_transition` | approved | observed B | 仅在其余 gate 通过时有 alias/eligible |
| `confirmed_provider_contamination` | approved | outer A | 原始 B 保留；仅在其余 gate 通过时映射 A |
| `adjudicated_unresolved` | approved terminal revision | null | 无 alias，ineligible |

`adjudicated_unresolved` 是撤回先前 genuine/contamination decision 的唯一 append-only 方法。旧 row 永久
保留；不能 update/delete。每个 series 从 version 1 开始，后续 version 必须逐一引用 predecessor。

在 exact pinned registry release 和 explicit cutoff 下，只选择 availability 不晚于 cutoff 的唯一最高
完整链版本。successor 生效前 predecessor 仍有效；successor 生效后 predecessor 不再 effective，但仍可
审计。一个 S4 source row 在 terminal-head 选择后最多命中一个 episode decision。

`identity_adjudication_id` 的 exact payload 包含 series、case、version、predecessor、disposition、nullable
canonical target、evidence digest、reason code/detail 和 rule version。审批字段不进入 decision ID，以避免
receipt-subject digest cycle；receipt 反向精确绑定 decision/plan/candidate/actor/time，且一个 decision 只能有
一个 accepted receipt。

## 8. Availability 与 physical cutoff build

裁决可用日为：

```text
max(identity_case_available_session,
    evidence_cutoff_session,
    approval_available_session)
```

派生表还必须等待 exact registry release 的 publication availability。四表显式保存
`identity_resolution_cutoff_session` 和 `source_identity_adjudication_release_available_session`；所有 case、
decision、external evidence 和 registry availability 均不得晚于 cutoff。

Historical-as-known 不能拿一张后来生成的 revised 表只 mask 几列。必须选择或重新构建一套 exact cutoff
release；若没有满足 cutoff 的 registry/candidate release，fail closed。Revised retrospective build 可以把
后来批准的映射应用到历史 episode，但仍保留所有 availability lineage，不能作为历史因子信号。

## 9. 五张表的职责

### `identity_adjudication`

一行是一个 deterministic case 的一个不可变 approved decision version。包含完整 episode scope、candidate
manifest、可选 external-evidence manifest、evidence digest、reason、approval receipt、calendar、availability
和 predecessor chain。它是受保护 upstream registry，不是四表 sibling。

### `asset_master`

- asset 只由 independently anchored canonical Composite FIGI 创建；
- direct-observed 日期与 adjudication 后 canonical membership span 分开；
- genuine 和 provider-contamination adjudication 分开计数；
- pending B、contamination-only B 和 S6-only target 不能创建 eligible override asset；
- observed hierarchy counts 保留，但污染-only Share-class/CIK 不能填 canonical hierarchy。

### `ticker_alias`

- observed/canonical Composite FIGI、Share-class FIGI 和 CIK 全部分列；
- interval 在任何 observed tuple 或 lineage 变化时拆段；
- alias ID 绑定 case、decision、candidate manifest、registry release、availability 和 cutoff；
- pending 或 adjudicated-unresolved episode 不生成 alias；
- confirmed FIGI relationship 若仍有其他 conflict，也不生成 alias。

### `issuer_master`

- CIK 仍只是 issuer key，绝不是 asset merge key；
- contamination-only CIK 不创建 issuer，也不进入 lifetime、name/SIC consensus；
- 已存在 issuer 的被排除污染 evidence 有独立 count；纯 contamination-only CIK 进入 build-level funnel；
- reference availability 包括用于 include/exclude 的 case、adjudication 和 registry availability。

### `universe_daily`

- 每个 S4 active `(session_date, ticker)` row 都保留；
- observed identity/hierarchy 永久保留，canonical fields 可为 null；
- pending、adjudicated unresolved 和普通 conflicts 都保留在 membership denominator 中但不可回测；
- 这套 reconstructed active snapshot 可减少 current-universe survivorship bias，但不是 archived as-known
  universe，不能宣称消除了所有 survivorship/revision bias。

`identity_quality_liquidation_signal` 恒为 false，identity quality 不能改写 active/inactive/delist。S7 只能
输出 `identity_uncertain_no_new_trade_no_forced_exit_run_incomplete` 控制状态；已有持仓遇到身份 gap 时不
强平、不填零收益、不静默 carry stale price 的实际行为，必须在后续 backtest-engine fixture 中作为 blocker
test 验证，不能由 schema approval 代替。

## 10. Resolution matrix

| 情况 | status / method | canonical | alias | eligible | continuity |
| --- | --- | --- | --- | --- | --- |
| case 尚不可知、普通 direct B | `resolved_strong / source_composite_figi_exact` | B | 视普通 gate | 视普通 gate | 与 eligibility 一致 |
| case 已知、无可用裁决 | `unresolved / provider_figi_bounce_pending_unresolved` | null | 无 | false | run incomplete |
| genuine 且无其他 conflict | `resolved_strong / approved_genuine_transition` | B | 有 | true | resolved |
| contamination 且无其他 conflict | `resolved_approved_override / approved_provider_contamination_override` | A | 有 | true | resolved |
| confirmed 但其他关系冲突 | `resolved_conflicted / approved_*` | 已确认 target | 无 | false | run incomplete |
| approved withdrawal | `unresolved / provider_figi_bounce_adjudicated_unresolved` | null | 无 | false | run incomplete |

“confirmed”只确认 episode 的 observed→canonical 关系，不等于自动可回测。

## 11. Critical / High QA

用户要求的三个 gate 已冻结：

- `suspected_provider_figi_bounce_rows`：High warning，必须输出 reason counts、availability 和 bounded
  examples；
- `unapproved_canonical_identity_override_rows = 0`：Critical；
- `suspected_provider_contamination_eligible_rows = 0`：Critical，覆盖 pending 与 adjudicated unresolved。

同时新增或强化：

- case/series/adjudication/alias exact ID fixed vectors；
- candidate、external evidence、plan、receipt、calendar 和 registry trust chain；
- maximal episode scope、source-record-set digest、terminal revision 与 overlap；
- unadjudicated B asset creation、unanchored target、confirmed-but-conflicted eligibility；
- observed/canonical hierarchy leakage 和 contamination-only CIK funnel；
- cutoff、case/adjudication/release availability；
- membership mutation、forced-liquidation signal 和 identity exclusion coverage；
- outcome-driven identity cleaning 永远为零。

真实 detector numerator、case IDs、examples、warning waiver 和 quarantine acceptance 必须等后续 detector
run 才能产生；本次 schema review 不预先批准任何 future warning。

## 12. 固定测试边界

合同测试覆盖：

1. genuine `A → B → A`；
2. confirmed contamination `A → B → A`；
3. pending unresolved；
4. append-only withdrawal to `adjudicated_unresolved`；
5. confirmed mapping 但 Share-class/issuer 等仍冲突；
6. case availability、decision availability 和 registry publication availability 三个 cutoff；
7. observed/canonical hierarchy mismatch；
8. active membership 不变、liquidation signal=false、run-incomplete state。

这些是 schema-level decision vectors 和 digest tests，不是 transform fixture，也不能证明 backtest engine 已
执行 no-forced-exit 行为。

## 13. 新候选 contracts

| Table | columns / QA | Contract ID | Arrow schema digest | Candidate SHA-256 |
| --- | ---: | --- | --- | --- |
| `identity/identity_adjudication` | 51 / 19 | `6423cc01b952498cc78d55e93a349d7afe408bd30003e4f7be59f211102f2d5e` | `e5082a8611bedb6913f79da506f1f5cc19c94507b9e27d04edfb88566033575f` | `eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231` |
| `identity/asset_master` | 40 / 34 | `adbba0d86bd9681e034b0ffda3e380da40b6fc92d280942d856d416a1b53f868` | `827ce87a698faa903c35b93f8957f807a83caedf4936736f351adc881fa4cdc0` | `0a6dd9cb244e60723eeff625b6d82b42fc6fe882fbe0660532807054a4f717f2` |
| `identity/ticker_alias` | 44 / 44 | `384d1e5acf2181f929e29c5e3a5369a796f0ee42cdde7740b7ca3bdfdf8faf3b` | `dd79463bc022a49b65c441f3baf98a3455c06ab563bbccf22ae100ab5c787e95` | `8ef120892c5748ca51fc1242d143372237c1b5d9b92ac9f4f2585aea48fd5afe` |
| `identity/issuer_master` | 30 / 33 | `4951c0ab96fdd91b961cf4234185607e858856fb1b1ad4279b2e84d41fb2eb58` | `638f66cdb812ed657844e26c91ea7e1dcda4b27aa7ea4aedd75e94b0353c8bd9` | `6f326ae11885affb5bac37500c2006bdc845f2205d7388e2043b5504d0fb0ec8` |
| `reference/universe_daily` | 48 / 47 | `0555e785b4fb5f9df8832d37f8c08cf5fc487e8573993cf39ae3ffba4ccc45b0` | `e22cddaa57c7836f49bc21633a521f795751e473b22b4f3215b13d2e74c83b68` | `fe8d5760384322419eb28a0f8b3af6f45d52c1cbba18bc5226578fa471766701` |

候选文件：

- [`identity_adjudication.schema-v1.candidate.json`](silver/contracts/identity/identity_adjudication.schema-v1.candidate.json)
- [`asset_master.schema-v1.candidate.json`](silver/contracts/identity/asset_master.schema-v1.candidate.json)
- [`ticker_alias.schema-v1.candidate.json`](silver/contracts/identity/ticker_alias.schema-v1.candidate.json)
- [`issuer_master.schema-v1.candidate.json`](silver/contracts/identity/issuer_master.schema-v1.candidate.json)
- [`universe_daily.schema-v1.candidate.json`](silver/contracts/reference/universe_daily.schema-v1.candidate.json)

原旧四份 Contract IDs：

- `d7a6ef66f72c1048b6556b57910af3afbea4926661f3c6062708937fbc2b4ba6`
- `e645573e813c18a82fcea80f3bfef07c547c738fc00cc01419a1f1824a27a47b`
- `33c146bab2a9aed61a44d8c20e9b301fd1aa116deb5185214df92a4ee69f632d`
- `915a389ccfa9d8442cd2b7b2a14f782adf68f2bb5c8635ddf90224102750e319`

复核过程中的中间五份 IDs（`91df2789…`、`c06cedc8…`、`5d87d912…`、`3155390e…`、
`79edaa58…`）也不是待批准版本。

## 14. 下一审批点

当前状态：**combined-source profile complete；五份 revised candidates awaiting explicit re-approval；
transform code not started；no remote S7 task run**。

如批准，请逐字批准第 13 节五份 Contract ID 与 Candidate SHA-256。该批准只允许下一阶段实现：

1. source readers；
2. bounded bounce detector 与 immutable candidate manifest；
3. optional external-evidence capture contract 和 adjudication control lifecycle；
4. cutoff-bound resolution engine 与固定小样本 fixture。

实现后仍停在 code-ready / bounded review；不会直接运行十年 preview、Full 或 publish。
