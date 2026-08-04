# S7 完成后、S8 开始前：Silver 增量架构改造计划

## 0. 决议、时点与边界

本计划定义一个位于 S7 与 S8 之间的架构检查点，简称 **S7.5**。它不是新的 Bronze family，
也不改变 S1–S7 已发布或正在生成的事实。目标是在 S7 建立十年身份基线后，把正式 Silver
从“每次补数都重放全历史和完整审批链”改造成“正常按日追加、异常有界修正、定期全量对账”。

执行时点固定为：

1. 当前 S7 四表 Full/Publish 完成并验收；
2. 将该 S7 release set 冻结为增量体系的只读 base release；
3. 完成本文的合同、实现、影子对账和切换验收；
4. 通过 S7.5 检查点后才正式开始 S8 `ipos`。

截至 2026-08-04，S7 四表已完成 Full、Publish 和 exact release-set 验证，用户已明确授权正式进入
S7.5。当前授权覆盖 I0 基线冻结以及 I1 本地合同/纯解析器实现；**不授权读取远程 Parquet、运行真实
增量任务、发布新的数据 release 或开始 S8**。I1 完成后必须停在 Gate A 等待审批。实施期间不得删除、
覆盖或重写 Bronze、S1–S7 build、registry decision、quarantine、approval、release 或审计证据。

## 1. 核心判断

S7 的身份语义安全边界必须保留，但现有控制与执行方式不应成为日常补数模板。

必须保留：

- Bronze 不可变、source lineage 完整且可定位到原始 artifact；
- provider-observed identity 与 canonical research identity 永久分离；
- permanent `asset_id`、ticker 有效区间和 point-in-time availability；
- identity adjudication append-only、版本化、带证据和审批；
- 多 registry Composite 冲突 fail closed，不按优先级、最近值或多数值自动选择；
- identity quality 不等于 inactive/delisted，也不能独自触发强制平仓；
- quarantine、Critical/High QA、内容寻址、原子发布和可回滚 reader；
- 日频和分钟频数据继续按 session 分区，研究层只消费固定 release。

第一优先级不是给现有 runner 增加 checkpoint，而是先修正 snapshot-scoped identity。当前
`ticker_alias_id` 的生成 payload 包含 `identity_resolution_cutoff_session`；如果沿用该合同，单纯把
cutoff 从 N 日推进到 N+1 日，就可能让历史 alias ID 和引用它的旧 membership 发生逻辑漂移。因此 v2
必须拆成：

- 稳定的 `alias_segment_id`：只由 provider/market/locale、ticker、observed identity、segment 起点和
  不可变 source subject 确定；canonical asset/FIGI、release 与 cutoff 均不得进入该 ID；
- 版本化的 `alias_resolution_version_id`：绑定 identity cutoff、policy bundle、evidence availability 和 release；
- `universe_daily` 按 partition 保存所采用的 resolution version lineage；
- 后来获得的证据只能生成新 resolution/correction version，不能冒充历史时点已经可用。

需要瘦身：

- 正常日更不再逐层创建 `candidate → plan → request → receipt → intent → release`；
- 同一控制 DAG 在一个进程中只验证一次，不在每个 loader 中递归重放相同证据；
- reader 不因当前 Git commit 与历史 writer commit 不同而拒绝合法旧 release；
- 新增一个 session 不再修改硬编码 session/row count，也不重跑十年 S4/S7；
- 干净日更不需要人工批准 preview、FullRunPlan 和 PublishPlan；
- 五类 identity registry 保持职责分离，但通过一个原子的 identity-policy bundle 对下游可见。

## 2. 当前增量障碍

现有 S7 Full 是建立初始基线的安全实现，但有以下日常运行障碍：

1. 输入固定绑定完整 S4 release、全部 session receipts 和总行数；新增一天会改变整套 source binding。
2. 四表物化先扫描全部 membership 生成 resolved partitions，再读取全部 resolved partitions 回填 alias。
3. S4/S7 build 使用新的 `build_id` 从历史起点重建，已有 session partition 不能作为已验证父版本复用。
4. registry 的 append-only 语义适合增量，但 production control chain 对每个 registry 重复构建和验证。
5. release 只描述完整 outputs，没有 `parent_release_id`、added/replaced partition 或 supersession map。
6. 断点恢复主要服务同一次 Full staging，不能直接把已发布 release 作为下一次补数 checkpoint。
7. schema/transform provenance 与 orchestration/runtime provenance 绑定过紧；修复 verifier 不应等同于改变数据语义。

如果不改造，即使只新增一个交易日，也可能触发新的 S4 全量 build、Gate/registry source rebinding、
S7 两遍全历史物化和完整 Publish 链。该成本不适合日频更新，也会阻塞 S8 以后新增上市证券。

## 3. 目标架构：base release + delta release

### 3.1 Release 类型

正式 Silver 增加三种 release 类型：

| 类型 | 用途 | 是否改写父版本 |
| --- | --- | --- |
| `base` | S7 初始十年基线，或周期性全量重建 | 否 |
| `delta` | 正常新增 session、master/alias 尾部变化 | 否 |
| `correction` | 有界历史修正、late data、adjudication 生效 | 否 |

每个非 base release 必须绑定唯一 `parent_release_id`。Reader 从被明确批准的顶层 release 开始，
确定性解析父链，得到一个 resolved snapshot；不得扫描目录寻找 `latest`。

体系同时保留两个明确视图，不能混用：

- `historical_as_known`：只使用对应研究时点已经 available 的 identity/evidence；
- `latest_reviewed_research`：使用当前已批准的 retrospective corrections，但仍完整标注其真实 availability。

回测默认视图必须由策略运行参数和 Gold manifest 显式绑定，不能由 reader 猜测。

当前首次 S7 base 明确定义为其发布 cutoff 下的 `latest_reviewed_research` 基线；它不被追认成每个历史
session 当时已经知道的 `historical_as_known`。V2 Full 与每个增量 partition 都必须保存该 partition 实际
采用的 source cutoff、identity/evidence cutoff、policy bundle 和 resolution version，禁止 Full runner 用一个
新的全局 cutoff 覆盖所有旧 partition lineage。

当前 S7 v1 release set 先作为不可变等价性 oracle，而不是被静默包装成已经具备 v2 FK 的生产父版本。
Gate A 必须在“一次性生成并对账 v2 base”与“显式 legacy adapter”之间作出迁移决议；在此之前不得让
v2 delta 直接引用 v1 `ticker_alias_id`，也不得把 mixed-schema reader 当作既成事实。

### 3.2 最小 release manifest

新的 release manifest 至少包含：

```text
release_id
release_type
parent_release_id
schema_digest
transform_semantics_digest
identity_policy_bundle_id
calendar_digest
source_cutoff
availability_cutoff
added_partition_receipts[]
replaced_partition_receipts[]
superseded_partition_receipts[]
added_rowset_receipts[]
superseded_row_version_ids[]
state_checkpoint_receipt
qa_receipt
writer_runtime_provenance
created_at_utc
```

规则：

- `release_id` 由确定性的 `release_identity_payload` 计算；该 payload 包含父版本、schema/semantics/policy、
  source bindings、partition/rowset receipts、checkpoint 和 QA，但不包含 wall-clock 时间或非语义 runtime metadata；
- 完整 manifest envelope 另有 `manifest_sha256`，覆盖 `created_at_utc` 和 runtime provenance；同一次 durable
  run 的 retry 必须复用首次冻结的时间与 envelope metadata；
- added/replaced/superseded 必须按 table、partition key 和 receipt digest 明确列出；
- `universe_daily` 使用 partition receipts；`ticker_alias`、`asset_master`、`issuer_master` 使用 append-only
  rowset receipts 和显式 row-version supersession，不把行级变化伪装成整表 replacement；
- replacement 只能遮蔽父链中的同一逻辑 partition，旧 bytes 和旧 release 永久保留；
- 不允许以“文件消失”表达删除；必要的删除语义必须由显式 tombstone/correction record 表达；
- 同一 resolved snapshot 中，一个 table/partition key 只能解析到一个有效 receipt；
- 同一 resolved view 中，一个 stable row key 在给定 cutoff 只能解析到一个 terminal row version；
- clean append 不得改变父 release 中既有 `universe_daily` partition receipts；
- 父链超过约定深度后可创建新的 checkpoint base，但必须逐 partition 与父链 resolved snapshot 对账；
- release manifest 是权威；checkpoint 是可重建加速状态，不能成为不可追溯的新事实源。

### 3.3 Digest 职责分离

必须拆分以下绑定：

- `schema_digest`：Arrow schema、PK、排序、nullability 和枚举；
- `transform_semantics_digest`：会改变行内容或研究含义的算法；
- `identity_policy_bundle_id`：cutoff 下五类 registry 的原子选择；
- `source_binding_digest`：实际输入 artifact/partition receipts；
- `writer_runtime_provenance`：Git commit、依赖和执行环境，仅用于复现与兼容性判断；
- `orchestrator_digest`：控制程序实现，不自动决定数据是否需要重算。

Schema、transform 或 identity policy 改变时必须重新计算受影响范围。仅 verifier、日志、CLI 或
orchestrator bugfix 改变时，允许在通过兼容性测试后继续读取旧 release，不能把当前 runtime 与历史
writer byte-for-byte 相同当作永久读取条件。

## 4. 三条运行通道

### 4.1 通道 A：正常按日追加

用途：新增一个或一小段连续交易日，provider 未回改历史数据，未出现未解决的 High/Critical identity 问题。

流程：

1. Bronze 下载器只追加并验证新的 session artifacts；已有完整 request/object 直接幂等跳过。
2. S4 只转换新增 session，输出新的 `universe_source_daily/session_date=...` partition。
3. 使用父 release checkpoint、冻结 calendar 和已发布 identity-policy bundle 解析新 membership。
4. Identity detector 只检查新 session 加固定 boundary lookback；lookback 参数进入 transform semantics。
5. `universe_daily` 只新增 session partition；不得读取或重写无关历史 partition。
6. `ticker_alias` 只处理父 checkpoint 中的 open tail：保持、关闭或新开 interval；延长或关闭时生成
   新的 resolution version，稳定 `alias_segment_id` 不变，父 release 的 alias row 不就地覆盖。
7. `asset_master`、`issuer_master` 只合并本次出现的 asset/issuer delta，并保留 first/last seen lineage。
8. 写入 checkpoint、QA 和 delta manifest；所有自动发布条件通过后原子发布。

自动发布条件：

- schema/transform/identity policy/calendar 与父 release 兼容；
- source session 连续，或缺口有显式 market-calendar explanation；
- source omission/duplication/mutation 为 0；
- identity registry collision eligible/resolved/alias rows 为 0；
- unknown/unapproved foreign identity 不可 eligible，且无 alias；
- alias overlap/gap interpolation/missing eligible alias 为 0；
- observed lineage、active/inactive 和 forced-liquidation invariants 全部通过；
- 无未豁免的 Critical/High issue；
- 磁盘、RSS、运行时间和输出 bytes 未越过冻结资源上限。

发现新 ticker 或未解决 identity 时，membership 仍保留，但 `backtest_identity_eligible=false`、alias 为空，
并进入 review queue；不得为了让整个日更成功而猜测 canonical identity。只要该行保持 fail-closed 且其他
Critical invariants 通过，是否允许其余干净行发布由正式合同在 Gate A 决定。

### 4.2 通道 B：有界历史修正

用途：provider 回改、迟到数据、新 adjudication、ticker/Share Class/Composite 修正或历史 QA 问题。

流程：

1. 先冻结 exact affected scope：provider、table、ticker/asset、date interval、source release 和原因；
2. 保存新旧 source receipts 与差异，不覆盖父 release；
3. 生成 replacement partitions；
4. 按 session 物理分区意味着修正一个 ticker 时仍须读取并原子替换受影响日期的整份 partition；未受影响
   行必须在 canonical projection 上 byte-equivalent，不得触碰无关日期 partition；
5. 从最早受影响 session 重算 alias/identity state，直到证明到达首个稳定边界；
6. 若稳定边界无法证明，扩大到该 exact ticker/group 的历史，不能自动扩大成全市场 Full；
7. 发布 `correction` release，显式列出 replaced 与 superseded receipts；
8. 人工审批所有 canonical identity override、历史 replacement 和 warning waiver。

Identity adjudication 继续使用现有职责互斥语义：

- `identity_adjudication`：bounce episode；
- `identity_cross_market_adjudication`：跨市场 Composite 污染；
- `provider_composite_override`：真实 transition 后的同市场 stale Composite；
- `share_class_adjudication`：只修正 Share Class；
- `asset_transition`：只表达 predecessor/successor relation。

它们各自 append-only，但下游只消费一个原子的 `identity_policy_bundle_id`。同一 source row 命中多个
Composite correction 仍必须 unresolved/ineligible，不能由 bundle 层发明优先级。

每个新 source release 或 registry bundle 必须先计算 deterministic impact set。若 impact set 无法有界证明、
alias 稳定边界无法找到、或变更涉及 schema/transform semantics，则自动降级到通道 C，不能用一个过宽的
correction 伪装成增量更新。

### 4.3 通道 C：周期性全量重建与对账

用途：月度/季度审计、schema/transform/calendar 改变、严重 source correction，或人工触发的灾难恢复演练。

流程：

1. 从不可变 Bronze 和固定 registry cutoff 独立生成新 base candidate；
2. 完整执行 S4/S7 历史扫描、Critical QA 和所有 output receipts；
3. 将其 resolved snapshot 与当前 base+delta+correction 父链逐 table/partition 对账；
4. 差异必须分类为预期 correction、明确算法版本变化或 blocker；
5. 对账通过并经人工批准后，才把新 base 设为后续 delta 的父版本；
6. 旧父链保持可读，不删除、不 reset、不就地 compact。

周期 Full replay 保留审计价值，但从日常更新路径移出。

## 5. 增量 checkpoint

每个发布后的 resolved snapshot 生成一个内容寻址、可重建的 checkpoint，至少包含：

- last completed source/session/availability cutoff；
- S4 source release/partition terminal receipts；
- 每个 ticker 的 open alias interval 与 canonical/decision key；
- asset/issuer bounded aggregates 和 first/last seen provenance；
- 五类 registry 的 terminal version map 与 bundle ID；
- calendar/schema/transform digests；
- unresolved review subjects 和最早受影响 session；
- resolved table/partition receipt map。

Checkpoint 规则：

- 必须能从父 release manifests 和正式 partitions 重建；
- checkpoint 损坏时任务 fail closed，但不能损坏父 release；
- 同一 input + checkpoint 重跑产生完全相同的 receipts 和 release ID；
- checkpoint 不能包含未发布 decision，也不能把未来 availability 回填到历史；
- registry bundle 更新后，只重算实际受影响 subject/date scope。

## 6. 四表的增量语义

| 表 | 正常追加 | 历史修正 |
| --- | --- | --- |
| `universe_daily` | 每个新 session 新增一个 partition | 替换明确 session partition，逐行保留 source lineage |
| `ticker_alias` | 为 open interval 追加新 resolution version，稳定 segment ID 不变；或创建新 segment | 从最早受影响日重算到稳定边界，禁止跨 gap 插值 |
| `asset_master` | 为本次出现 asset 写 copy-on-write 聚合 delta，稳定 asset ID 不变 | 仅更新受影响 asset，transition edge 仍来自正式 registry |
| `issuer_master` | 为本次出现 issuer 写 copy-on-write 安全字段 delta，稳定 issuer ID 不变 | 仅按独立 issuer/CIK 证据修正，不受 FIGI override 越权影响 |

三张非 session 表采用以下确定性 overlay：

- `ticker_alias` stable key 为 `alias_segment_id`，row version key 为
  `(alias_segment_id, alias_resolution_version_id)`；successor 只 supersede 同 stable key 的旧 resolution version；
- `asset_master` stable key 为 `asset_id`，每次聚合变化产生内容寻址 `asset_master_version_id`；
- `issuer_master` stable key 为 `issuer_id`，每次安全字段变化产生内容寻址 `issuer_master_version_id`；
- tombstone 必须是一条带原因、availability 和 source lineage 的 successor row version，不能删除旧 row bytes；
- 旧 `universe_daily` 行同时保存 `alias_segment_id` 与其采用的 `alias_resolution_version_id`；旧 alias version
  永久可解析，因此新 delta 不破坏父 release 的 FK；
- resolved reader 按明确 view/cutoff 选择 terminal row version，遇到同级分叉、缺失 predecessor 或循环时拒绝。

`backtest_identity_eligible` 仍只是身份层必要条件：

```text
final_tradability_eligible
  = backtest_identity_eligible
  AND security_type_policy
  AND price/liquidity_availability
  AND entitlement/corporate_action_policy
```

S7.5 不提前实现最终 tradability，也不让 identity pipeline 根据流动性、价格或策略需求删除 membership。
`ticker_alias` v2 必须使用稳定 `alias_segment_id`；cutoff、release 与 evidence 版本只进入
`alias_resolution_version_id` 和 lineage，不能继续污染稳定 segment identity。

## 7. 简化后的控制面与审批矩阵

正常任务只保留三个持久控制对象：

1. `run_spec`：父 release、inputs、cutoff、semantics、资源上限；
2. `run_receipt`：实际 inputs/outputs、QA、资源使用、错误和幂等信息；
3. `release_manifest`：父链、partition delta、checkpoint 和最终可见性。

同一个 content-addressed 控制对象在单次进程内只验证一次；验证结果按 path、SHA、bytes、schema 和
semantics digest 缓存。写入/发布时执行完整验证，日常 reader 验证 manifest、receipt、schema 和父链；
全 source replay 放在通道 C 或显式 deep-audit 中。

| 情况 | 是否需要人工审批 |
| --- | --- |
| 干净的新 session，所有自动发布条件通过 | 否 |
| 已允许的普通 warning，未改变语义和 eligibility | 由合同预先定义，不逐日审批 |
| 新 canonical override、registry successor/withdrawal | 是 |
| 历史 replacement/supersession | 是 |
| schema、transform、calendar 或 identity policy 语义改变 | 是 |
| 未豁免 Critical/High、资源越界或父链冲突 | 阻断，不得批准成“成功” |
| 新 base cutover | 是 |

不得用自动发布绕过身份 adjudication。自动化只覆盖“输入新增但语义未变、QA 全部满足”的常规路径。

## 8. 实施步骤

实施不再为每个微型对象设置独立人工门，只保留三个正式 Gate。每个步骤仍需代码、测试、文档、
独立 Git commit，并通过现有三端同步规则。

### I0 — 冻结基线与实测

- 等待当前 S7 完整发布；
- 记录 base release set、四表 receipts、registry bundle、wall time、bytes、RSS 和 QA；
- 固化一组正常日、新 ticker、ticker gap、identity collision、历史修正 fixture；
- 不修改或重新解释 S7 base bytes。

验收：base reader 和现有深度 replay 均通过，基线可作为后续 equivalence oracle。

I0 实际冻结锚点（2026-08-04）：

- S7 release set：`5ce4ad18b44d86fe70fd25c50d1023fb1aa39f25f50fa2f93a0a1c4452eb811e`；
- source cutoff：`2026-07-29`，release available session：`2026-08-03`；
- 四表行数：`asset_master=14,865`、`ticker_alias=33,081`、`issuer_master=14,955`、
  `universe_daily=69,376,329`；
- 现有 publish deep verify 已于 `2026-08-03T03:41:13.780347+00:00` 成功完成，不在 I0 重跑；
- I0 只保存一个 content-addressed base-freeze manifest 和原始 wall-clock 日志；它们是对既有发布链的
  只读 pin，不是新的 Silver release，也不授予任何运行或发布能力。

### I1 — 增量合同与 resolved snapshot resolver

- 先设计稳定 `alias_segment_id` 与版本化 `alias_resolution_version_id`，并定义 v1→v2 映射；
- 定义 `historical_as_known` 与 `latest_reviewed_research` 两类显式 resolved view；
- 定义 base/delta/correction manifest schema；
- 定义 partition key、added/replaced/superseded 和 parent-chain invariants；
- 定义三张非 session 表的 stable key、row version、supersession、tombstone 和旧 FK 保留规则；
- 实现纯函数 resolver，检测环、重复 partition、缺父版本和非法 replacement；
- 定义 digest 职责分离与 runtime compatibility policy；
- 定义 `run_spec`、`run_receipt` 和 checkpoint schema。

**Gate A：合同设计审批。** 只审批 schema、语义、自动发布边界和迁移方案，不授权远程数据运行。

### I2 — S4 按 session 增量转换

- 去除固定 session/row count 代码绑定，改为 manifest-derived bounds；
- 对新增 Bronze active+inactive artifacts 只生成新 S4 session partition；
- 添加 source gap、duplicate page、active/inactive mismatch 和 idempotent retry 测试；
- 保留 full S4 runner，供通道 C 使用。

验收：补一个 session 不读取旧 session Parquet content；重复运行 receipt 完全一致。

### I3 — S7 checkpoint 与按日四表增量 runner

- 从 base release 构建首个 checkpoint；
- 实现 universe append、alias tail、asset/issuer delta；
- 实现 boundary lookback detector；
- 把五类 registry release 包装为一个原子 identity-policy bundle；
- 加入进程内 control DAG verification cache；
- 保留现有 full streaming runner 作为 oracle 与周期审计路径。

验收：正常新增 session 的 content read 范围不随十年历史长度线性增长。

### I4 — 有界 correction runner

- 实现 exact affected scope 和 replacement manifests；
- 实现 alias 从最早受影响日到稳定边界的有界重算；
- 加入 late source、registry successor、withdrawal、cross-market collision 和 issuer 不越权测试；
- 证明 foreign locale 的合法 Composite 不受 US-scope correction 影响。

验收：一个 ticker 的修正只读取并替换 affected-date partitions，未受影响日期 receipts 不变；被替换日中
无关行的 canonical projection 不变；无法证明稳定边界时 fail closed。

### I5 — 影子运行与等价性验证

- 选择连续正常日、半日市、上市/退市、ticker change 和已知 contamination 日期；
- 从同一 base 分别运行 incremental 路径与独立 full oracle；
- 对四表逐行、逐 schema、逐 partition receipt 和 QA 对账；
- 模拟中断、重复执行、checkpoint 损坏、父链缺失、并发锁和磁盘 hard floor；
- 测量单日 wall time、read bytes、write bytes、RSS 和 chain resolution latency。

影子集必须覆盖 alias extend/close/gap/reopen、cutoff 推进但历史 segment 不漂移、后来证据 availability、
registry collision、retroactive correction，以及 S8 新上市 ticker 尚未裁决的情形。

Gate B 使用两个明确比较层：

1. `canonical_research_projection`：排除 release ID、物理路径、创建时间和 resolution-version envelope，比较
   canonical facts、eligibility、有效期、observed lineage、availability 和 decision references；
2. `physical_reuse_projection`：clean append 要求旧 partition receipts 与稳定 segment IDs 完全不变，只有
   新增 partitions 和三张小表的 terminal row versions可以变化。

同一 view、逐 partition resolution lineage 与同一 cutoff 的 v2 Full 必须在第一层逐行等价。若新 base
有意采用更新 policy/correction 而不同，必须作为受审批的语义差异分类，不能标成 clean append 等价。

**Gate B：影子等价审批。** 所有非预期差异为 0，失败恢复和资源指标达标后才能申请切换。

### I6 — 切换与回退演练

- 保持当前 S7 base 为公开 canonical release；
- 先发布一个不面向 Gold 的 shadow delta；
- reader 解析 base+delta 并与 base/full oracle 对账；
- 完成一次撤回顶层 delta、回到父 release 的无损回退演练；
- 审批后才把增量 resolved snapshot 暴露给后续 S8 和研究 reader。

**Gate C：正式切换审批。** 切换只改变明确的顶层 release pointer，不修改任何历史 release。

### I7 — 周期 Full reconciliation 演练

- 从 Bronze 独立重建一个新 base candidate；
- 与 base+delta/correction resolved snapshot 全量对账；
- 验证 checkpoint base 创建不会改变任何逻辑 row；
- 固化月度或季度频率、触发条件和告警。

验收：全量审计仍可执行，但日常补数不依赖它完成。

## 9. 总体验收标准

S7.5 只有同时满足以下条件才算完成：

1. 当前 S7 release 仍可 byte-for-byte 读取和深度验证；
2. 新增一个干净 session 不重建 S4/S7 历史，不扫描全部 69M membership；
3. 单日任务读取与写入规模随当日数据和固定 boundary window 增长，而不随全部历史线性增长；
4. Clean append 不改变任何既有 `universe_daily` partition hash 或稳定 `alias_segment_id`；
5. 在当前服务器上，单日 clean update 目标不超过 30 分钟、峰值 RSS 不超过 2 GiB；最终阈值由 I5 实测冻结；
6. 相同 parent+inputs+semantics 重跑得到相同 receipts、checkpoint 和 release ID；同一 durable run retry
   还必须复用冻结 envelope metadata 并得到相同 manifest SHA；
7. 新 ticker、unknown identity 和 registry collision 保留 membership，但不能获得错误 alias/eligibility；
8. 历史修正只替换明确 partitions，旧版本可通过旧 release 完整复现；
9. Alias 不重叠、不跨缺席插值；identity quality 不产生退市、零收益或强平信号；
10. 增量 resolved snapshot 与同输入、同 cutoff 的 full oracle 非预期差异为 0；
11. 任意未发布/失败 delta 不影响父 release reader；回退只需选择父 release，不删除文件；
12. Schema、transform、policy 与 runtime provenance 职责分离，普通 verifier 修复不迫使重算数据；
13. S8 可以在新上市 membership 尚未完成 identity adjudication 时保留事件与 membership，同时保持研究层 fail closed。

## 10. 失败与回退边界

- 任一阶段失败时，继续使用当前已发布父 release；
- staging 和未发布 manifest 不得被 reader 发现；
- 不 force-push、不 reset 远程 checkout、不删除旧 build 或通过覆盖来修复失败；
- delta 父链出现环、断链、重复 partition 或 digest mismatch 时 reader 必须拒绝顶层 release；
- 自动发布误判为高风险缺陷时，立即停用自动发布并回退父 release，但保留失败证据；
- 若 incremental 与 full oracle 无法达到逐行等价，S8 保持 blocked，不能以性能理由降低身份安全门。

## 11. 对 S8 的具体依赖

S8 `ipos` 会持续引入新 ticker、上市日期和 provider 状态修订，因此它是第一个必须消费增量身份体系的
正式数据集。S8 开始前至少需要：

- S4 新 session partition 可以按日追加；
- 新 ticker membership 可以安全保留为 identity-ineligible；
- `asset_listing_event` 能绑定明确 source/availability，但不能自动创造 canonical asset；
- Identity adjudication 后可通过 correction release 有界回填受影响日期；
- 首个 bar、active snapshot 与 listing event 的对账不要求重跑十年 S7；
- S8 的 listing/IPO 状态不被当成最终 tradability，也不回填成事件发生日前已知。

S8 只能消费显式的 S7/S7.5 release ID。IPO/listing event 可以提供新的 review evidence，但不得从 S8
反向静默修改 S7 identity；需要修改时必须进入通道 B，发布新的 S7 correction release，再由后续 S8
release 显式绑定，避免形成不可审计的 S7↔S8 循环。

未通过 Gate C 前，S8 只允许做 schema/source-profile 讨论，不运行正式 preview、Full 或 Publish。

## 12. 主要风险与降级规则

| 风险 | 强制处理 |
| --- | --- |
| v1 alias ID 被 cutoff 污染 | Gate A 先完成 segment/version ID 拆分，禁止直接复用 v1 ID 作为稳定键 |
| Retroactive decision 漏算影响范围 | 不能证明 exact impact set 时降级 full reconciliation |
| Open alias 跨 cutoff 错误延长 | checkpoint 保存完整 decision key，并用 gap/reopen fixtures 对账 |
| Asset/issuer aggregate 漂移 | 增量聚合与 full oracle 定期逐 row 对账 |
| Delta 父链过长 | 达到冻结阈值后创建已对账 checkpoint base，旧链继续可读 |
| 两种时间视图混淆 | Reader、Gold 和 backtest manifest 必须显式绑定 view 与 cutoff |
| 控制面瘦身误放身份冲突 | Composite collision 和未批准 override 的 Critical gates保持不变 |

任何风险不能有界证明时，正确动作是停止该 delta、继续服务父 release，并转入人工 review 或通道 C；
不能为了满足日更 SLA 自动放宽身份安全门。
