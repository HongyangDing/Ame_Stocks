# S7.5 I3：S7 native-v2 checkpoint 与单 session 四表增量设计

## 1. 状态与本轮边界

I3 的目标是把 S7 日常补数从“反复扫描十年历史”改为“一个 native-v2 parent checkpoint 加一个
exact next session”。现有 S7 v1 release set
`5ce4ad18b44d86fe70fd25c50d1023fb1aa39f25f50fa2f93a0a1c4452eb811e` 继续作为不可变的
canonical equivalence oracle 身份，但不能直接冒充 native-v2 parent，也不能从 v1 的计数字段猜测增量状态。
本地 fixture 只对调用方提供的 v1 projection 做内容绑定；它不声称已重新读取或认证该 production release 的 bytes。

本轮授权只覆盖：本地 schema、checkpoint、固定 dispatcher、fixture bootstrap、fixture 单日 runner、测试和
三端代码同步。它不授权读取远程 Parquet、生成真实 native-v2 base/checkpoint、修改 registry、执行 correction、
Publish、切换生产 base 或进入 S8。真实数据的下一步必须是一个单独批准的 bounded native-v2 base staging，
完成 v1 oracle 对账后仍停在 no-publish。

I3 不修改以下已冻结对象：

- S7.5 architecture plan、I0 base freeze、Gate A candidate/review/approval；
- I2 candidate/design 与 `2026-07-10` S4 staging receipt；
- S7 四张 v1 contract/resource 和现有 Full/Publish runner。

## 2. native-v2 物理合同

四张表均使用 checksummed v2 overlay；overlay 明确绑定相应 v1 Contract ID 与 resource SHA，保留未声明删除的
全部 v1 列，禁止复制后静默漂移。

| 表 | v2 变化 |
|---|---|
| `asset_master` | 保留稳定 `asset_id`，新增 append-only `asset_master_version_id`、predecessor、availability 和完整 aggregate-state digest。 |
| `ticker_alias` | 删除 legacy `ticker_alias_id`，分离稳定 provider-observed `alias_segment_id` 与可版本化 `alias_resolution_version_id`。 |
| `issuer_master` | 保留稳定 `issuer_id`，新增 append-only issuer row version；FIGI/Share Class 规则不得改变 CIK 或 issuer。 |
| `universe_daily` | 每日 membership 显式引用 alias segment/resolution、asset/issuer exact row versions、policy bundle 和 row availability。 |

四表 combined schema bundle digest 在代码和 candidate 中固定。v1 只参与 canonical projection 对账，v2 的
row-version、segment 和 checkpoint 信息没有合法的 v1 替代值。

native-v2 release 另使用 closed typed manifest。manifest 把 fixture family
`s7_5_native_v2_fixture` 与未来 production family `s7_5_native_v2` 分开，并明确绑定 terminal session、release
availability、schema bundle、transform semantics、identity-policy bundle、migration/v1-oracle lineage、parent
release/source checkpoint、完整 `resolved_state_digest` 和固定四表 typed output projections。每个 output 都绑定
table role、Contract ID、schema digest、terminal session、row count 与 artifact pin。release ID 只由可重算 logical projection 产生，manifest
artifact pin 再认证完整 canonical bytes，因此不存在 manifest self-hash cycle，也不能靠填写 family/schema 标签伪装
native-v2 parent。checkpoint exact loader 必须同时读取并对账该 parent manifest；它只认证 manifest/checkpoint bytes
与 `resolved_state_digest` 相等，并不声称读取或认证四个 output 文件 bytes、row count 或 terminal-state reconciliation。
当前 exact loader 只接受 fixture family；production family 名称虽保留，
但在独立 migration/release authority 实现前明确 fail closed。

I3 candidate contract：

- 文件：`docs/silver/contracts/control/s7_5_identity_session_incremental_bundle-v1.candidate.json`；
- Contract ID：`4ac6fdb83ef5d0c080c997841406a5bf4614818269ec2a7cc81d833cf4ce4605`；
- Candidate SHA-256：`22bb9d2eb5b01f618e824ee437127937de4434e033dcb8519bd1396ae0228898`；
- v2 combined schema bundle digest：
  `22ffa9d2b96b7c9a26f1766d58fcb6d2cb4e8c3f89599a26d3440e95592cf579`。

## 3. Identity policy bundle

每次解析原子绑定且只绑定五类职责互斥的 registry release，固定顺序为：

1. `identity_adjudication`：deterministic bounce middle episode；
2. `identity_cross_market_adjudication`：跨市场 Composite 污染；
3. `provider_composite_override`：真实 transition 后、同市场 exact-scope stale Composite；
4. `share_class_adjudication`：只修正 Share Class，不产生或改变 `asset_id`；
5. `asset_transition`：只表达 predecessor/successor，不执行 override。

每个 member 都绑定 release ID、exact artifact pin、decision cutoff 和 member release availability。fixture policy
snapshot 只能由五份 canonical registry release bytes 的 strict loader 生成；loader 逐份校验 byte count、SHA、closed
schema、release/decision ID、registry kind、cutoff、availability、exact source row、market/Share Class scope 与 closed
disposition，再按
`provider, market, locale, ticker, source_record` 建 immutable index。进入热路径前一次性重建全部 decisions、index
与 snapshot ID；之后逐行只查对应 bucket，既阻断内部状态注入，也不退回逐行扫描全 registry。调用方不能直接构造或注入 decision。真实
staging 必须复用现有 S7 `load_registry_release_set` 完整 trust-chain loader，fixture 格式不是 production
authority。decision cutoff 与 member-knowledge availability 分别取五个 member 的最大值；operational policy cutoff 再取 decision
cutoff 与 bundle wrapper availability 的最大值，确保 resolution 永不早于 wrapper 实际可见日。之后创建的
bundle wrapper artifact 另在 run input pin 和 receipt 中记录实际可见时间；绝不把 2026 年抓取/封装时间回填成
历史公司事件日。

不同 Composite registry 对同一 source row 同时命中时不按优先级、最近值或多数值自动选择。原始 collision
数量进入 review；任何 collision-derived eligible、resolved 或 alias row 都必须为 0。Share Class 修正只能在
canonical Composite 已唯一确定后附加，不能修改 issuer/CIK。

五类 registry 都使用 closed disposition matrix。Composite/Share Class 的 unresolved decision 保留 observed lineage，
但不允许回退成 direct-observed eligible canonical identity；`asset_transition` 是唯一例外，它无论 confirmed 或
unresolved 都只表达关系 lineage，不改变 identity eligibility。只有 confirmed transition 才给 predecessor/successor
写对称 edge；provider Composite override（含 unresolved override）也只能绑定一条 confirmed genuine transition。
foreign→US→foreign 的 middle observation 若被标成 genuine transition，直接触发 Critical。

每个实际命中的 decision 另输出 typed lineage：`registry_kind, decision_id,
decision_available_session`。identity resolution method/disposition 由 registry kind 和密封 decision 派生，不能信任
supplied v1 row 的同名字符串。真实 production base 对旧 v1 的 method/disposition 需要一份显式、固定的
compatibility projection（尤其旧 provider override 的 legacy 表示）；canonical/share/eligibility/membership 仍逐字
对账，但不得因 legacy 字符串不同而把旧表示反向提升为新 authority。

## 4. checkpoint 内容与权威边界

checkpoint 是可重建 cache，不是 publication authority。它的 canonical bytes 和 SHA 必须逐字认证，并包含：

- typed exact native-v2 parent release manifest projection 与 v1 oracle/migration lineage；
- `last_session`、source/availability cutoff、calendar/schema/transform digests；
- 当期 exact 三张 S4 terminal partition pins；
- atomic identity-policy bundle；
- open alias segment 与 terminal resolution；
- asset/issuer 的完整 distinct-value sets、完整 fixed-name counters 与 terminal row version；
- unresolved review subjects；
- resolved universe partition receipt map；
- 三张非 session 表的 stable-key → terminal row-version map。

checkpoint 不保存“只有数量没有集合”的近似状态；asset aggregate 明确保留 canonical Share Class 完整集合以及
五类 adjudication/override/transition decision ID 完整集合。byte count、SHA、closed JSON fields、canonical
serialization、所有派生 ID、FK、availability、固定顺序、terminal completeness 和 parent-manifest projection 任一
不一致即 fail closed。进程内 exact-pin
cache 只减少同一 control DAG 的重复读取；同一路径出现不同 pin 时立即失败。

`resolved_state_digest` 覆盖 parent 之外的全部 rebuildable checkpoint state。parent manifest 必须逐字携带同一
digest，因此攻击者不能只替换 resolved partition、aggregate 或 terminal row 后重新 pin checkpoint，却继续引用旧
parent manifest。parent release availability 还必须不早于 policy wrapper、S4 pins、resolved partitions、terminal rows、
open aliases、asset/issuer aggregates 和 unresolved subjects 中的任何 availability。

## 5. 固定 boundary dispatcher

正常 append 的逻辑窗口固定为 target session 加前两个 exact XNYS sessions；不足三个 session 不能缩短窗口或
降级运行。coverage receipt 与 fixture input binding 必须证明：

- target 是 checkpoint `last_session` 的 exact calendar successor；
- 三个 session 按 calendar 连续且分别对应不同的 S4 partition receipt/artifact pins；fixture 将这些 pins、实际
  supplied row window 和 selected-source set 交叉哈希，防止误混，但 pins 仅是 declarative lineage，不能证明 rows
  真来自相应 Parquet；
- runner 只读取这三个 session 的身份边界输入，不目录扫描、不读取其他历史 partition；
- 每个 target row 都运行 market-consistency 检查，因而长期不发生 bounce 的 foreign Composite 也会被发现。

dispatcher 是 module-owned closed set，调用方不能注入 QA、proof builder、severity 或 callback。它覆盖
US→foreign→US、foreign→US→foreign inverse、长期 US-locale/US-primary-exchange 下的 foreign Composite，
以及 multi-registry collision。原始 observed FIGI 和 S4 lineage 永久保留。

row semantic proof 只允许两种 clean append 操作：

- `new_root`：新 observed segment；
- `mechanical_successor`：相同 immutable segment subject、exact next calendar session 的 tail extension。

correction、replacement、tombstone、policy mutation、registry mutation、historical addition 和 base cutover 在 I3
dispatcher 中全部拒绝。私有 staging attestation 不是 Publish capability，也不能解除 Gate A public validator 的
默认拒绝。

## 6. 单 session runner

热路径只接受显式传入的 fixture target rows、checkpoint、三日 S4 declarative partition pins、calendar coverage、
structurally reproduced sealed policy snapshot 与一个 non-authoritative fixture input binding。该 binding 把 requested
sessions、target、S4 lineage pins、canonical row window、selected-source set、会影响输出的 reference name/SIC projection
及其 availability 交叉绑定。fixture runner 只接受 fixture-family parent；production-family parent 必须由未来独立
staging executor 的 exact loader 使用。禁止 filesystem discovery 或调用旧 S7 Full runner。

处理顺序：

1. 认证 native-v2 fixture checkpoint、typed parent manifest、policy bundle，并内容绑定固定 fixture boundary window；
2. 从五份 exact fixture registry bytes 得到 sealed policy snapshot，并逐 ticker 调用 closed dispatcher；
3. 将 dispatcher 的 canonical Composite/Share Class、eligibility 与 membership 结果同 supplied v1 oracle row 对账，
   并逐 decision 对账 ID、registry kind、decision availability、evidence availability、closed disposition/method，
   同时执行 market consistency、registry collision 和 row semantic proof；
4. 相同 observed subject 且 exact next session 时追加 alias resolution successor；gap/reopen/new subject 创建新 segment；
5. Composite/Share Class identity unresolved 或 collision 行保留 membership，但 alias、canonical FKs 为空且
   `backtest_identity_eligible=false`；unresolved `asset_transition` 只保留 lineage，不改变原 identity 输出；
6. 用 checkpoint 中完整集合/counter 更新受影响 asset/issuer，写 append-only row versions；
7. universe row 引用同一次 snapshot 中精确 alias/asset/issuer versions；
8. 先运行 closed QA；任何 Critical 非零时不得构造 next checkpoint、native-v2 manifest 或 diagnostic receipt；
9. 通过后生成 partition/row receipts、next checkpoint candidate、typed native-v2 manifest 与 content-addressed
   diagnostic receipt；
10. diagnostic receipt 最后生成并绑定 input、QA catalog/result、release 与 checkpoint；它只存在于内存，不是 final
    production receipt，也不授予远程写入或 Publish。

`active_on_date` 始终表示 source membership。identity 不确定、temporary security 或 registry collision 不能被解释为
inactive/delisted，也不能触发强制平仓。最终交易资格留给后续明确合同：

```text
final_tradability_eligible
  = backtest_identity_eligible
  AND security_type_policy
  AND price/liquidity availability
  AND entitlement/corporate_action policy
```

## 7. native-v2 base bootstrap

fixture bootstrap 会从显式给定的最早 session 建立 fixture-family root，并按 exact calendar succession 完整消费
一个小型 fixture 历史，建立完整 aggregate sets、open tails、terminal versions 与 checkpoint；每个 session 都将
canonical research projection 与独立 v1 oracle 逐行对账。它用于证明算法和合同，不构造或伪造 production
parent。单 session fixture root 只接受 direct-observed、无 selected decision/transition 的干净行；任何需要
adjudication 的行都必须进入具有固定两日 lookback 的 closed dispatcher，不能借 bootstrap 绕过 availability 或
market-consistency 校验。

真实 base staging 必须单独执行一次全历史 S4 读取，因为 v1 master 只有部分 distinct counts，不能安全恢复
完整增量集合。真实 staging 的最低要求是：

- exact pin I0 v1 oracle、同一 S4 release、五 registry releases、calendar 与 v2 contracts；
- 使用 authenticated I2 run receipts 并从 exact pinned S4/resolved artifacts 读取和验证内容，显式记录
  `source_s4_run_receipt_id`，不能把旧 Full release-set ID 冒充新 session lineage；
- 一次性流式构建 native-v2 四表和完整 checkpoint；
- 验证四表 output bytes、row counts 与 checkpoint terminal state 一致；
- `canonical_research_projection` 非预期差异为 0；
- 原 v1 与 S4 bytes 不修改，输出仅写新 immutable staging namespace；
- no-publish、no-cutover，完成后停在 `awaiting_review`。

## 8. QA 与失败语义

QA 使用一个由 dispatcher、runner、candidate 和 diagnostic receipt 共同绑定的 exact catalog。dispatcher-owned 项在 row
proof 阶段执行；alias/FK/availability/membership 等 materialization-owned 项在写 checkpoint 前执行。前一阶段不得
把后一阶段尚未执行的检查写成 0 pass，而要明确标记 `deferred_to_materialization`。

必须为 0 的 Critical 指标包括：

- `multi_registry_composite_override_collision_eligible_rows`；
- `multi_registry_composite_override_collision_resolved_rows`；
- `multi_registry_composite_override_collision_alias_rows`；
- `unapproved_cross_market_composite_eligible_rows`；
- `suspected_provider_contamination_eligible_rows`；
- `inverse_bounce_misclassified_as_genuine_transition_rows`；
- eligible membership 缺 exact alias/asset version，或 ineligible membership 携带 alias；
- row-version/semantic-proof/FK/availability/coverage digest mismatch；
- source omission、duplication、gap interpolation 或非幂等输出。

原始 `multi_registry_composite_override_collision_rows`、`suspected_provider_figi_bounce_rows` 和
`us_locale_non_us_composite_figi_rows` 可大于 0，但必须带 reason counts 与 bounded examples，且对应行保持
ineligible/unresolved。reason count 总和必须等于 observed count，typed bounded examples 必须逐字复现 example source
IDs。v2 overlay 中尚未进入共享 fixture catalog 的 table-level 检查只在 constructor 可覆盖范围内执行，其余明确延后到
production content validator，不得宣称已通过。任何 blocking QA 时不生成 diagnostic receipt；损坏 checkpoint 时 parent release 仍可读，runner
不得自行重建、跳过或选择“最近”的状态。

## 9. 本地验收与下一检查点

本地验收覆盖：

- v2 overlay 的 base pin、Contract ID、schema digest 和 combined bundle digest；
- policy exactly-five、职责顺序、cutoff/availability、closed disposition、cross-market country/Share Class exact scope、collision fail-closed；
- checkpoint round-trip、byte/SHA/field/ID/FK/terminal map corruption；
- clean append、alias extend、gap/reopen、新 ticker、unresolved membership；
- A→B→A、inverse bounce、长期 foreign Composite、missing lookback；
- fixture-family native-v2 exact four-role typed manifest 与 v1 canonical projection 等价，production family 不能由 fixture helper
  生成；
- fixture input digest 同时绑定 row window、S4 lineage、reference metadata 与 availability，但明确不提升为 Parquet
  authentication；
- 重跑 bytes/IDs 完全一致；instrumentation 证明读取范围为 target + 固定两个 boundary sessions；
- public Gate A Publish/correction/cutover 继续 default deny。

通过后提交并同步本地、GitHub、远程代码。下一次需要用户批准的操作是：准备并执行真实 native-v2 base 的
bounded staging/no-publish 包；在那之前不读取远程 S4 Parquet。
