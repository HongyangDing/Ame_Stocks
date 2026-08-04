# S7.5 Gate A：增量合同与解析语义评审

## 1. 本 Gate 的授权边界

Gate A 只评审本地、纯函数的合同与迁移语义。当前实现不得读取远程 Parquet、构建真实 checkpoint、
运行 S4/S7 增量、改动 registry、发布 release 或开始 S8。

S7 已发布 release set
`5ce4ad18b44d86fe70fd25c50d1023fb1aa39f25f50fa2f93a0a1c4452eb811e` 保持不可变，I0 freeze
`da74c44f426310bcc6519c11751bc87352884c1be88caedb73d817bcf3a62f79` 是本 Gate 的等价性 oracle。

## 2. 精简后的对象模型

日常运行只持久化三个控制对象：

1. `run_spec`：父 release、输入、cutoff、语义和资源上限；
2. `run_receipt`：实际输入输出、确定性 QA、checkpoint pin、资源观察和失败信息；
3. `release_manifest`：父链、partition/row-version 变化和最终可见性。

Checkpoint 是 `run_receipt` 的可重建输出，不是第四套审批链，也不是新事实源。正常 clean delta 不再生成
plan/request/approval/intent 的递归组合；人工决议本身仍以 exact approval/evidence pin 进入 correction 或 policy
bundle。

三个对象按单向关系生成：`run_spec → checkpoint/run_receipt → release manifest body → external manifest pin`。
Manifest body 不包含自己的 path/bytes/SHA；序列化 body 后才生成外部 pin，因此没有 self-hash。Checkpoint 绑定
的是排除 top release/父链 lineage 的 `resolved_content_digest`，也不会形成 `release → receipt → checkpoint →
release` 哈希环。

所有内容 ID 使用 canonical JSON 的 SHA-256。Wall-clock、RSS、日志位置、writer Git commit 和 resource caps
不进入 release logical ID；resource caps 仍进入 run spec。Manifest envelope 用 exact object ID/path/bytes/SHA pin
住 run spec 与 receipt，并由一个纯 `validate_release_projection` 一次检查三对象一致性。同一 parent、sources、
semantics 和 logical outputs 即使资源上限不同，release ID 仍相同；run spec/receipt pin 会保留执行差异。

QA 不再是一个 opaque digest。`run_spec` 内含逐 check 的 `QaPolicy`（check ID、severity、semantics digest），
`run_receipt` 内含绑定 exact run spec、source、change set 和 availability 的 `QaReceipt`。每个 result 必须复现 policy
中的 checker semantics，并只使用一个 path/bytes/SHA 的 exact details artifact pin，不再并列保存一个无法对账的自由
details digest。Critical/High 的 publish limit 固定为 0；普通 Warning 的 nonnegative limit 进入 policy ID，超过才阻断，
因而不产生逐日审批；Info 只作指标，也不存在可由调用方填写的 waiver flag。Gate A 只冻结 policy/result 的结构，
尚未冻结可执行 checker；I3 必须提供 module-owned 的 closed dispatcher，逐项白名单绑定 exact check ID、semantics
digest、severity、Warning limit 和执行器，在此之前不能把调用方自带的 policy 当作生产发布凭证。最低必需检查为
`partition_session_calendar_contiguous`（High）与 `row_semantic_proof_complete`（Critical），并且 observation count
必须覆盖 exact partition/row change set。

Correction 不再接受任意 artifact pin，而是使用 canonical body + exact artifact pin 的
`PinnedCorrectionAuthorization`。它逐项绑定 validated parent、exact change set、派生 scope、source、schema、
transform、calendar、identity policy before/after、证据、审批人和 approval availability。每份 evidence pin 包含
path/bytes/SHA 与本项目可使用它的 availability，且 evidence availability 不得晚于 approval；RunSpec 的 exact input
pins 与 source binding 则独立约束数据输入，不能把审批证据伪装成行情 source。普通 base/delta 不能携带该对象。
该对象在 Gate A 仍只是结构化 candidate：任意调用方都能构造 canonical body，尚不能证明真实审批发生。因而
correction 即使 control projection 合法，也不能取得 reader capability；I2/I3 必须从 append-only approval-event ledger
核验 exact event 后才允许签发生产能力。新 base cutover 同样默认拒绝，不能靠任意新 RunSpec 绕过审批。

`release_available_session` 同时进入 run spec 与 manifest 正文，并因此进入 manifest SHA/release ID；external pin
只能复现正文中的日期，不能为同一 manifest 自由选择一个更早日期。`receipt_available_session` 与
`qa_available_session` 也被持久化并满足 QA ≤ data/knowledge cutoff ≤ receipt ≤ release。这能阻止同一 manifest body 被重新 pin 到更早日期，
但第一次写入的 availability 仍是 writer 声明；防止“新建一条整体回填的旧链”需要 I2/I3 的 append-only publication
event ledger，Gate A 不夸大为已经解决该外部信任问题。

`wall_clock_cap_seconds=null` 表示私人服务器任务不因总时长被杀死；RSS hard cap、磁盘 hard floor 和 QA 仍是
发布硬门。成功 receipt 必须保存资源观察并通过这些门。失败 receipt 只允许因果前缀：空、inputs、
inputs+outputs、inputs+outputs+QA 或完整 receipt；不允许先有 output 却没有 actual inputs，也不允许孤立 QA/checkpoint。

控制投影成功后只在内存中产生 `ControlValidatedCandidate`；它还不能供 reader 使用。只有在 manifest 的 publication
cutoff 上重新解析完整链、并复现 `resolved_content_digest` 与 `snapshot_digest` 后，才产生
`ContentAttestedRelease`。这里的 “content” 严格指 content-addressed receipt/index graph 与 snapshot digest，不表示本
模块已经打开并核验了 Parquet、checkpoint、proof、QA details 或 evidence bytes。Resolver 拒绝裸 manifest 和
control-only candidate，只接受后一种 capability，并会在 publication cutoff 再复算一次 receipt graph。这两类对象
都不是第四个 durable object；Python private seal 只是进程内误用防护，不是密码学或 IO 信任边界。
Gate A 测试中可为纯 metadata 的 base/delta 签发该内存对象，只为验证 resolver 语义；它不是生产 publish authority，
更不代表新 base cutover 已获批。生产 loader 仍必须执行 external trust gate。

父链的 control replay 使用 base→top 迭代遍历，不递归调用 parent validator；resolver 维护 running partition
frontier，并用线性状态遍历检查 row predecessor graph。因此长期日更不会在约 1000 层触发 Python recursion limit，
也不会因每层重复扫描全部历史 key 形成平方级 resolver 热点。Checkpoint receipt 固定 rule version
`s7_5_checkpoint_receipt_v1`，其 `last_session` 必须恰好等于 parent 与本次 partition operation 合并后的最大
session，不能只满足“不超过 cutoff”。Gate A 尚未定义 checkpoint bytes→state 的生产 validator；I3 必须冻结内容
schema 并验证 exact bytes 后才能消费 checkpoint，损坏时只允许放弃本次增量并回到已发布 parent。

Gate A 实现的是纯内存 canonical reserialization 与 exact pin/projection 验证。真正按 path 读取 bytes、先核验
size/SHA、再严格解析，执行冻结 checker，并核验所有被消费 pin（input、manifest、run spec/receipt、partition
Parquet、row index/proof、checkpoint、QA details、evidence、authorization）的 bytes，最后结合 append-only
publication/approval event 签发 capability 的 production IO loader 是 I2/I3 的实现要求，不在本 Gate 假称已完成。

## 3. Alias v2

### 3.1 稳定 segment

`alias_segment_id` 的哈希使用 namespace `ame_stocks.identity.alias_segment`、rule version
`ame_stocks_alias_segment_id_v1`，并只绑定以下不可变 subject：

- provider、market、locale、规范化大写 ticker；
- provider-observed Composite FIGI、Share Class FIGI 和规范化 observed CIK；
- segment 首个 session；
- segment 起点的 exact source record ID。

Canonical asset/FIGI、decision、policy、evidence/cutoff、release、runtime、区间终点均不得进入 segment ID。
因此 cutoff 推进、后来证据和 canonical correction 不会让既有稳定 segment 漂移；membership gap 后重新出现则
必须创建新 segment。

### 3.2 版本化 resolution

`alias_resolution_version_id` 使用独立 namespace/rule version，并绑定 segment、canonical identity、闭集的
resolution method/status/disposition、decision lineage、policy bundle、identity/evidence cutoff 与 availability、
区间终点、source-range digest、predecessor 和 tombstone。它不得包含当前 release ID。

存储的 asset/share-class/issuer ID 必须分别由 canonical Composite FIGI、Share Class FIGI 和 normalized CIK
按冻结 S7 规则重新计算，不能只做 64 位十六进制格式检查。创建、反序列化和 successor 都必须同时提供 exact
segment subject。Direct Composite resolution 的 Composite/issuer 必须与 observed 一致且没有 Composite decision
lineage；override、transition 和 withdrawal 必须有各自 decision lineage。Provider stale Composite 使用独立
disposition，不能借一个宽泛的 source-correction 类型绕过五类 registry。

Extension、closure、canonical correction 或 later evidence 都创建 successor version；旧 version 永久保留，旧
`universe_daily` FK 仍可解析。四条 knowledge 时间轴不得倒退；这些 predecessor-aware 单调性与 tombstone terminal
规则由 successor validator 检查，单行 constructor/from_dict 只验证本行 shape。Tombstone 只使用
`resolution_available_session`，必须有 predecessor、原因和决议，且不能再产生 successor；不能通过删除 bytes
表达撤回。

Composite/asset resolution 与 Share Class resolution 是两个可组合 component。前者保留
`resolution_method + decision_lineage_ids`，后者独立使用 `share_class_resolution_method +
share_class_decision_lineage_ids`。因此同一 row 可以同时表示“跨市场 Composite 污染已修正”与“Share Class 临时重复
已裁决”；ShareClass component 只能在 canonical Composite 已唯一确定后应用，不能修改 Composite 或 issuer。没有
Share Class FIGI 时只有 `not_applicable` 一种表示，避免同一事实产生两个 version ID。Clean delta 不得携带任一
component 的 approved lineage。

## 4. Release 与 row-version 规则

### 4.1 父链

- `base` 无 parent、至少包含一个 `universe_daily` session partition，且 row version 只能是 `new_root`；
- `delta` 和 `correction` 必须 pin 唯一 parent 的 release ID、manifest path、bytes、SHA 和 availability；
- reader 只能从调用方给出的 exact top pin 开始，禁止目录扫描或猜测 `latest`；
- I2/I3 production loader 必须先校验 exact path/bytes/SHA，再解析不含 self pin 的 manifest body；
- loader 还必须读取并精确验证 manifest 内 pin 的 run spec、run receipt、QA details 与 typed authorization，先签发
  `ControlValidatedCandidate`，并核验全部被消费的 output artifact bytes 与可信 publication/approval event，完成
  publication-cutoff receipt-graph attestation 后再签发 `ContentAttestedRelease`；resolver 不接受裸 manifest 或
  control-only candidate；
- 环、断链、同 ID 不同 pin、body/ID 不一致、重复逻辑 key、时间倒流或兼容性 digest 漂移均 fail closed。
- 整条 capability 父链以迭代方式从 base 到 top 重放；同一 release ID 出现不同 exact pin 也直接拒绝。

### 4.2 Partition

`universe_daily` 使用 session partition receipt。

- clean `delta` 只能添加父 snapshot 不存在的新 session；
- clean `delta` 至少添加一个 session partition；row-only 或历史补写必须走 correction；
- `correction` 可以在 exact authorization 下补入缺失的历史 partition；replacement 必须同时 pin 新 receipt 与父
  snapshot 当前 exact receipt；
- replacement 只能遮蔽同 table/key，旧文件永久保留；
- added/replaced/superseded 必须一一对应，禁止用文件缺失表达删除。

### 4.3 非 session 表

`ticker_alias`、`asset_master`、`issuer_master` 使用 stable key + version key + predecessor 的 append-only row-version
index。操作类型只有 `new_root`、`mechanical_successor`、`reviewed_correction`、`tombstone`；clean delta 只能使用
前两类，而且 writer 必须从 predecessor/new row 的 exact bytes 重新验证 mechanical 分类，不能相信调用方标签；
后两类必须走 correction。Resolver 先验证整个 exact 链，再单独投影 consumer view；遇到 fork、loop、
缺失或指向未来 release 的 predecessor/FK、stable-key 变化或无理由 tombstone 时拒绝。物理 row locator 由 exact
index receipt 提供，reader 不为找 terminal row 扫整张 Parquet。

每个 row receipt 现在必须绑定 `RowSemanticProofReceipt`：table/key/version/predecessor、operation、old/new payload
digest、validator semantics digest 与 exact proof artifact。Gate A 只冻结这个 proof contract，不开放任意 callback；
当前 module-owned dispatcher 明确为 disabled，因此任何 row-bearing candidate 都 fail closed。I3 只有在为
`ticker_alias`、`asset_master`、`issuer_master` 分别加入固定 dispatcher 与恶意标签负例后，才能打开对应能力。
Ticker alias 的 interval extension 还必须加载 calendar-aware exact source-coverage receipt；当前 mechanical helper 只验
结构、冻结字段和时间不倒退，不能证明跨度内没有 membership gap。这避免用一个 no-op callback 或 opaque source
digest 将 canonical 修改/跨 gap 插值伪装成 `mechanical_successor` 或 `new_root`。

任何空 delta/correction 均拒绝，避免无意义增长父链。Schema、transform 或 calendar 改变必须新建 base；
identity-policy bundle 只有带 exact authorization 的 correction 可以改变。

## 5. 两类 view

- `historical_as_known`：先对完整链做结构审计，再只消费在显式 knowledge cutoff 已 available 的
  release/version。若最早 base 本身晚于 cutoff，必须拒绝。Consumer catalog 不暴露 future row versions；完整链
  只在显式 `audit_row_version_catalog` 中保留。
- `latest_reviewed_research`：消费 exact top 下所有已批准 retrospective correction，同时保留每个事实的真实
  availability，不回填成历史时点已知。

Gold 和回测 manifest 必须显式 pin view 与 cutoff，resolver 不提供隐式默认值。

## 6. v1 → v2 迁移决议候选

当前 S7 v1 同时存在两个限制：`ticker_alias_id` 混入 canonical/cutoff；历史 partitions 又由一个全局
`2026-07-29` identity cutoff 生成。因此 Gate A 建议采用：

1. 当前 S7 v1 永久保留为 byte-level 与 canonical-research-projection oracle；
2. I3–I5 从相同 S4/registry pins 一次性生成独立 v2 base，并按 partition 保存真实 resolution lineage；
3. v1→v2 映射必须回到 exact S4 origin row 获取 provider/market/locale/observed CIK，禁止用 latest metadata 猜测；
4. v2 base 与 v1 在 `latest_reviewed_research` canonical projection 上逐行等价后，才可在 Gate C 成为 delta parent；
5. `historical_as_known` 必须由按 availability 重算的 v2 数据提供；在此之前明确 unavailable/fail closed；
6. 不采用永久 mixed-schema legacy adapter，也不静默把 v1 ID 当作稳定 segment ID。

这会产生一次有审计价值的 v2 base 构建，但之后日更不再重放十年。该选择优先保证两类 view 都真实可复现，
同时避免把兼容层永久带入 reader。

## 7. 自动发布边界候选

可自动发布：

- 只新增连续 session；schema、transform、calendar 与 identity-policy bundle 和 parent 完全兼容；
- 未修改任何旧 partition receipt 或稳定 segment ID；row operation 仅为 new root 或可机械证明的 tail successor；
- 所有 eligibility/alias/lineage/forced-liquidation Critical invariant 为 0；
- 新 ticker 或未知 identity 的 membership 被保留，但 `backtest_identity_eligible=false` 且无 alias；该安全隔离
  可以进入 review queue，不阻塞同一 session 的其他干净行；
- raw registry collision 可以作为 review fact 保留，但 collision eligible/resolved/alias rows 必须为 0；
- 资源和磁盘 hard floor 通过，输出仍在冻结的单日上限内。

必须人工审批：

- 任何 canonical override、registry successor/withdrawal 或 policy bundle 更新；
- 任何历史 partition addition/replacement、reviewed row-version correction 或 tombstone；
- schema/transform/calendar/QA policy 语义变化或新 base cutover。

直接阻断：

- 任一 Critical/High failure、超过合同固定上限的 Warning、错误 alias/eligibility、父链冲突或 source
  mutation/duplication；
- 无法证明 exact impact set 或 alias 稳定边界；
- 资源 hard cap、磁盘 hard floor 或幂等 digest 不满足。

Identity quality 永远不等于 inactive/delisted，也不能独自产生强制平仓。

上述“可自动发布”是 I3 完成固定 row dispatcher 后的目标边界。Gate A 的实际 capability flags 全为 false；当前
既不授权数据读取/执行/发布，也不允许任何 row-bearing release 获得 runtime capability。

## 8. Candidate contracts

候选合同：

- 文件：`docs/silver/contracts/control/s7_5_incremental_contract_bundle-v1.candidate.json`
- Contract ID：`b54781ddfb37f7720315dffea340e017b23fad77853f9725b1ae969a46aa66bb`
- Candidate SHA-256：`87345e453ec7ea0656ec441b3f3f3559fe11fd02f7f28c080ba18eec9a8a4390`

实现模块：

- `backend/ame_stocks_api/silver/incremental_identity.py`
- `backend/ame_stocks_api/silver/incremental_gate.py`
- `backend/ame_stocks_api/silver/incremental_contract.py`
- `backend/ame_stocks_api/silver/incremental_resolver.py`

固定测试：

- `tests/test_silver_s7_5_incremental_identity.py`
- `tests/test_silver_s7_5_incremental_gate.py`
- `tests/test_silver_s7_5_incremental_contract.py`
- `tests/test_silver_s7_5_incremental_resolver.py`

Gate A 固定测试共 149 项：identity 53、gate 36、contract 31、resolver 29；另有 I0 freeze 4 项，合计 153 项。
I0 测试确认既有 S7 oracle 文档和证据未被本次设计改写。Candidate 测试会重算 Contract ID、逐项对齐 alias
subject/enums、release logical payload 和完整 capability key set，并确认所有执行/读取/发布能力仍为 false。

Gate A 批准前，这份 candidate 不会成为生产 schema，也不授权远程 Parquet 读取、checkpoint 构建、真实增量
执行、registry mutation、release publish 或 S8。
