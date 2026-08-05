# S7.5 I2：S4 单 session 增量转换设计

## 1. 状态与边界

Gate A 已批准。批准对象保持原字节不变：

- Contract ID：`b54781ddfb37f7720315dffea340e017b23fad77853f9725b1ae969a46aa66bb`
- Candidate SHA-256：`87345e453ec7ea0656ec441b3f3f3559fe11fd02f7f28c080ba18eec9a8a4390`

Gate A 后，用户又明确授权：在没有报错或明显越界时，继续完成一次真实服务器的 exact-next-session、
staging-only、no-publish I2 验证。因此 I2 当前允许读取精确绑定的远程控制面、下载唯一目标 session 的
active/inactive Bronze pair，并生成该日三张 staging partition、QA、run spec 与 final receipt。它仍不创建
production checkpoint、不发布 release，也不修改 S7/S8 或 identity registry。现有 S4 Preview/Full 及其
已批准固定授权保持原样，继续作为 Channel C 全量对账路径。

Gate A approval 只新增一份开发审计记录，不恢复旧式
`plan → request → approval → intent` 链。远程预检已经将首次范围固定为 S4 base `2026-07-09` 后的
唯一下一 XNYS session `2026-07-10`；任何 source gap、边界漂移或资源异常仍立即停止。

## 2. 热路径

单日 S4 增量严格按以下顺序执行：

1. 从 exact parent frontier receipt 读取终止 session；只读 receipt 元数据，不读父历史 Parquet。
   base adapter 和 session receipt 都必须证明 calendar、reference、transform、三表 contract/schema 与
   Parquet writer policy 完全兼容。
2. 用 exact、内容寻址的 XNYS calendar 验证目标日恰好是下一个交易 session。
3. 由目标日期确定性生成 Massive `active=true` 与 `active=false` 两个 `ProviderRequest`，直接定位两份
   manifest；禁止 `glob`、`rglob`、目录扫描或 `latest`。
4. 从 manifest 派生 request、page 与 row bounds；不再使用固定 session/page/row count。
5. 如果 exact final receipt 已存在，核验目标日输出 pins 后直接幂等返回，不读取 Bronze page、
   不调用 transform；只重新认证两份小型 manifest JSON 以发现 source correction。CLI 也先走这条
   fast path，不会为了重建 run spec 而预读 Bronze page。
6. 否则只流式读取目标日两份 Bronze request，调用一次现有 `transform_asset_session`，同时生成
   `asset_observation_daily`、`asset_observation_version` 和 `universe_source_daily`。
7. 任一 blocking Critical/High QA 或 quarantine 出现时停止，不写 final receipt。
8. 三份数据 Parquet 和一份 session-local QA details 以 immutable 方式写入；final receipt 最后写，
   是本次结果唯一完成标记。

同一 session 若已有 final receipt 但 source binding 不同，必须返回 `correction_required`；clean append
不能覆盖或自动选择新 source。中断留下的无 receipt staging 不对 reader 可见，重试只补齐相同内容。

## 3. I2 控制对象

I2 不复用 Gate A 的 `PartitionReceipt`，因为后者专属于最终 `universe_daily`，而 S4 有三张 source 表，
其中 `asset_observation_version` 合法情况下可以是零行。

I2 使用两个 durable 对象：

- `S4SessionRunSpec`：parent frontier、单日 source binding、calendar、S1/S2 reference binding、三张已批准
  S4 contract、transform semantics 与 Parquet writer policy；所有 bounds 均由 exact inputs 派生。
- `S4SessionRunReceipt`：exact run spec、三份 partition receipt、row funnel、session-local QA details 与
  availability；receipt ID 不含 wall clock、RSS、日志路径或 writer Git commit。

首次 append 前，`S4BaseFrontier` v2 必须由 exact production S4 release-set marker 的 ID、SHA 与 bytes
反推。bootstrap 会验证完整 release-set control plane、三张表的合同/schema、FullRunPlan 语义、三表一致
且覆盖 pinned XNYS calendar 完整区间的 session paths，以及末日三 partition digest；只允许额外读取两张
很小的 exact S1/S2 reference 表，禁止打开任何历史 S4 DATA Parquet。之后每次消费 base 都重新执行同一
反推并要求整个 frontier 对象相等，调用方手写但形状合法的 frontier 不能进入转换。

Source knowledge time 与 control visibility time 分开：`pair_available_session` 只表示 Massive 双 manifest
何时可用于研究；`receipt_available_session` 必须不早于 source pair 与 parent frontier 两者的 availability，
且 parent/receipt 两个 availability 日期都必须存在于 exact pinned XNYS calendar。
历史 source 可以在较晚发布的 base 后补算，不能反过来把较晚 control visibility 回填成 source 时间。
writer commit 和携带它的 runtime artifact pin 永久保留在 exact envelope，但不传播进 parent 或后代的
semantic IDs。

执行器不会信任调用方拼出的 run spec：它会再次推导 canonical active/inactive request IDs 与 manifest
路径，并从 exact published S1/S2 release chains 重建 ticker type 与 exchange MIC vocabularies。生产入口固定
调用 module-owned `transform_asset_session`，不接受 caller-supplied transform。

`S4SessionSourceBinding` 和 `S4SessionPartitionReceipt` 是上述对象内的 typed value，不是额外审批层。
Release manifest 留到 I3；I2 不创建或发布 production release。

I2 candidate contract：

- 文件：`docs/silver/contracts/control/s7_5_s4_session_incremental_bundle-v1.candidate.json`
- Contract ID：`400a788ff4a6cc173b7814ac3b81b5609b75302d3f455bfcfe9b1e4a17b905a0`
- Candidate SHA-256：`43d1f8e9030cd36daa6d2423c87cb5f98eaa3a025e26541644dbad4e42986a2c`

## 4. QA 语义

现有 S4 transform 的所有单 session QA 保持原语义。以下边界必须显式保留：

- provider row 的 `active` 与 request scope 不一致：失败；
- 同一 ticker 同时存在 active/inactive：原始 observation 保留，但 universe 排除且 blocking；
- 合法的 exact duplicate row：按现有 S4 warning/selection 规则处理，不误判成 duplicate page；
- `cross_session_ticker_identity_churn_groups` 在单日运行中只记录为 deferred full-history QA，不能宣称
  已完成全历史检查；Channel C Full 和 I3 boundary detector 继续负责跨 session 语义。

## 5. I2 验收

- 正常相邻 session 只产生当日三份 data partitions；
- 周末、节假日和半日市使用冻结 XNYS calendar 正确推进；
- source gap、重复 session/page、active/inactive 缺失或错配均 fail closed；
- 输出 row count 全部来自 manifest 和实际 transform，version 零行合法；
- 同一 parent、inputs 和 semantics 重跑的 Parquet pins、receipt ID 与 receipt bytes 完全一致；
- 已存在 final receipt 时不再读取 Bronze page或调用 transform；
- active/inactive manifest 在 JSON canonical 排序后即使顺序反转，角色绑定、reload 与幂等重试仍正确；
- base bootstrap 只读 exact release-set control 与 S1/S2 小表，不读历史 S4 DATA；
- 放置损坏的旧 session Parquet 不影响新 session，证明热路径不读取旧 session content；
- 旧 S4 Full 回归测试全部通过。

完成本地验收并三端同步后，按已授权范围只执行 `2026-07-10` staging/no-publish；不得自动进入 I3、
checkpoint 或 publish。
