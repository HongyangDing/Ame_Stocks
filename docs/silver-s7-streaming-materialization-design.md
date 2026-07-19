# S7 四表生产物化：流式、两遍、原子可见设计

## 1. 目的与边界

S7 最终需要从固定的 S4 `universe_source_daily` release、Gate B Composite reference、
Gate C findings 以及五份 identity registry release 生成：

- `asset_master`；
- `ticker_alias`；
- `issuer_master`；
- `universe_daily`。

现有 `identity_materialization.py` 是小样本 contract/atomicity fixture。它要求调用方把四表
全部行放入内存，不能用于 69,376,329 行的正式 Full。生产入口必须是独立的流式 runner，不能
把 fixture runner 直接接到远程 S4 release。

本设计不改变 observed lineage，不把 identity quality 解释为 inactive/delisted，不产生强平信号，
也不把 S7 `backtest_identity_eligible` 误称为最终策略 tradability。

## 2. 固定输入

每次 FullRunPlan 必须逐字节绑定：

1. S4 atomic release set，以及全部 2,513 个 `universe_source_daily` Parquet receipts；
2. Gate B reference candidate/release manifest、数据 receipt、分类规则和 availability；
3. Gate C candidate/completion manifest 与 QA；
4. 五份 registry release manifest、每个 decision artifact 及 availability：
   `identity_adjudication`、`identity_cross_market_adjudication`、
   `provider_composite_override`、`share_class_adjudication`、`asset_transition`；
5. S5 两份 release 与 S6 release；
6. 四份 v4 derived contracts、XNYS calendar artifact、代码 commit/tree/runtime pins；
7. cutoff、资源上限、磁盘 hard floor 和唯一 worker。

禁止发现 latest、调用网络、接受调用方自造行、按多数/最近值纠正身份，或在运行时改写 registry。

## 3. 单行解析顺序

对每个 S4 membership source row，顺序固定为：

1. 保留原始 ticker、Composite、Share Class、CIK、MIC、type 和 source lineage；
2. 读取 Gate B 对 observed Composite 的封闭状态；
3. 同时计算三类 Composite correction registry 的 exact source-row 命中；
4. 命中数大于一时保留 membership，但 canonical/alias 为空且 identity ineligible；禁止优先级；
5. 唯一 Composite correction 才能修正 canonical Composite；零命中只在 Gate B `known_us`
   且其他 identity quality gate 通过时允许 direct identity；
6. Gate B unknown/no-mapping/ambiguous/conflict/unavailable 或未批准的 US-locale foreign
   Composite 均保留 membership、无 alias、identity ineligible；
7. canonical Composite 唯一确定后才允许应用 `share_class_adjudication`；它不能产生或改变
   `asset_id`，也不能修改 CIK；
8. `asset_transition` 只添加 predecessor/successor edge，不执行 override、不拼接收益；
9. 始终输出 `identity_quality_liquidation_signal=false`；最终 tradability 留给后续 security type、
   price/liquidity 和 entitlement policy。

## 4. 两遍流式算法

### Pass 1：逐 session 解析

- 按冻结的 2,513 个 session 顺序逐个读取一个 Parquet，单 session 内按 contract sort key 排序；
- reference 和 registry 索引只保留约数万级控制行；不得把 69M membership 放入内存；
- 每个 session 产生一个临时 resolved partition；
- 同时维护有限状态：asset/issuer 聚合器、每 ticker 的当前 alias interval、bounded QA examples；
- alias 仅在相邻 XNYS session 且完整 canonical/decision key 相同时延续；缺席、身份改变或
  ineligible 都关闭 interval，不能插值跨 gap；
- 每个 source row 只能被一个 Composite correction 结果消费；所有原始命中仍进入 collision QA。

### Pass 2：回填 alias 并写最终分区

- 先关闭所有 alias interval，生成排序且不重叠的 `ticker_alias`；
- 用按 ticker 排序的 interval index 第二遍读取临时 resolved partitions；
- 只有 eligible 且被唯一 interval 覆盖的行才能回填 `ticker_alias_id`；
- 每个 session 原子写一个最终 `universe_daily` partition，校验 PK、schema、排序和 source-row
  一一对应；
- 完成 `asset_master`、`issuer_master` 与 transition 双向边；
- 删除的只能是本次 staging 内、manifest 尚未引用的临时 partition；不得清理 Bronze、S4、旧
  candidate 或其他 workflow staging。

整个 candidate 只在所有文件、QA 和 manifest 复算通过后，通过同文件系统目录 rename 一次可见。

## 5. 必须为零的 Critical QA

- source membership omission/duplication/mutation；
- reference inventory unattempted rows；
- unknown 或未批准 foreign identity eligible rows；
- unapproved canonical override rows；
- multi-registry collision eligible/resolved/alias rows；
- Share Class correction before unique canonical Composite；
- Share Class correction changing `asset_id` 或 CIK；
- inverse bounce marked genuine transition；
- foreign-locale override leakage；
- inactive/delisted inference from identity quality；
- identity-quality forced-liquidation signal；
- alias overlap、gap interpolation、missing eligible alias、ineligible alias；
- transition self-edge、missing reciprocal edge 或 automatic return stitching；
- four-table FK/PK/schema/sort/source-release mismatch；
- artifact receipt、candidate replay、resource cap 或 disk hard-floor mismatch。

原始 Composite registry collision 数量作为 High/review numerator 保留；只有其 eligible、resolved、
alias 三个投影要求 Critical=0。

## 6. 资源与恢复

- 单 worker、非阻塞 per-plan lock；
- RSS hard cap 2 GiB；每 session/batch 后检查；
- FullRunPlan 冻结 source/output/temp bytes caps、wall clock、session/row counts；
- 剩余空间低于 60G 预警，预测或运行中低于 40G 立即拒绝/停止发布；
- durable intent 必须早于第一个 source Parquet content read；
- 每个 completed session receipt 可恢复，但同一 session 的半写 staging 必须 fail closed；
- stable candidate 已完成而 completion receipt 中断时，只允许校验后补 completion；
- idempotent replay 必须重算全部 output receipts、QA、manifest 和 control bindings。

## 7. Gate 与发布

顺序固定为：

`source binding → bounded size/profile preview → FullRunPlan → exact approval → Full candidate`

`→ QA/review → PublishPlan → exact approval → atomic four-table release set`

Full 不隐式授权 Publish；Publish 不修改 candidate。公开/研究 reader 只能读取发布后的 release set。
