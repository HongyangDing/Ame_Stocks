# S7.5 factor-first reset

本文是 2026-08-19 生效的实施决议，取代此前冻结规划中的 S7.5 完成门。旧规划是 I0 的历史输入，
保持 byte-for-byte 不变；它描述历史设计，不再定义当前默认运行路径。

## 决议

S7.5 的目标是给因子研究提供稳定、可增量更新的身份和每日 membership，不是建立一套安全软件。
此前实现把审计证据、故障演练、发布指针和周期 Full 都放进了完成门，导致正常补一个交易日也要经过
大量与因子结果无关的控制对象。2026-08-19 起，正式主路径收缩为：

```text
已完成的 native-v2 BASE
        -> 一个真实、单日、增量 DELTA
        -> 目标 universe_daily 分区因子检查
        -> S7_5_COMPLETE.json (factor_ready_for_s8)
```

该完成标记只表示 S7.5 已经可以被 S8 消费；它明确写入 `s8_started=false`，不会启动 S8。

## 本质问题

此前的报错大多不是数据质量错误，而是控制层的三类设计错误：

1. **重复验证但定义不一致。** 底层 resolver 允许未决行保留 issuer/CIK 等描述性 lineage，上层
   DELTA 却把这些字段也当成可交易身份而拒绝，形成同一行在两层“一层合法、一层失败”。
2. **把一次观测写成恒定真理。** `16` 个 Gate-B miss 和 `1` 个 SOR 是某次输入的统计，不是合同。
   数据新增或证据更新后数量变化本来是正常现象，不应使构建失败。
3. **把存储和演练细节当成研究语义。** exact bytes 相同的 RunSpec 因目录不同而失败；每次发布前
   强制中断恢复、pointer rollback、独立 Full 和 Git 历史重放。这些可以帮助审计，但不改变因子值。

结果是边界修复不断产生新边界，代码和控制对象数量增长，而日常增量路径反而更脆弱、更慢。

## 正式保留的硬门

以下条件会改变回测结果，必须继续 fail closed：

1. **无未来信息：** 身份、source、registry 和完成 availability 不得晚于研究可用时点。
2. **membership 完整：** 一个 source membership 必须恰好对应一个 session/ticker 输出，不得遗漏或重复。
3. **可交易身份唯一：** `backtest_identity_eligible=true` 的行必须有唯一 asset、alias 和版本化 master link。
4. **未决或冲突不可新开仓：** unresolved、registry collision 或缺少必要映射的行保留 membership，
   但必须 `backtest_identity_eligible=false`，且不得进入 alias/master 可交易图。
5. **身份不确定不等于退市：** identity quality 不得改变 `active_on_date`，不得发出强平信号，也不得
   把已有持仓静默记为零收益。
6. **物理输出可读：** 目标分区的 exact bytes、schema、row count 和 session 必须一致。

这里的 `backtest_identity_eligible` 是身份层的新开仓必要条件，不是最终 tradability。最终交易资格仍由
security type、价格/流动性、entitlement/corporate-action 和策略规则共同决定。

## 改为报告、而非构建失败

以下内容继续记录在 QA/summary 中，但数量变化不阻断正常 DELTA：

- 新 ticker、unknown identity、Gate-B 尚未覆盖和 provider stale mapping 的数量；
- 需要人工复核的具体 ticker 和原因；
- provider bounce、临时证券和跨市场嫌疑统计；
- wall time、读取量、写入量和 RSS 的观测值（只有预计会实际耗尽磁盘或内存时才阻断）。

未决行可以保留 observed identity、issuer、CIK、source record 和 evidence lineage。只有 alias segment、
alias resolution version、asset/issuer master version 等会把行接入可交易身份图的字段必须为空。

## 从主路径移除

下列检查或流程不再决定 S7.5/S8 readiness：

- 固定的 `{Gate-B miss: 16, expired SOR: 1}` 统计；
- exact SHA/bytes 已经绑定后，再要求 RunSpec 必须位于某个固定目录；
- 每次日更都执行 deliberate interruption/retry exercise；
- I4 correction、I5 independent Full equivalence、I6 pointer rollback、I7 checkpoint compaction/Full
  reconciliation 的完成证明；
- 13 项跨 I0–I7 completion ceremony。

对应的 I4–I7 runner、pointer/cutover runtime、故障演练 runtime 及其专用测试已经从活动代码删除；
删除前分别做了文本依赖扫描和 Python import 依赖扫描。已有 S7/S7.5 数据、发布 manifest 和历史决议
仍原样保留，读取历史 release 所需的兼容解析逻辑也保留。换言之，删掉的是未进入因子主路径的执行框架，
不是已处理的数据或 lineage。

## 单一入口与完成条件

日常操作使用一个命令：

```text
ame-silver-identity-incremental run-delta --data-root <data-root>
```

命令从当前 S7.5 marker 精确取得 BASE/DELTA parent，按交易日历选择下一 session，读取该日已完成的
S4 receipt，依次 prepare、stage，并在目标分区通过上述六个硬门后写：

```text
manifests/silver/incremental/s7_5/S7_5_COMPLETE.json
```

该文件是很小的 current pointer；每次完成的不可变 marker 保存在
`factor-ready/completions/session_date=.../marker_id=.../manifest.json`。daily checkpoint 使用确定性 gzip，
因此不会再为每个交易日重复写一个百兆级 JSON。五个 identity registry 的完整历史控制链只在正式输入
绑定阶段共享缓存重放一次，不再在 materializer、stage 和 seal 验证中重复展开。

S7.5 完成条件只有：

1. native-v2 BASE 已成功且保持不变；
2. 一个真实新 session DELTA 成功，旧 partition 不被改写；
3. DELTA target partition 通过六个因子硬门；
4. marker state 为 `factor_ready_for_s8` 且 `s8_started=false`。

S8 可以在 marker 生成后另行开始，但本次重构不运行 S8。
