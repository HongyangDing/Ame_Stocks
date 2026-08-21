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

## 源 FIGI 为空时的窄范围外部补全

`observed_composite_figi` 和 `observed_share_class_figi` 是 Massive 原始事实，永远不由互联网
查询回填。对当前 active、`type_code=CS` 且这两个字段同时为空的行，S7.5 允许生成一份独立的
`external_figi_resolution` release：

```text
S7.5 target universe partition
        -> exact ticker + primary MIC + observed CIK scope
        -> OpenFIGI TICKER/US/Equity response
        -> Nasdaq official symbol-directory MIC corroboration
        -> immutable external FIGI evidence + resolution release
        -> new S7_5_COMPLETE.json (old DELTA and four tables unchanged)
```

正常入口为：

```text
ame-silver-identity-incremental backfill-missing-figi --data-root <data-root>
```

若环境变量 `OPENFIGI_API_KEY` 存在，命令只在请求 header 中使用，不写入 request、response、
manifest、日志或 CLI 输出；否则按匿名 API 的五 job 批量和限速运行。运行中断时只保留一个按
source-set ID 定位的可恢复 workspace；成功固化 evidence/release 后删除本次 cache，不递归清理
任何不属于该运行的目录。

接受规则故意很窄：OpenFIGI 必须只产生一个 exact ticker、`Equity`、`Common Stock`、非空
Composite/Share Class FIGI pair；同时 ticker 必须存在于 Nasdaq 官方 symbol directory，并与
S7.5 的 `XNAS`、`XNYS` 或 `XASE` MIC 一致。歧义、无结果、错误证券类型、listing 不一致、缺 CIK
或不支持的 MIC 都继续 unresolved，不做多数投票和模糊匹配。CIK 是 overlay key 的一部分，避免
ticker 日后回收时沿用旧映射。

release 只提供 separate canonical overlay，不改变 observed lineage、历史 Parquet、CIK/issuer、
`active_on_date` 或核心 `backtest_identity_eligible`。其 availability 是所有接受证据抓取完成后第一
个 XNYS open，不能回填成原 membership 日期。`run-delta` 会校验 parent 就是 current marker，随后
自动继承 release；新行只有在 exact ticker/MIC/CIK 仍一致且 source FIGI 仍为空时才应用。S8 及后续
reader 必须从 verified `S75CompletionResult.external_figi_resolution` 读取这层 canonical identity，
不能绕过 marker 自行查询互联网。

2026-08-21 的首次正式运行以匿名 API 完成，1,108 个目标中 1,103 个具有可绑定 CIK 并实际查询；
989 个通过全部接受条件。其余结果为 no-result 104、非 Common Stock matching result 6、Nasdaq
listing/MIC 不一致 3、ambiguous 1，另有缺 CIK 5。正式 release ID 为
`2de5f9145e815a2958ba6d05c07d7bfd87d196a0cfda2217e0a564603574f315`。运行前后 target
Parquet SHA-256 均为 `2eb1ad8bdf8309dda6748301dcf675caf1e5673aae412fb74fbc5b7859ac3f5e`；
幂等复跑返回 `reused=true`，没有新增 evidence/release 或改变 marker。
