# Ame_Stocks Silver 分数据集处理与逐项验收计划

## 0. 结论、边界与当前状态

Bronze 已具备进入 Silver 的条件。最终 Bronze v9 已证明冻结范围内的下载计划和物理完整性
通过；已知问题属于需要在 Silver 显式处理的 provider 内容差异，而不是本地文件损坏。

本计划覆盖冻结目录中的 **31 个数据集 family（29 REST + 2 Flat Files）**。本文件只定义处理
顺序、输入结构、目标结构、时点规则、QA 和审批门，**不授权运行任何转换、下载或全量任务**。
编号 S1–S34 中，S7、S14、S15 是跨数据集派生审批点；其余 31 项与 31 个 Bronze family
一一对应。

截至 2026-07-15，S1–S6 已分别发布；S7 的只读 combined-source profile 已完成，旧四表 proposal
因 provider-FIGI bounce 风险撤回，现有一份受保护 `identity_adjudication` registry 加四份派生表共五份
revised schema candidates。当前为 `revised schema review / awaiting explicit re-approval`，尚无
transform、pipeline fixture、preview 或 release。不能描述成“全部 Silver 数据已处理”：

- S1 exchanges、S2 ticker types 和 S3 condition codes 已正式发布；
- S4 Assets 的十年 full scope 已按三个精确 `FullRunPlan` 完成并作为一个原子 release set 发布；
  三条 workflow 均为 `published` sequence 10，但 publication scope 固定为
  `identity_evidence_pending_s7`，`backtest_identity_eligible=false`；
- `minute`、`universe` 和 `coverage` 只有 2016-07-11、2021-07-12 两个验证日；
- 旧 Ticker Overview lifecycle/safe v2 是 legacy provisional oracle；正式 S6 release 已从
  lifecycle + Bronze 重算，发布 30,570 条 evidence-only DATA，另有 169 条 High quarantine，仍不是
  最终 `asset_id`、可回测身份或完整 PIT 行业表；
- S5 Ticker Events 已发布 15,173 行 request-status 父表和 12,895 行 ticker-change 子表；
  193 条空 target occurrence 进入标准 High quarantine，两表仍为
  `backtest_identity_eligible=false`；
- 旧 universe 代码会拒绝单个快照内的重复 ticker；新的 S4 runner 已在十年全量中保留版本，
  但永久 `asset_id` 与可回测身份仍必须等待另行审批的 S7；
- 复权、最终身份、PIT availability、SEC/财务、新闻和宏观的正式 Silver 转换尚未实现；
- 旧 `ame-materialize`/`ame-flatfiles convert` 仍只生成历史 pilot；正式路径已由独立
  `ame-silver` 合同和 release-only reader 隔离，S1–S6 已发布。S4 只允许显式的 identity-evidence
  reader 读取；S5 只发布事件证据；S6 只发布 `backtest_identity_eligible=false` 的 Overview
  evidence；Gold/backtest lineage 在 S7 前仍 fail closed。

现有 pilot 产物保持不变。正式 Silver 使用新版本路径，不覆盖 pilot，也不修改 Bronze。

## 1. 强制执行节奏：每个数据集单独停下

本文的 Phase 只表示依赖和阅读顺序，**不代表批准一个 Phase 就能连续处理其中全部数据集**。
每个数据集都采用下面的独立硬审批：

1. 展示 Bronze 字段、固定样例、目标表、类型、主键、去重和时点规则；此时不跑全量。
2. 用户批准 schema 后，编写该数据集的转换代码和 fixture 测试。
3. 只运行固定小样本，产出 `preview` build。
4. 汇报输入/输出样例、row funnel、quarantine、QA、运行时间、实际体积和全量外推。
5. 如果 full scope 大于 preview，先登记完整 inputs、代码、参数和资源预测的不可变
   `FullRunPlan`，再次展示并单独批准；不能用 preview approval 代替。
6. 获得 full-run approval 后才运行全量转换；全量 build 停在 `full_ready`，展示分区、校验和、
   覆盖率和异常。
7. 请求发布后停在 `awaiting_publish`；用户批准才生成不可变 `published` release。未发布的 build
   不供网页或 Gold 使用。
8. 代码和文档单独 Git commit，经 GitHub 快进到远程源码后暂停。
9. 不自动开始下一个数据集。

建议状态流为：

```text
planned → schema_review → code_ready → preview_ready → awaiting_review
        → full_run_plan_review → approved_full_run
        → full_ready → awaiting_publish → published
```

失败或拒绝的 build 保留证据，不通过覆盖文件或删除异常来“变绿”。

## 2. 正式 Silver 的统一输出合同

### 2.1 版本化目录

建议正式路径为：

```text
/mnt/HC_Volume_106309665/american_stocks/
├── silver/
│   └── schema=v1/
│       ├── reference/<table>/build_id=<digest>/
│       ├── identity/<table>/build_id=<digest>/
│       ├── market/<table>/build_id=<digest>/
│       ├── corporate_actions/<table>/build_id=<digest>/
│       ├── positioning/<table>/build_id=<digest>/
│       ├── sec/<table>/build_id=<digest>/
│       ├── fundamentals/<table>/build_id=<digest>/
│       ├── news/<table>/build_id=<digest>/
│       ├── macro/<table>/build_id=<digest>/
│       ├── quarantine/
│       └── qa/
├── staging/silver/
└── manifests/silver/
    ├── builds/
    └── releases/
```

同一逻辑的新版本写入新的 `schema=vN` 或 `build_id`，不能无痕覆盖。现有
`silver_unadjusted/` 两日 pilot 与 Overview safe v2 作为历史验证证据保留。
网页、Gold 和回测不按目录修改时间寻找“最新文件”，只能读取用户批准的 release manifest
中固定的 table → build_id 映射。

### 2.2 格式与分区

- Canonical 持久化格式为强类型、ZSTD 压缩 Parquet；Pandas、Polars、PyArrow 和 DuckDB
  都可以直接读取。不保存 pickle/Pandas object。
- 分钟线、日线和 universe 按 `session_year/session_date` 分区；通常每交易日一个 Parquet。
- 公司行动按 `execution_year` 或 `ex_dividend_year` 分区。
- SEC/财务按 `filing_year`，新闻按 `published_year`，宏观按 `observation_year` 分区。
- 当前字典/快照按 `capture_date` 和 schema version 保存。
- 不按 ticker、CIK 或 `asset_id` 物理分区，避免大量小文件。
- 单日文件若超过经 pilot 验证的内存安全上限，允许确定性拆成 `part-NNNNN`；不能为了坚持
  “一天一个物理文件”制造 OOM 风险。

### 2.3 Canonical 采用长表

基础类型约定：日期使用 Arrow `Date`，时间戳使用 `Datetime(ns, UTC)`，标识符使用保留大小写的
nullable `String`，价格/比率/允许碎股的 volume 使用 `Float64`，真实整数计数使用 nullable
`Int64`，状态使用 `Boolean` 或有版本的枚举字符串。缺失保持 null，不能用 `0`、空字符串或
`unknown` 偷换。会参与研究 join 的数组（ticker、manager、data type 等）拆成 bridge 表；只有
不参加行级研究的复杂规则对象才允许以稳定 JSON 保留。

分钟行情永久保存为 long format：

```text
session_date, bar_start_utc, asset_id, ticker_at_source,
session_segment, open, high, low, close, volume, transactions
```

不永久保存成“一只股票一行、390 个分钟槽展开成数百列”的宽表。半日市只有约 210 个 RTH
分钟，停牌、无成交、盘前盘后和 DST 也会使宽表 schema 不稳定。页面或研究代码需要矩阵时，
再对选定日期、股票池和字段临时 pivot。

日线是一日一证券一行的 long panel：

```text
session_date, asset_id, price_source,
open, high, low, close, volume, transactions, vwap
```

### 2.4 永久身份

```text
asset_master(
  asset_id, issuer_id, composite_figi, share_class_figi,
  security_type, resolution_status,
  first_seen_session, last_seen_session
)

ticker_alias(
  asset_id, ticker,
  valid_from_session, valid_to_session,
  evidence_type, confidence, source_record_id
)

issuer_master(
  issuer_id, cik, legal_name, resolution_status
)
```

- ticker 永远不是永久主键，且必须保留 provider 原始大小写。
- `asset_id` 表示可交易证券，优先由 Composite FIGI 使用固定 namespace 的 UUIDv5
  确定性生成；缺 FIGI 时只能生成带 `provisional` 状态的确定性内部 ID。
- `issuer_id` 表示发行人，可由规范化 CIK 确定性生成；CIK 不能代替 `asset_id`。
- ticker alias 使用半开区间 `[valid_from_session, valid_to_session)`。
- 身份修复通过新映射和新 release 发布，不静默改写历史 ID。

### 2.5 时间与回测可用性

禁止使用含义模糊的单一 `date`。根据数据类型保留：

```text
session_date
bar_start_utc
event_date / event_at_utc
period_start / period_end
filing_date / filing_at_utc
published_at_utc
available_at_utc
available_session
source_capture_at_utc
availability_rule
availability_quality
```

- 时间戳统一为 UTC aware；纽约时间只用于派生 session、RTH 和 cutoff。
- date-only filing/disclosure 默认该日收盘后公开，因此下一交易日才可进入日频信号。
- `period_end`、settlement date、transaction date 都不能替代 `available_session`。
- 当前快照只能在 capture date 以后使用。
- 没有历史 release/vintage 的宏观数据标记 `revised_history=true`、`pit_eligible=false`。

### 2.6 Lineage、quarantine 和 QA

每个 build manifest 至少保存：

```text
build_id, schema_version, transform_version, git_commit,
exchange_calendar_version, input_manifest_paths_and_sha256,
source_digest, parameters, row_funnel,
output_paths_and_sha256, started_at, completed_at,
qa_summary, approval_status
```

Flat File 可使用文件级 lineage，避免在数十亿分钟行中重复长路径。REST 合并、去重和 SEC
表按需要增加：

```text
source_artifact_id, source_request_id,
source_page_sequence, source_row_ordinal, source_row_hash
```

Quarantine 是 append-only 正式输出：

```text
source_record_id, table_name, issue_code, severity,
detected_build_id, source_pointer, field_name,
observed_value, expected_rule, review_status
```

Data Health 的统一检查结果为：

```text
build_id, table_name, partition_key, check_id,
severity, status, numerator, denominator, rate,
threshold, bounded_examples_path
```

每类至少展示 Bronze 输入行、Silver 接受行、精确重复 excess、quarantine、身份未解析、主键
重复、null、日期范围、输入输出 SHA 和同输入重跑 checksum。

### 2.7 磁盘和运行保护

- 每个 preview 必须实测输出压缩率、运行内存、临时峰值和耗时，再外推该数据集全量。
- 数据盘剩余空间低于 60 GiB 时预警；预计任务会让剩余空间低于 40 GiB 时拒绝启动。
- 不通过删除 Bronze、旧项目、旧 Docker Volume 或审计证据腾空间。
- preview 只写 `staging/silver`；用户批准 full build 后才写正式 versioned Silver 路径。
- 所有临时文件完成 fsync、hash、schema 和 row-count 校验后才原子发布。
- 任何一个数据集运行期间都不并发启动另一个未经批准的 Silver family。

## 3. 以量化后续处理为优先的明确取舍

| 可能的直觉或早期偏好 | 正式选择 | 原因 |
| --- | --- | --- |
| 每日文件中一行一股票、分钟为数百列 | 每日物理分区仍保留，但 canonical 是 sparse long table | 半日市、停牌、无成交和盘前盘后不会改变 schema；更适合 Parquet 增量扫描 |
| 保存成 Pandas 文件/pickle | 保存 Parquet，Pandas 用 `read_parquet()` | 类型明确、压缩好、跨语言、支持列裁剪，长期比 pickle 稳定 |
| ticker 直接作为股票主键 | `asset_id` + 有效期 ticker alias | ticker 会变化、复用且大小写有语义 |
| inactive 每日只保留一个 ticker 行 | 所有版本保留为证据，先解析身份再选研究行 | 同 ticker 可能代表不同历史证券；粗暴去重会产生身份错误 |
| 三套日线清洗成一套并覆盖差异 | Flat Day、REST Daily、minute-derived RTH 永久分源保存 | 三个产品的交易时段和 condition update 规则不同 |
| 缺分钟补 0 或前值 | 保持稀疏，另存 missing/停牌/无成交/源缺失状态 | 虚构 bar 会污染成交价、波动率和流动性 |
| 一个 `adjusted_close` 足够 | 保存 raw、事件、link return、split/total-return 明确口径和版本 | 防止双重复权和锚定日期变化 |
| 财务指标全部 pivot 成 filing-wide | Silver metric-long，Gold 对常用 allowlist 再 pivot | 指标稀疏、单位/来源/版本复杂，长表更适合 PIT 和 provenance |
| 稀疏事件也按每天建文件 | 公司行动/SEC/新闻主要按年度分区 | 避免大量空分区和小文件 |
| 当前 Float、Overview 市值/SIC 可补历史 | 禁止历史回填 | 会产生未来信息；SIC 也不是完整 PIT 行业史 |
| 13-F 没有 holdings 就当 0 | `holdings_status=not_public_or_unavailable` | 无明细不等于零持仓 |
| 清洗就是删除异常 | 异常进入带 lineage 的 quarantine | Bronze 不变，所有排除必须可审计 |

## 4. 依赖顺序

```mermaid
flowchart TD
    C["S0 统一 Silver 合同"] --> D["小型参考字典"]
    D --> A["Assets 每日观察"]
    A --> I["Ticker Events + Overview + IPO"]
    I --> M["最终 asset_id / alias / universe"]
    M --> CA["Splits + Dividends"]
    M --> P["Flat Day + REST Daily + Minute"]
    CA --> R["复权与日收益"]
    P --> R
    M --> S["Short / Float"]
    M --> E["EDGAR spine"]
    E --> O["Forms 3/4/13F"]
    E --> T["10-K / 8-K / Risk"]
    E --> F["Legacy Financials"]
    M --> N["News"]
    C --> X["Macro"]
```

完成身份、行情、公司行动和复权后即可先做价格型日频因子；不必等待 SEC、新闻和宏观全部
完成。Classic Barra 的历史 shares/market cap 和完整 PIT industry 仍是数据源限制，不能由
Silver 清洗自动创造。

## 5. Phase 0：共同合同，只建框架不处理数据

### S0 — Silver schema、preview 和 publish 框架

**状态（2026-07-14）：已获批准、实现并验收。** S0 最初只在临时合成 fixture 上验证；S1–S6
随后均通过该控制面运行各自获批范围。详细冻结合同见
[`silver-s0-contracts.md`](silver-s0-contracts.md)。当前已经具备：

- schema/QARule registry、SourceInventory/source layer 和完整 lineage；
- 不可变 build/approval/release manifest 与 hash-chain workflow；
- 对 scope-deferred preview 增加独立 `FullRunPlan` review/approval 状态；preview 本身不能授权
  更大的 full inventory，plan 必须另行冻结 inputs、代码、calendar、参数、资源预测和被 review 的
  preview build/manifest/event；
- QA/sample/quarantine Parquet 对账，以及 Critical/High 审批门；
- 只接受已发布 release ID 的公开 reader；
- `ame-silver fixed-cases/validate-contract/status/inspect-release` 四个只读检查命令。

S0 完成本身不授权 S1；`exchanges` schema 已于 2026-07-13 另行获得精确 contract 批准。

- 输入：无数据转换；只读取已有 schema、manifest 和 fixture。
- 输出：schema registry、build/release manifest、quarantine contract、QA contract、preview
  展示格式和固定案例清单。
- 已补齐：`preview → awaiting_review → full_run_plan_review → approved_full_run → … → published`
  的独立 scope-deferred 路径，同时保留早期同 scope preview 的兼容路径；CLI 不能直接把新产物写成
  “已完成”。
- 固定案例：正常日、半日市、current-only reference snapshot、2:1 split、reverse split、
  普通/特殊分红、停牌/缺分钟、ticker change、ticker reuse、退市、大小写相近 ticker、
  2019-08-12 异常、date-only filing、13-F header-only。
- 验收：不接触全量数据；使用 fixture 证明 schema、原子写、幂等、lineage 和审批状态。

S0 控制面已完成，S1–S6 均已完成各自获批范围并发布。S3 的真实 94 行
`condition_codes` 已通过双 workflow 的 preview、review-bound full build、release 与精确重放。
S4 又验证了 scope-deferred 三表全量 build 与原子 release-set 路径：2,513 个 session 的三个
workflow 均为 `published` sequence 10。S4 的发布范围仍是 `identity_evidence_pending_s7`，不会越过
S7 的永久身份 gate，也不会被通用 published reader 或 Gold/backtest 当作已经可交易的股票池。
S5 又验证了 coverage receipt v2 下的双粒度 source inventory、父子表联动审批和先父后子
发布；这些事件证据同样不越过 S7 的永久身份 gate。S6 又从 30,739 个 lifecycle/Bronze 响应
独立重算并发布 30,570 条安全 Overview 证据，将 169 条无可比身份字段的记录留在 High quarantine；
正式 DATA 全部 `backtest_identity_eligible=false`。详细证据见
[`silver-s6-ticker-overview-schema-review.md`](silver-s6-ticker-overview-schema-review.md)。S4/S5/S6
都仍等待 S7 合并裁决，任何一张表都不能单独充当可交易 universe。

## 6. Phase 1：小型参考字典

### S1 — `exchanges`

**状态（2026-07-13）：Phase 1 / `published`。** 用户批准的精确 contract
`1803d28f2b4b6088e32d27d06c7102111e4f141b6645a1059829732442f0e479` 已完成真实 27 行
preview、review-bound full build 和 publish。Preview 与 Full 均为 27→27、20/20 QA passed、
0 quarantine；release ID 为
`feab0e1f32a5685d1115a6e4e87aab8ff50c18b99c6336a8790ecba44464d838`。S2 精确 contract、真实
bounded preview、review-bound full build 与 release 也已完成；S1 在 S2 发布前后保持不变。

- Bronze：当前快照，一行一个场所；主要字段为 `id, name, acronym, mic, operating_mic,
  participant_id, type, asset_class, locale, url`。
- Silver：`reference/exchange_dim`，粒度为 `(capture_date, exchange_id)`；保留 MIC、场所类型和
  snapshot lineage。
- 处理：强类型化 MIC/ID，不把今天的字典伪装成历史交易所成员表。
- QA：`id` 唯一、MIC 冲突、asset class/locale 合法、后续 `assets.primary_exchange` 覆盖率。
- 建议 pilot：全部 27 行即可，但仍先展示 schema、再获批写正式 build。

### S2 — `ticker_types`

**状态（2026-07-13）：Phase 1 / `published`（sequence 9）。** 用户已逐字批准 contract
`b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d`。批准内容已经封装为
[`ticker_type_dim.schema-v1.json`](../backend/ame_stocks_api/silver/schema_resources/ticker_type_dim.schema-v1.json)，
并由 [`ticker_type_contract.py`](../backend/ame_stocks_api/silver/ticker_type_contract.py) 固定加载。
manifest-bound 读取和纯转换分别实现在
[`ticker_type_source.py`](../backend/ame_stocks_api/silver/ticker_type_source.py) 与
[`ticker_types.py`](../backend/ame_stocks_api/silver/ticker_types.py)。Synthetic fixtures 验收后，
manifest-bound runner 又只处理了当前 24 行真实 source：24→24、0 duplicate excess、0 quarantine、
17 列、20/20 QA passed，input/output sample 均为完整 24 行且未截断，5/5 fixed assertions passed。
用户随后明确授权 S2 继续到结束；review-bound full build 保持同一 24→24 row funnel、17 列、
20/20 QA passed 和 0 quarantine，三项 earliest-capture temporal QA 均为 0/0。详细证据和批准边界见
[`silver-s2-ticker-types-schema-review.md`](silver-s2-ticker-types-schema-review.md)。Preview build
`38998bc76c2ed04f3d9064e3a019cc953e6f1ed5d6594d9485a4978862f0b90d` 的六个 staging 输出均为
`0444` 且 `nlink=1`。Full build
`f02a6ad085e5f78ac15f3d1e26caf75079275204e7b55b58b4bb679bdfab2780` 的七个输出同样不可变，
其 DATA SHA-256 与 preview 均为
`8b3512e293edbfd5b6d813851720a9b5cb69dc212ab1ee3376daeee7f0fc6f11`。Release
`11a62f9c06ea5c609c159a7d619ba94cabbe39d3b07518fec279fa4758c882f6` 只暴露这一份 DATA；完整
trust chain/artifact 与 exact published replay 均通过，Bronze 和 preview metadata 未改变。
**S2 已结束；S3 与 S4 后续也已分别通过独立审批并发布。**

- Bronze：当前快照，一行一个类型；`asset_class, locale, code, description`。
- Silver：`reference/ticker_type_dim`，粒度 `(capture_date, asset_class, locale, type_code)`。
- 处理：保留 provider code，不提前把类型粗分成 common stock/ETF；研究 eligibility 另有版本。
- PIT：`manifest.completed_at` 是 capture evidence，只有严格晚于 capture 的首个 XNYS open
  才可用；request date label 不进入业务字段，current snapshot 不向历史回填。
- QA：候选键唯一、空 description、新增/消失/描述变化；非 `stocks` asset class 或非 `us`
  locale 的原行仍保留，但相应 High QA 失败会阻断 build，不通过 quarantine 将 domain mismatch
  隐藏。三项 temporal QA 只比较相邻 capture，最早 capture 的 numerator/denominator 均为 0。
  `assets.type` coverage 到 S4 按 PIT availability 正式验收，不把当前字典回填为历史事实。

### S3 — `condition_codes`

**状态（2026-07-13）：Phase 1 / 两个 workflow 均 `published`（sequence 9）。** 用户授权
S3 一次推进到完成，但授权不扩展到 S4。冻结的 Dim/Bridge contract ID 分别为
`de48f79738b2ed8d65c04a49c9f889ace84b69a4df7771051f67d30acd153192` 和
`a088a7ab0c562a9fbb90fb0a242be598b7d983d004af27973dd22666d16960dd`。Manifest-bound
source 94→94 Dim、94→123 Bridge，27/27 与 23/23 QA passed，0 duplicate excess、0 quarantine、
0 waiver/acceptance；两个 release ID 分别为
`9c0eb2eec54428bfa58754fc0b6f58a33b5fd804fe5917253f2a411574ab35b2` 和
`bdb5286b592dae80477cc45025f822c53aab140202f74cf41d2fc39075b86d66`。Release-only reader、
跨表 123/123 parent coverage、2/2 exchange FK 与 exact published replay 均通过。完整证据见
[`silver-s3-condition-codes-schema-review.md`](silver-s3-condition-codes-schema-review.md)。
**S3 至此结束；S4 后续已通过独立 FullRunPlan、PublishPlan 与 release-set 审批并发布。**

- Bronze：当前快照；`id, name, type, asset_class, data_types[], exchange, legacy,
  sip_mapping, update_rules`。
- Silver：`reference/condition_code_dim` 与 `condition_code_data_type_bridge`；数组展开后每行一个
  data type，保留 current/legacy 和 SIP/update rule JSON。
- 处理：不能按 `(asset_class, data_type, id)` 静默覆盖 legacy 版本。
- QA：数组/domain、当前/legacy 歧义、exchange 外键、SIP mapping/update rules 可解析。
- 用途：解释 provider aggregate 差异；没有逐笔数据时不声称能反推每笔 eligible trade。

## 7. Phase 2：每日股票池和永久身份

### S4 — `assets` active + inactive

**状态（2026-07-14）：Phase 2 / 三个 workflow 均 `published`（sequence 10）。** 已完成全部
2,513 个 session、5,026 个 active/inactive manifests、72,038 pages 和 69,381,182 source rows 的
manifest-bound 十年转换。三张精确 contract、单日 bounded preview、三个独立 `FullRunPlan`、full
build、`PublishPlan` 和原子 release set 均按独立审批门推进。完整证据见
[`silver-s4-assets-schema-review.md`](silver-s4-assets-schema-review.md)。

远程 runtime identity：

- `asset_observation_daily` workflow
  `c1bae241ed90e49aed1ae8a98b6801f511d6abaac2cef93c66ccba59d33775ec`；
- `asset_observation_version` workflow
  `989c8c513905e2710714c0b6f94352119e8fb1128147d8c2db9486c1e03df6da`；
- `universe_source_daily` workflow
  `918ebc04d2eded87243387804d58fa9f24e4282ee27a8a26ac6ac22f4390b755`。

三条 chain 都已走完
`planned → schema_review → code_ready → preview_ready → awaiting_review → full_run_plan_review →
approved_full_run → running_full → full_ready → awaiting_publish → published`。单日 bounded-preview
的 shared SourceInventory 为
`d61a9eb9ff52f721f61e931cdf0ec3460b1f361e619b8f731b13562f875adc25`：37 pages、35,647 rows，
同时绑定两个 Bronze request manifests 与已发布的 S1/S2 release manifests。

历史 bounded-preview 证据如下；它只负责支持后续 full plan review，不曾隐式授权十年范围：

| Preview table | Output | Unmapped | Build ID | Awaiting-review event |
| --- | ---: | ---: | --- | --- |
| `asset_observation_daily` | 35,647 | 0 | `baaf04a909973984f51eaaeccfd3e2408763acd6aa76403cdf62017edd0422ba` | `4d172aa12ff368e0dd42f77df83eeeadcba6c51a800baac10ab4fdda11e7e53c` |
| `asset_observation_version` | 82 | 35,565 | `1c560bbaffbb7a838fbcbccf90d0da83e4c69f2866515bf860f0c05eb1406e8f` | `b0fe4549477f079fb92f75cc05732baa5a7de04820c40bfca659c37a7b195c47` |
| `universe_source_daily` | 35,606 | 41 | `442ac3894e68e14332621b73de6b4eb83e362c549328223c57b63f80828dc755` | `d9d993eafa729de1f88b785ee1752f0144e7a3a5ebb6f9fc082a0e611c564b76` |

三表 preview 均为 input=accepted=35,647、`version_preserved_rows=82`、0 quarantine、0 blocking
QA；warning 仅保留需人工解释的 provider/identity diagnostics。相同参数和 sequence-5 event SHA 的
重跑成功且 build/event 均未变化。

十年 full build 与发布结果：

| Table | Full rows / bytes / partitions | Full build | Release / published event | Waiver / quarantine |
| --- | --- | --- | --- | --- |
| `asset_observation_daily` | 69,381,182 / 8,248,987,847 / 2,513 | `9e3b5df531c01d1bcdd73cbd9cdf747bd30cdff459481b262e1ed7a23f40acc4` | `26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c` / `fffcdd9f0946acfa9d4aaa83319642a993320cb302de897f488840cc58bc6f43` | 7 / 0 |
| `asset_observation_version` | 9,706 / 14,376,829 / 2,513 | `59708791dc897214d3151dfd7da6b15534800afabf0c36dd36c566bd8d01ef9a` | `b422fd05df859b33587b8ece80d078247dd972d01d272710ef49c3529b0e54be` / `0f4297e151ea94f9a75643d477ff7fa0817c0afc255417a457971c8d786b0aa2` | 2 / 0 |
| `universe_source_daily` | 69,376,329 / 7,661,290,322 / 2,513 | `21921c72c4be79665d41077664f8f027a1beb9ac0600ff4c6610d4f40638b185` | `c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614` / `f48c695c5c3e8354a55b6debbba72f70a059cb82f21aa3a517476425a273da5d` | 8 / 0 |

三表由 PublishPlan
`908b0982f273149e2f5a4340edcf369f9b2463a09a85d92677c8bd401564ec01`（SHA-256
`cf6129c7149d2f38297d443e533f1d3e6f79eafe976b012d19d69830a4fa779d`）绑定成 release set
`f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d`（SHA-256
`937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13`）。用户批准的 7/2/8 个
Medium warning result ID 逐项绑定，三表 accepted quarantine issue 均为空；runtime RSS review 明确
accepted。发布范围固定为 `identity_evidence_pending_s7`，`backtest_identity_eligible=false`，因此
S4 的“published”只表示身份证据层不可变可读，并不表示永久 identity 或回测股票池已经完成。

- Bronze：查询日 × active 参数 × ticker；主要字段为 `active, ticker, type, name, market,
  locale, primary_exchange, currency_name, cik, composite_figi, share_class_figi,
  delisted_utc, last_updated_utc`。
- Silver 输出：
  - `identity/asset_observation_daily`：32 字段，保留 69,381,182 个 source observations，duplicate
    version 不去重；
  - `identity/asset_observation_version`：24 字段，只投影 4,853 个 multi-version groups 的
    9,706 个 members，不复制 singleton；
  - `reference/universe_source_daily`：38 字段，每 `(session_date, exact ticker)` 一个经过审计的
    selected source observation，active/inactive 均保留。
- 处理：4,853 组 duplicate 全为 inactive；精确分类为 2 exact、2,115 only-last-updated、2,736
  delisted+last-updated。4,851 个语义版本组仅在 identity fields 一致且 `last_updated_utc` 有唯一最大值
  时选择；2 个 exact groups 只用最小 page/ordinal 选择物理 occurrence。`delisted_utc` 和 row hash
  不替语义冲突决定 winner，未来 tie/conflict 必须 unresolved 并阻断 universe。
- PIT：`session_date` 是实际发送给 provider 的 reconstructed membership effective date；2026 年
  `source_capture_at_utc` 与 operational availability 单独保存。61,106,281 rows 的
  `last_updated_utc` 晚于 query session，因此它不是历史 research availability。
- Identity：不生成 provisional/final `asset_id`。同日 FIGI 对多 ticker 和跨日 ticker/FIGI/CIK
  churn 证明单字段不能安全唯一化；只保存 `identity_link_status`，永久 identity 留给 S7。
- QA：active/provider flag mismatch=0、active/inactive exact-ticker overlap=0；冻结每日 pair、row
  funnel、case-sensitive ticker、duplicate selection、parent coverage、当前 S1/S2 仅诊断 coverage
  与禁止 current-dictionary backfill。唯一当前 S2 unmatched type 为 `INDEX` 1,188,877 rows，保留。
- 边界说明：旧 materializer 遇到这些重复仍会报错，S4 只使用新的 version-preserving runner；
  十年 full scope 已发布，但在 S7 前只能通过专用 identity-evidence reader 读取，不能进入通用
  production reader、Gold 或 backtest lineage。

### S5 — `ticker_events`

**状态（2026-07-14）：Phase 2 / 两张业务表均已 `published` sequence 9。** 正式范围与
日期质量方案、双 source inventory、schema contract、QA/quarantine 以及 release 证据见
[`silver-s5-ticker-events-schema-review.md`](silver-s5-ticker-events-schema-review.md)。

- Bronze：每个 identifier 一条事件时间线；`results.name/cik/composite_figi/events[]`，事件含
  `date, type, ticker_change.ticker`；另有稳定 404 receipts。
- Silver：发布 `identity/ticker_event_request_status` 和 `identity/ticker_change_event` 两张业务表；
  异常 occurrence 使用每个 build 的标准 `quarantine-record.parquet`，不建第三个业务 workflow。
- 处理：15,173 个正式 identifier 全部进父表，其中 11,471 个 complete、3,702 个 404；
  100 个 pilot identifier（含 84 个 pilot 404）全部排除。13,088 个 raw events 产生 12,895 个合法子表行
  和 193 个 High quarantine；同响应的合法 sibling 保留。
- QA：事件键、target ticker、request-to-response FIGI、父子外键、空 target、同日多事件、ticker/FIGI
  重用和日期质量；endpoint 不提供 prior ticker，因此 S5 不伪造“前后 ticker”。
- 发布：父表 15,173→15,173，0 quarantine；子表 13,088→12,895 + 193 quarantine；
  所有未 waiver 的检查通过，两表 `backtest_identity_eligible=false`。

### S6 — `ticker_overview`

**状态（2026-07-14）：Phase 2 / `published` sequence 9。** 正式 contract、source profile、
coverage receipt、双 source inventory、审批、build/release 和发布后信任链证据见
[`silver-s6-ticker-overview-schema-review.md`](silver-s6-ticker-overview-schema-review.md)。Workflow
ID 为 `bb474b8a62d8d4f316b906ca082197800a3ca4917512fbe6f8e31a0a950a85c6`，preview/full build ID
分别为 `d9d40f14475916a4a83f442281fbfcb85793947da2a071eb033abf012264ed8c`、
`f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0`，release ID 为
`8715f90d0e01f990e9738b9266edfeb2830a76d59a00ae4fb7490d9f077092a5`。正式 funnel 为
30,739→30,570 DATA + 169 High quarantine；22 个 Critical 检查通过，4 个精确 warning 及 169 个
quarantine issue 只在 full/publish 审批中按 ID 接受。169 条仍是 `pending` unresolved evidence，
不是已裁决身份；全部 DATA 均为 evidence-only、`backtest_identity_eligible=false`。

- Bronze：每个 deduplicated identity lifecycle 查询一次；身份、SIC、list date，以及不安全的
  current-looking market cap/shares 字段。
- Silver：`identity/ticker_overview_safe`，粒度为一 lifecycle；固定 allowlist 包含生命周期、
  身份校验、SIC、list date 和 reference 字段。
- 处理：现有 safe v2 可作为 review 输入；正式 release 不自动重下。`market_cap`、
  `weighted_shares_outstanding`、`share_class_shares_outstanding` 永远不进入第一阶段历史表。
- QA：30,570/30,739 identity match；169 行隔离/人工复核；`list_date <= query_date`；SIC/list
  date coverage；不得把 safe SIC 描述成完整 PIT industry。

### S7 — `identity_adjudication` + `asset_master` / `ticker_alias` / `issuer_master` / `universe_daily`

**状态（2026-07-15）：`revised schema review / awaiting explicit re-approval`。** 只读
combined-source profile 和五份 revised candidate contracts 已完成，详见
[`silver-s7-identity-resolution-schema-review.md`](silver-s7-identity-resolution-schema-review.md)。
当前没有 S7 transform、pipeline fixture、preview、FullRunPlan 或 release。旧四份 ID/SHA 已撤回；
只有用户逐字批准新五份 Contract ID/candidate SHA 后，才可进入 code-ready；后续 detector case
manifest、adjudication registry、bounded preview、Full 和 publish 仍分别审批。

S7 必须在 S4–S6 分别验收后单独审批：

- 综合 Assets、Ticker Events 和 Overview 身份证据；
- 确定性检测短期 `A → B → A`，但 detector 只报警，不能自动 canonicalize；
- `identity_adjudication` 先作为 episode-scoped、append-only、显式批准且可追加
  `adjudicated_unresolved` 撤回版本的受保护 registry 单独发布；
- 永久分离 provider-observed 与 canonical research 的 Composite FIGI、Share-class FIGI 和 CIK hierarchy，
  禁止多数/最近值静默覆盖，也禁止 FIGI 裁决绕过其他 relationship conflict；
- Massive 无法确认的 case 可使用监管、交易所、FINRA、公司公告或 reviewed identifier reference，但
  必须先固化原始快照、URL、SHA、公布/抓取时间、具体 assertion 与 availability；可变 URL 或第三方
  majority 不能自动产生 canonical override；
- 生成确定性 `asset_id`、`issuer_id` 和 ticker 有效区间；
- 生成 `reference/universe_daily`，以每个信号日的 active snapshot 为左表，附最终
  `asset_id`、身份状态、security type 和版本选择 lineage；
- 同 ticker 不同 observed FIGI 必须拆成不同 observation interval；canonical asset 默认也不同，只有
  绑定完整 episode、证据、审批与 availability 的版本化 adjudication 才能让不同 observed FIGI
  指向同一 canonical asset；
- 所有四张派生表都绑定明确的 identity-resolution cutoff 与不晚于 cutoff 的 registry release；不能把
  一张后来生成的 revised 表简单按日期 mask 后冒充 historical-as-known；
- 仅凭名字、ticker root 或相同 CIK 不自动合并 share class；
- identity quality 不改变 active/inactive/delist，也不能独自触发强制平仓；
- S7 只输出 fail-closed continuity state 和恒假的 identity-quality liquidation signal；已有持仓遇到身份
  不确定时不强平、不填零收益、不静默 carry stale price 的实际执行逻辑，必须在后续 backtest engine
  fixture 中作为独立 blocker test 验证，不能由 schema approval 代替；
- 输出 identity coverage、conflict、provisional 和人工 review 清单。

### S8 — `ipos`

- Bronze：一个 provider IPO/DPO 事件版本；`ticker, issuer_name, announced_date, listing_date,
  last_updated, ipo_status, offer prices, shares, offer size, exchange, security identifiers`。
- Silver：`corporate_actions/ipo_event_version` 与 `identity/asset_listing_event`；无 provider ID 时
  使用规范化 row hash，保留修订版本。
- 处理：`listing_date` 可用于上市年龄；今天看到的 rumor/pending/final 状态不能假装成历史
  每日 PIT 状态。
- QA：asset link、listing date 与首次 active/首个 bar、状态 domain、重复 hash、异常日期顺序。

## 8. Phase 3：公司行动

### S9 — `splits`

- Bronze：一行一次事件；`id, ticker, execution_date, adjustment_type, split_from, split_to,
  historical_adjustment_factor`。
- Silver：`corporate_actions/split_event`；包含 `asset_id, execution_date,
  split_ratio_new_per_old, provider_historical_factor, event_factor, source lineage`。
- 处理：事件 ratio 从 `split_from/split_to` 独立计算；provider 累计因子只作 QA，不能再当事件
  ratio 连乘。
- QA：正数比例、同日多事件顺序、asset link、2:1/reverse/stock dividend 手算、拆股前后价格
  与 volume/shares 方向相反。

### S10 — `dividends`

- Bronze：一行一次现金事件；`id, ticker, cash_amount, split_adjusted_cash_amount, currency,
  declaration_date, ex_dividend_date, record_date, pay_date, frequency, distribution_type,
  historical_adjustment_factor`。
- Silver：`corporate_actions/dividend_event`，分开保存 raw cash 与 current-share-basis cash。
- 处理：总收益按 ex-date；公告因子只能在 declaration date 后；币种未转换时不能直接加入 USD
  收益；special/irregular 不当普通季度分红。
- QA：日期顺序、币种、金额、重复、split chain 一致性、普通和特殊分红手算。

## 9. Phase 4：三套未复权行情

### S11 — `day_aggregates`

- Bronze：每日 gzip CSV，一行 ticker-session；`ticker, volume, open, close, high, low,
  window_start(ns), transactions`。
- Silver：`market/provider_day_flat/session_date=.../bars.parquet`；粒度
  `(session_date, asset_id)`，保留 `ticker_at_source`、raw OHLCV、timestamp status 和 QA flags。
- 处理：ns → UTC/ET/session；连接 `asset_id`；不复权、不覆盖其他日线。
- QA：主键、OHLC 不变量、有限/非负、日期归属、identity coverage；2019-08-12 的 29 个
  非规范 timestamp 进入 quarantine/flag，不静默修正。
- 现状：现有通用 Flat converter 没有独立 Day 单测，也不会隔离上述 29 行；正式 schema 需新版本。

### S12 — `daily_bars` REST Daily Market Summary

- Bronze：请求交易日 × ticker；`T, o, h, l, c, v, vw?, n?, t, otc?`，外壳含
  `adjusted, queryCount, resultsCount`。
- Silver：`market/provider_daily_rest/session_date=.../bars.parquet`，保留
  `requested_session_date, provider_window_end_utc, exchange_close_utc, vw, transactions` 及
  missing flags。
- 处理：`vw/n/otc/results` 均按官方 optional 合同；`t` 是名义 16:00 ET，不是半日市真实
  close/available time；2016-07-11/12 是 entitlement 缺口，不造假行。
- QA：外壳 count、OHLC、OTC=false、时间合同、字段 missing、与 Flat Day 的 coverage/numeric
  difference。差异只进 QA，不互相覆盖。

### S13 — `minute_aggregates`

- Bronze：每日 gzip CSV，一行 ticker × 有合格成交的分钟，字段与 Flat Day 相同。
- Silver：`market/minute_unadjusted/session_date=.../bars.parquet`；粒度
  `(session_date, asset_id, bar_start_utc)`；增加 `ticker_at_source, session_segment,
  is_half_day, minute_index, identity_link_status, qa_flags`。
- 处理：保留稀疏 bar；标记 pre/RTH/post/outside；不填满 390/210 行、不把 null 写 0、不前填
  成真实成交；按 `asset_id, bar_start_utc` 排序。
- QA：源键和身份后键、OHLC、非负 volume/transactions、文件日期、DST、RTH/半日市边界、
  重复差异、每日 ticker/rows/volume 分布。
- 资源规则：先用正常日和半日市测内存、时间、压缩比和临时峰值，再决定 Polars lazy/batched
  实现。现有代码是整日 `read_csv`，不能在计划中声称已真正 streaming。
- 全量硬停点：45.51 GB Bronze minute 只能在 pilot 报告和磁盘外推获批后运行。

### S14 — 派生 coverage、RTH 日线和执行代理

在 S11–S13 分别验收后，另行审批以下非 Bronze 产物：

- `market/market_coverage_daily`：active-without-bars、inactive-with-bars、bars-without-reference、
  minute/RTH minute count；
- `market/daily_rth_from_minute`：按真实交易日历聚合的 RTH OHLCV；
- `market/open_30m_execution_proxy`：对 `[09:30, 10:00)` ET 内有 bar 的分钟计算
  `sum(minute_close * minute_volume) / sum(minute_volume)`，明确命名
  `minute_close_vwap_proxy`，不能标成真实 VWAP；无足够窗口数据时输出 unavailable，不用全天
  VWAP 或未来分钟替代。

Flat Day、REST Daily、minute-derived RTH 永久使用不同 `price_source`。Canonical selection 或
fallback 需要单独 policy 和审批，保存 `selected_source/fallback_reason`。

### S15 — 派生复权、日收益和总收益

在 S9–S14 通过后单独审批：

```text
market/daily_adjustment_factor
market/daily_return
```

核心字段：

```text
asset_id, session_date, price_source,
split_factor, dividend_cash, currency,
close_raw, return_price, return_total,
return_status, adjustment_version
```

优先保存逐日 link return，而不是只有一个随最新 anchor 变化的 `adjusted_close`。如需复权价格
曲线，必须显式保存 `anchor_date`。分钟数据默认不复制一份 adjusted 全量表；按需连接每日因子，
避免重复占用数十 GB。

## 10. Phase 5：做空与当前截面

### S16 — `short_volume`

- Bronze：ticker-date；short/exempt/non-exempt/total volume、ratio 和 venue 分项。
- Silver：`positioning/short_volume_daily`；粒度 `(session_date, asset_id)`，保存 coverage scope
  和 `available_session=next_session` 的版本化规则。
- 处理：重算 short volume、exempt/non-exempt 和 ratio；不强制等于 SIP 总成交量。
- QA：分项加总、ratio、非负、asset join、日期连续性和 2024-02-06 覆盖起点。

### S17 — `short_interest`

- Bronze：ticker-settlement date；`short_interest, avg_daily_volume, days_to_cover`。
- Silver：`positioning/short_interest_observation`；增加 `release_date, available_at,
  available_session, availability_status, pit_eligible`。
- 处理：重算 days-to-cover；settlement date 不能直接当可用日。
- QA：数值关系、双周节奏、asset join、重复和 release lag。
- 硬限制：在 FINRA release calendar 或保守 lag policy 单独获批前，`pit_eligible=false`。

### S18 — `float`

- Bronze：最新 ticker-effective-date 截面；`free_float, free_float_percent`。
- Silver：`reference/current_float_snapshot`，粒度 `(capture_date, asset_id)`；仅当前展示/方法 QA。
- 处理：不能展开为十年日表，不能进入历史 Size/Turnover；一行缺 ticker 进入 quarantine。
- QA：asset coverage、百分比范围、重复、effective/capture date、历史 join 防护测试。

## 11. Phase 6：SEC spine、所有权、文本和财务

所有 SEC 表都依赖 `edgar_index` 的 filing spine。修订 filing 保留版本/lineage，不无痕覆盖。

### S19 — `edgar_index`

- Bronze：accession × registrant/CIK metadata；`accession_number, cik, ticker, issuer_name,
  form_type, filing_date, filing_url`。
- Silver：`sec/filing_header` 与 `sec/filing_registrant`；同 accession 可有多个合法 registrant。
- 处理：规范整行 hash 去除 22,032 个精确重复 excess；6,148 个候选 metadata 版本保留版本
  证据；不能按 accession 单键删掉联合申报 CIK。
- QA：候选键、exact duplicate funnel、filing date/form/url、CIK 格式和后续 endpoint accession
  coverage。

### S20 — `disclosure_taxonomy`

- Bronze：当前快照；119 行三级 disclosure 分类和 description。
- Silver：`reference/disclosure_taxonomy_dim`，粒度 `(capture_date, taxonomy_version, 三级路径)`。
- QA：路径唯一、版本/capture date、所有 8-K disclosure 类别覆盖；不声称 taxonomy 历史不变。

### S21 — `risk_taxonomy`

- Bronze：当前快照；140 行三级风险分类。
- Silver：`reference/risk_taxonomy_dim`，粒度 `(capture_date, taxonomy_version, 三级路径)`。
- QA：候选键、描述、所有 risk factor 类别覆盖；当前 taxonomy 不回填为历史版本事实。

### S22 — `form_3`

- Bronze：filing 被拆成证券/持仓行；含 filing、issuer、owner、security、shares、footnotes。
- Silver：`sec/form3_filing`、`sec/form3_position_line`、可选 footnote child table。
- 处理：filing header 与行事实分离；整行 hash；保留修订、直接/间接持有和脚注。
- QA：EDGAR accession/date/issuer、行级重复、owner/security domain、shares、amendment lineage。

### S23 — `form_4`

- Bronze：filing 下 transaction/holding 行；含 transaction code/date、A/D、price、shares、
  security type、post-transaction holdings 和 footnotes。
- Silver：`sec/form4_filing`、`sec/form4_transaction_line`、`sec/form4_holding_line`、footnotes。
- 处理：交易方向不能只看 transaction code，必须结合 A/D、record/security type、脚注和修订。
- QA：EDGAR、transaction value 重算、买卖符号手算、holding/transaction 分离、迟报和修订。

### S24 — `form_13f`

- Bronze：filing metadata 或 information-table holding；含 accession/filer/period/CUSIP/value/
  shares/put-call/discretion/voting。
- Silver：`sec/form13f_filing`、`sec/form13f_holding`、`sec/form13f_manager_bridge`、
  `sec/ownership_asset_link`。
- 处理：152 条 header-only 保留在 filing header，设置
  `holdings_status=not_public_or_unavailable`，不写入 holding、不解释为 0；HR/A replacement/
  addition policy 单独审批；额外 3,396,312 行 audit pilot 永远不混入正式 41 季权威输入。
- QA：EDGAR、holding 完整/整组缺失、数值/domain、CUSIP/asset link confidence、quarter end 与
  filing availability、amendment。
- 全量硬停点：正式 13-F 超过一亿行，先用一个季度测内存、存储和 join 规则。

### S25 — `ten_k_sections`

- Bronze：CIK × filing date × section；`ticker, period_end, section, text, filing_url`。
- Silver：`sec/ten_k_document` 与 `sec/ten_k_section_text`，metadata 和长文本分离。
- 处理：确定性 Unicode/空白规范化、content hash、版本保留；情绪/embedding 属于 Gold。
- QA：EDGAR/CIK、section domain、正文 hash/version、9,910 个候选版本和 8 个精确重复处理、
  `available_session` 不使用 period end。

### S26 — `eight_k_text`

- Bronze：一份 8-K/8-K-A 解析正文；`accession, cik, ticker, form_type, filing_date,
  items_text, filing_url`。
- Silver：`sec/eight_k_document` 与 text payload；修订作为独立版本。
- QA：EDGAR accession/date/CIK、正文 hash、1 个精确重复 excess、空文本和 amendment lineage。

### S27 — `eight_k_disclosures`

- Bronze：filing × 三级分类 × supporting text；同 accession 可多分类、多 ticker；2022 前成功
  空响应。
- Silver：`sec/eight_k_disclosure_fact`、`sec/disclosure_ticker_bridge`、supporting text payload。
- 处理：按 filing identity + taxonomy + text hash；展开 ticker bridge；2016–2021 标为 provider
  coverage unavailable，不解释成零事件；pilot 永远非权威。
- QA：EDGAR、taxonomy 100% decode、row hash、ticker/asset link、coverage boundary、日期和修订。

### S28 — `risk_factors`

- Bronze：CIK/filing date × 三级风险类别 × supporting text。
- Silver：`sec/risk_factor_fact`、text payload 和 taxonomy link。
- 处理：规范整行 hash 去除 30,449 个精确重复 excess；无可靠 accession 时显式保存 linkage
  confidence，不能猜 filing。
- QA：taxonomy、CIK/asset、正文 hash、candidate versions、filing availability 和重复 funnel。

### S29 — `legacy_financials`

- Bronze：377,576 个 report rows，内部有 18,124,688 个
  `financials.<statement>.<metric>`；根字段含 CIK、period、filing/acceptance、timeframe、SIC、
  tickers 和 accession URL；metric 含 value/unit/source 及 xpath/formula/derived_from。
- Silver：
  - `fundamentals/financial_report_header`；
  - `fundamentals/financial_metric_long`；
  - `fundamentals/metric_derivation_bridge`；
  - `fundamentals/financial_ticker_bridge`；
  - `quarantine/legacy_financials`。
- 处理：metric-long 保存 `statement, metric_name, value_f64, unit, provenance_method, xpath,
  formula`；direct/imputed/derived 分层；所有 `derived_from` filing 必须当时已公开。
- QA：39 条 `end_date > filing_date` hard quarantine、2 条 EDGAR 日期差异、492 行/480 accession
  EDGAR coverage gap、acceptance、CIK、unit/value、重复 ticker array、空 SIC、derived lineage。
- 硬限制：这是 legacy fallback；没有 PIT shares/market cap 时，不能据此宣称 classic Barra
  基本面/Size 已完整。常用 metric-wide 表只能在 Gold 用固定 allowlist 另行审批。

## 12. Phase 7：新闻

### S30 — `news`

- Bronze：一篇 article；`id, title, description, author, published_utc, article_url, publisher,
  tickers[], keywords[], insights[]`。
- Silver：`news/article`、`news/article_ticker_bridge`、`news/article_insight`、publisher dimension；
  文章正文/描述只存一次。
- 处理：published UTC → ET/session；多 ticker 和 insight 展开；保留 canonical URL、content hash、
  修订/聚合来源和 asset mapping status；模型 sentiment/embedding 属于 Gold。
- QA：article ID/URL/hash、时间、syndication/重复、ticker link、insight domain、公开网站许可边界。

## 13. Phase 8：宏观数据

四类数据统一规范为：

```text
macro/observation(
  series_id, observation_date, value, unit, source_dataset,
  published_at_utc, available_session, capture_date,
  revised_history, pit_eligible, availability_rule
)
```

Provider 的宽行可保留为 source projection，但正式研究表拆成长 series，因为同一响应中的字段
频率、开始日期和发布时间不同。

### S31 — `treasury_yields`

- Bronze：date + 1m/3m/6m/1y/2y/3y/5y/7y/10y/20y/30y yields。
- Silver：每个 tenor 一条 `series_id` observation。
- QA：期限 domain、百分比单位、自然缺失、曲线覆盖起点；不存在的历史 tenor 不补 0/前值。
- PIT：release/availability policy 获批前不假设 observation date 当日盘前已知。

### S32 — `inflation`

- Bronze：date + CPI/core/同比/PCE/core/PCE spending。
- Silver：每个指标独立 series，保存频率、单位和 availability status。
- QA：频率、单位、值域、系列起点、缺失和修订；不同字段不能因在同一 row 就假设同刻发布。
- PIT：无 release/vintage 补充前 `pit_eligible=false`。

### S33 — `inflation_expectations`

- Bronze：date + 5y/10y breakeven、5y5y forward、模型 1/5/10/30y。
- Silver：市场 series 与模型 series 分开。
- QA：单位、期限、缺失、系列起点和不同生成时点；不混合市场与模型 availability。
- PIT：每类 release rule 单独审批。

### S34 — `labor_market`

- Bronze：date + unemployment、participation、hourly earnings、job openings。
- Silver：每个指标独立 series。
- QA：频率/单位、自然缺失、序列诞生时间、值域和修订；不向序列开始前回填。
- PIT：没有 release/vintage 历史时只允许展示或非 PIT 实验。

## 14. 每个数据集的统一验收包

每次 preview 和 full build 都必须向用户展示：

1. Bronze 实际样例外壳与 5–20 条有界样例，不泄露 API Key；
2. 输入字段 → Silver 字段映射表；
3. 输出 Parquet schema、主键、分区和排序；
4. 输入、接受、精确重复、版本保留、quarantine、未映射行的 row funnel；
5. null、duplicate、domain、日期、identity、availability 和 referential-integrity QA；
6. 正常与异常样例的前后对比；
7. 输出文件、manifest、SHA-256、代码 commit 和 transform/schema version；
8. 实际运行时间、压缩比、输出体积、临时峰值和全量外推；
9. Data Health 将展示的指标和 bounded examples；
10. 明确列出哪些列 `research_eligible=true/false` 及原因。

Critical 问题（checksum、manifest、主键、未来数据、双重复权）立即停止；High 问题进入
quarantine 并等待人工 review；Medium/Low 保留标志，不能从分母中静默删除。

## 15. 推荐的下一步

S0–S6 已分别通过独立审批并完成；S7 combined-source profile 与五份 revised schema proposal 已完成。
当前下一步只建议：

1. 用户逐字批准五份 S7 Contract ID 与 candidate file SHA-256；
2. 获批后只实现 source readers、bounded bounce detector/candidate manifest、optional external-evidence
   capture contract、adjudication control lifecycle、resolution engine 与固定小样本测试，然后停在
   code-ready；
3. 先显式 review/approve adjudication registry，再让四张派生表消费 exact registry release；
4. 后续严格按 bounded preview → review → 单独批准 full/publish 推进，不沿用 S6 的连续完成授权。

当 S4–S15（身份、公司行动、三套行情、复权和收益）完成并发布后，price-derived Barra 和
普通日频因子即可开始；S16–S34 可以继续逐项扩充，不应阻塞第一批价格型因子。

## 16. 相关证据和字段字典

- [Bronze 全面审计](bronze-audit-2026-07-12.md)
- [完整 Bronze 字段字典](../DATA_README.md)
- [数据处理护栏与时点规则](massive-data-processing-guardrails.md)
- [Massive research catalog](massive-research-catalog.md)
- [Downloader 与存储说明](massive-downloader.md)
- [S4 Assets schema 与发布证据](silver-s4-assets-schema-review.md)
- [S5 Ticker Events schema 与发布证据](silver-s5-ticker-events-schema-review.md)
- [S6 Ticker Overview schema 与发布证据](silver-s6-ticker-overview-schema-review.md)
