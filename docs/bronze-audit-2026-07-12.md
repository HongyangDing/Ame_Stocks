# Ame_Stocks Bronze 全面数据审计（2026-07-12）

## 结论

本次审计冻结的市场数据窗口为 **2016-07-11 至 2026-07-09**。结论不是“不同产品的
每个数值都完全相同”，而是：

- 已完成的全量物理审计对每个已登记 artifact 重新计算 SHA-256、完整解压 gzip/校验 CRC，
  并重新解析 JSON/CSV、核对行数与 manifest。旧目录 232,519 个文件全部通过，新增的
  Daily Market Summary 与 legacy financials 也分别完成逐页校验；没有证据表明本地文件
  损坏、截断、错路径或成功响应漏页。新增数据纳入统一目录后的 Bronze checkpoint v6
  （内部 `report_schema_version=3`）已证明物理完整性与正式计划覆盖；它同时暴露出审计合同
  错把官方 optional 的 `vw` 当成必填。代码已修正，最终统一结论以重跑的 checkpoint v7
  为准。
- Flat Day、Flat Minute 和 REST Daily 是 Massive 的不同产品，不能期待逐字段相等。独立
  REST Daily ↔ Flat Day 全量对账发现价格差异率很低，但 volume/transactions 约 35% 不同；
  独立 Flat Day ↔ Minute 对账则因 RTH/全 session 与 condition update rules 的口径差异更大。
  这些都作为产品差异保留，不能误报成 bit rot。
- 唯一让两份市场对账报告的 source-integrity gate 失败的是 2019-08-12 Flat Day 的 29 行
  时间戳异常。对同一 S3 对象重新下载得到相同 SHA-256，已确认是 provider 文件中的稳定
  行级异常；Silver 必须隔离，不能改写 Bronze。
- 本轮补齐了两个原目录没有的数据集：2,511 个交易 session 的 REST Daily Market Summary，
  以及可访问的 legacy combined financials。前者提供全日 `vw`；后者补到了 377,576 条年报、季报和 TTM
  财务记录。当前正式研究目录由 **29 个 REST 数据集 + 2 个 Flat File 数据集**组成；在本次
  已逐项评估并版本化的 Massive research catalog 范围内，除明确排除的逐笔 Trades/Quotes 外，
  没有发现另一个当前 Key 可访问、对日频因子或本阶段 Barra-style 研究有用、但尚未下载的
  小型或中型数据集。这是有明确 catalog 边界的完备性结论，不代表穷举 Massive 的所有产品。
- 日频价格、成交量、公司行动、可交易 universe 与 price-derived Barra 风格因子具备 Bronze
  输入。legacy financials 使 value/profitability/growth/leverage 等基本面风格可以开始实验，
  但还不能把平台描述成“完整 classic Barra”：历史 point-in-time shares/market cap proxy
  尚未定稿，安全 SIC 不是完整 PIT 行业史，且新版三表/ratios endpoint 对当前 Key 仍为 403。

因此可以进入 Silver 的清洗、确定性去重、时点控制、复权和特征构造；必须同时保留本报告
列出的 quarantine 与 coverage flag，不能为了让 gate 变绿而静默删除 provider differences。

## 审计边界与证据层级

| 范围 | 冻结窗口 / 数量 | 本次检查 | 当前结论 |
| --- | --- | --- | --- |
| Flat Minute + Flat Day | 2016-07-11 至 2026-07-09；各 2,513 个 session | 全文件 hash、gzip CRC、CSV schema/行级不变量、跨产品对账 | 物理完整；1 个 provider 时间戳异常日 |
| 既有 REST 目录 | 原 27 个正式数据集 | request plan、manifest/page、JSON、字段合同、候选键、SEC lineage | 物理完整；存在可解释或需隔离的内容异常 |
| REST Daily Market Summary | 2016-07-13 至 2026-07-09；2,511 个 session/request/page | 下载终态、逐页 hash/JSON/行数、Flat Day 独立对账 | 完整保存；2016-07-11/12 当前权限为 403 |
| Legacy combined financials | 2009-03-29 至 2026-07-09；18 个年度 request、3,784 pages | 全量结构/数值/provenance、候选键、EDGAR accession/date/CIK | 完整保存；39 行存在 PIT 日期危险，另有小量 coverage 异常 |
| 统一 Bronze 目录 | 29 REST + 2 Flat File | checkpoint v6 全量诊断；修正 optional `vw` 后重跑 v7 | v6 物理/计划通过；**待最终 v7 回填** |
| 代码 | 下载器、4 套审计器、计划与字段合同 | Ruff + pytest；时间边界、故障注入、缓存失效、进程回收 | **241 项测试通过** |

所有数据检查均为只读。下表列有路径的机器可复核报告写到数据盘的 `manifests/audits/`；
legacy financials 的两次补充深扫统计由本文汇总，在统一 REST v6 报告完成前不把它们称为一份
独立 durable JSON 报告。没有重写 Bronze、删除旧文件、修改旧 Docker Volume 或触碰
Mogikabu。API Key 不进入 Git、报告或日志。

## 机器可复核报告索引

| 审计 | 报告 | SHA-256 | 状态 |
| --- | --- | --- | --- |
| Bronze 全目录 checkpoint v6（诊断；report schema v3） | `/mnt/HC_Volume_106309665/american_stocks/manifests/audits/bronze/full-2026-07-12-v6.json` | `2288daa04de1ef97af832dbca86909eca26dc9a00037ac4b5fecc9a0bdf626f0` | `failed`：物理/计划通过；含 optional `vw` 合同假阳性 |
| Bronze 全目录 checkpoint v7（最终；report schema v3） | `/mnt/HC_Volume_106309665/american_stocks/manifests/audits/bronze/full-2026-07-12-v7.json` | `PENDING_BRONZE_V7_SHA256` | `PENDING_BRONZE_V7_STATUS` |
| REST semantic schema v6（29 个 REST 中的 26 个） | `/mnt/HC_Volume_106309665/american_stocks/manifests/audits/rest_semantics/full-2026-07-12-v6.json` | `PENDING_REST_V6_SHA256` | `PENDING_REST_V6_STATUS` |
| REST Daily ↔ Flat Day schema v4 | `/mnt/HC_Volume_106309665/american_stocks/manifests/audits/daily_product_crosscheck/full-2026-07-12-v4.json` | `f0588ca0b1ac54dcd2d4883c010725cafe723d0931977200f5c8b0486d34c7fe` | `failed`：仅 1 个 source-integrity 异常日；coverage/numerical 为 `different` |
| Flat Day ↔ Minute schema v5 | `/mnt/HC_Volume_106309665/american_stocks/manifests/audits/market_crosscheck/full-2026-07-12-v5.json` | `d5a2e03a2c04f9f3fc4157b5499ed14c4f7ed61ca9ad65662b0918613243009d` | `failed`：同一个 2019-08-12 provider 异常；两项 reconciliation 为 `different` |
| Assets 重复版本 | `/mnt/HC_Volume_106309665/american_stocks/manifests/audits/assets/duplicate-versions-2026-07-12.json` | `bf5abe8e8bde1671b69c2d1e0546212fa5b99189e660cf2cef8f0936000d3641` | 4,853 组可确定处理的 inactive 版本行 |

这里的 `failed` 不等于“文件损坏”。审计器故意将 provider 行级合同异常设为 hard gate；另把
跨产品正常但不相等的结果标成 `different`，避免把 reconciliation difference 混进 corruption
计数。

## Bronze 物理完整性与计划覆盖

在新增 Daily/legacy 数据纳入冻结清单前启动的 checkpoint v5（report schema v3）为：

- 报告：`/mnt/HC_Volume_106309665/american_stocks/manifests/audits/bronze/full-2026-07-12-v5.json`
- SHA-256：`6a79945c5abbd80ec445f0f94d5e70654a5fbe44feb5fc75265c19e178ad1bad`
- 文件大小 / 耗时：48,167 bytes / 5,778.371 秒
- 覆盖 29 个当时的数据集、56,242 manifests、232,519 个实际文件、205,944,660 条 REST
  记录和 59,817,850,320 bytes 压缩数据。
- `authoritative_plan=passed`、`physical_integrity=passed`、`semantic_consistency=failed`。
  408 个 hard issue instances 为 Assets 214、Ticker Events 193、Float 1；3,942 个 warning
  主要是正式+pilot Ticker Events 404、152 条 13-F filing-only，以及审计快照之后并发出现的
  新 artifact 提示。

v5 在扫描开始时冻结 manifest 清单，后来下载的 Daily/legacy 6,295 个 artifacts 因而只能被
当作“审计快照外新增”提示，不能把它视为新目录的最终证明。checkpoint v6 已从当前 31 个正式
数据集重新构建权威计划，并对所有新旧文件统一执行 hash、解压、解析和记录数核对。

### Bronze checkpoint v6 诊断结果与 v7 最终回填点

v6 报告为 72,124 bytes，耗时 6,990.179 秒，覆盖 31 个数据集、58,771 manifests、238,814
个 artifacts、230,783,074 条 REST 记录和 60,920,727,431 bytes 压缩数据；声明行数与重新
解析行数完全一致。`authoritative_plan=passed`、`physical_integrity=passed`，因此下载计划、
manifest/page、SHA-256、gzip CRC/EOF、解析和行数层面没有失败。

`semantic_consistency=failed` 中有 1,920 个 Daily 页面同时报 `required_fields_missing` 与
`invalid_daily_bar_value`。逐行对照和官方响应 schema 证明原因是这些页面含缺少 optional
`vw` 的合法行；独立 Daily-product 审计已把 24,317,162 行 `vw` present 与 143,676 行
missing 分开记录，且没有 REST source-integrity failure。统一合同现已移除 `vw` 的必填要求，
同时继续对 present 的 `vw`、`n`、`otc` 做类型/数值检查。v6 的其他 hard issue 是 Assets
214 个 session、Ticker Events 193 个页面、Float 1 个页面，以及 38 个包含 39 条 PIT 日期
危险行的 legacy 页面，均为已知内容 QA。

`PENDING`：修正后的 checkpoint v7 完成后回填 SHA-256、状态和 gate；v6 保留为审计器发现并
修复自身合同错误的诊断证据，不能冒充最终语义结论。

### 新增数据的独立下载完整性

| 数据集 | 请求 / pages | 记录 | 压缩体积 | 下载失败 | 独立检查 |
| --- | ---: | ---: | ---: | ---: | --- |
| Daily Market Summary | 2,511 / 2,511 | 24,460,838 | 709,824,165 bytes | 0 | 每 request 单页；manifest、hash、gzip、JSON 与 record count 一致 |
| Legacy financials | 18 / 3,784 | 377,576 | 393,052,946 bytes | 0 | 全页可解压/解析；18,124,688 个 metric objects 通过深层合同 |

Daily 从最老可访问日 2016-07-13 顺序下载到 2026-07-09。2016-07-11/12 的安全 probe 均返回
HTTP 403，因此 REST/Flat 独立对账从 2016-07-13 开始；Flat Day 与 Flat Minute 完整覆盖冻结
窗口最初的这两个 session，所以缺口只存在于 REST Daily 产品，不会造成主行情 bars 的历史
空洞。Legacy financials 按年度、单并发从 2009-03-29 下载到冻结日。

## REST Daily 与 Flat Day 独立对账

最终 schema v4 报告共 37,033,258 bytes，覆盖 2,511 个 session。审计器把两个来源分别从
各自 manifest 重新载入，以 `(session_date, ticker)` 对齐；缓存绑定 manifest 与 artifact 的
SHA-256，任何源变化都会失效。结果如下：

| 指标 | 结果 |
| --- | ---: |
| Flat Day rows | 24,452,546 |
| REST Daily rows | 24,460,838 |
| 两边共同 ticker-session | 24,452,482 |
| REST-only / Flat-only | 8,356 / 64 |
| REST `vw` present / missing | 24,317,162 / 143,676 |
| `passed` / `passed_with_differences` / `failed` sessions | 186 / 2,324 / 1 |
| 存在 ticker coverage difference 的 sessions | 139 |
| 存在 numerical difference 的 sessions | 2,252 |
| 并发上限 / 峰值 in-flight | 2 workers / 4 tasks |
| `max_tasks_per_child` / 实际观测 worker processes | 4 / 628 |

| 字段 | 可比较 ticker-session | Mismatch | 比率 |
| --- | ---: | ---: | ---: |
| Open | 24,452,482 | 705 | 0.002883% |
| High | 24,452,482 | 1,466 | 0.005995% |
| Low | 24,452,482 | 15,872 | 0.064910% |
| Close | 24,452,482 | 4,784 | 0.019564% |
| Volume | 24,452,482 | 8,643,243 | 35.347099% |
| Transactions | 24,315,241 | 8,640,546 | 35.535515% |

三道 gate 为 `source_integrity=failed`、`ticker_coverage=different`、
`numerical_reconciliation=different`。source failure 只来自 2019-08-12 Flat Day 的 29 行
非规范 timestamp；没有 REST Daily source-integrity failure。数值和覆盖差异是两个独立
Massive 产品的 reconciliation 结果，不是下载损坏。

REST Daily 的 `t` 是 provider 日聚合 window end。当前十年样本在普通交易日和半日市都使用
名义 **16:00 America/New_York**，不是 ET 午夜，也不是交易所实际半日市 13:00 close。审计器
早期 schema v2 错把它要求为 exchange session close，因此半日市判断无效；schema v3 修正为
名义 16:00，但仍有一条逐行 issue 证据路径没有严格限长；schema v4 将所有逐行 issue 都改为
有界计数和示例，最终报告只认 v4。旧 v1/v2 cache 与 v3 报告均不作为证据，也不会被 v4 复用。

`vw` 是**全日聚合 VWAP**。它可用作日级价格 QA 或明确命名的全日执行代理，不能冒充策略规则
中的次日 09:30–10:00 VWAP。当前 Minute Aggregate 没有逐分钟 `vw` 或逐笔成交价，只能按
版本化 condition/RTH 口径构造明确命名的 minute-price × volume proxy，不能称为精确 VWAP；
精确的 provider VWAP 需以后只对目标 universe 调用带 `vw` 的 Custom Bars，或引入逐笔 Trades。

最初的全量日频对账实现曾因线程并发同时驻留多个大表，在 8 GB、无 swap 的远程机上触发
OOM。修复后改为 `spawn` 进程池、2 workers、最多 4 个 in-flight task、每进程 4 个 session
后回收，最终 v4 全量稳定完成。OOM 没有改写 Bronze；Mogikabu、Caddy、PostgreSQL、Redis、
worker 和 frontend 随后均确认仍在运行。旧 cache 被保留作审计历史，但因 schema 版本隔离
不会污染最终结果。

## Flat Day 与 Minute Aggregate 对账

稳定的 schema v5 报告解析了 3,689,316,811 行分钟线和 24,468,470 行日线。2019-08-12
Flat Day 的 29 行使用下一美东自然日午夜 `window_start`；源文件和 manifest hash 一致，从
Massive S3 独立重下同一对象又得到相同 SHA-256
`a9e2a03ffdcdefd37aacce082cd6ba97a1143a3ad0519830f3fdec60d7409b0e`。这是 provider 当前
对象中的异常，不是本地 bit rot。

2,513 天累计有 23,842,420 个 ticker-session 同时出现在两套产品中；day-only 626,050、
minute-only 16,579，另有 4,893 个日线 ticker-session 没有 RTH 分钟。单日最大跨产品缺口率
5.99123%，最大无 RTH 比率 0.52029%，都出现在半日市。v5 用 10% 灾难性覆盖失败阈值，并把
低于阈值的全部差异保留为 QA。

| 字段 | 可比较 ticker-session | Mismatch | 比率 |
| --- | ---: | ---: | ---: |
| Open | 23,837,527 | 2,315 | 0.009712% |
| High | 23,837,527 | 759,832 | 3.187545% |
| Low | 23,837,527 | 712,938 | 2.990822% |
| Close | 23,837,527 | 13,359,660 | 56.044656% |
| Volume | 23,842,420 | 23,085,932 | 96.827134% |
| Transactions | 23,842,420 | 23,085,880 | 96.826916% |

该报告将 open/high/low/close 与 RTH 分钟聚合比较，将 volume/transactions 与同 session date
的全部分钟比较。Massive 的
[Condition Codes](https://massive.com/docs/rest/stocks/market-operations/condition-codes/) 为不同交易
条件定义了不同的 OHLC/volume update rules，所以简单 `groupby` 本来就不保证和 provider 日线
完全相等。Silver 必须明确并版本化自己的可交易/RTH 聚合规则，同时保留 Flat Day 与 REST
Daily 作为两套独立 QA 基准。

## Legacy financials 深层检查

当前 Key 对新版 Income Statements、Balance Sheets、Cash Flow Statements、Ratios 的最小
probe 均返回 HTTP 403。为不遗漏本月权限中实际可访问的研究输入，本轮把旧 combined
financials endpoint 单独登记为 `legacy_financials`；它不是把最新值回填到历史，而是按
`filing_date` 顺序保存 provider 返回的原始报表、时点字段和每个 metric 的 lineage。

全量两次独立扫描的结果一致：

- 18 manifests、3,784 pages、377,576 rows、18,124,688 metric objects；JSON parse error、
  duplicate JSON key、page/manifest record-count mismatch、重复 request ID/continuation、重复
  整页和精确重复整行均为 0。
- 377,576 条强候选键
  `(accession, cik, start_date, end_date, filing_date, timeframe, fiscal_period, fiscal_year)`
  全部唯一，冲突为 0。不要拿 accession 前缀当 issuer CIK：255,259 行两者不同，属于 SEC
  filing identity 语义，不是错误。
- 财务 section 只出现 `balance_sheet`、`cash_flow_statement`、`comprehensive_income`、
  `income_statement`，均为非空对象。18,124,688 个 value 全部为有限 int/float，`order` 全部
  为 int，`unit/label/source` 全部为字符串。
- `direct_report` 9,325,293 个 metric 全都有非空 `xpath`；`intra_report_impute` 7,258,958 个
  全都有非空 `formula`；`inter_report_derive` 1,540,437 个全都有非空且去重的
  `derived_from` accession 列表。三种 provenance 的字段集合完全符合各自合同。
- fiscal 组合为 annual/FY 91,662、quarterly/Q1–Q4 284,009、ttm/TTM 1,905，合计与总行数
  一致。299,200 行没有 `acceptance_datetime`；78,376 行有合法 UTC `Z` 时间。与
  `filing_date` 比较时先把该时间转换为 `America/New_York` 日历日期；转换后下表的
  4,245/18 条早于/晚于差异仍然存在，不能归因于 UTC 跨日。

### Legacy financials 与 EDGAR 对账

`filing_date >= 2016-07-11` 的 236,249 行对应 234,825 个不同 accession。EDGAR 对账结果：

| 检查 | 结果 |
| --- | ---: |
| EDGAR 中存在 | 235,757 rows / 234,345 accessions |
| EDGAR 中缺少 | 492 rows / 480 accessions |
| EDGAR 存在时 root CIK 不匹配 | 0 |
| Filing date 不匹配 | 2 rows / 2 accessions |

两条 date mismatch 都是 legacy `2026-03-31` 对 EDGAR `2026-03-30`：
`0001477932-26-001755` 与 `0001193125-26-132193`。EDGAR 缺少的 480 个 accession 记为来源
coverage difference，不等于文件损坏；Silver 必须保留缺失 flag，不能假造匹配。

### Legacy financials 必须隔离或降级的内容

| 异常 | 数量 | Silver 规则 |
| --- | ---: | --- |
| `end_date > filing_date` | 39 rows | PIT hard quarantine；绝不能在该 filing date 入场 |
| 同一 row 的 ticker list 含重复元素 | 8 rows / 15 excess items | 保序去重并保留原始 list 与 QA flag |
| SIC 为空字符串 | 2,322 rows | 视为 coverage null，不据此做行业中性化 |
| `tickers=null` | 125,812 rows | 用 CIK/accession 做 filing identity，不猜 ticker |
| acceptance（先换算 ET 日期）早于/晚于 filing date | 4,245 / 18 rows | 交叉核验；缺 acceptance 时使用更保守的 filing-date lag |
| EDGAR filing-date mismatch | 2 rows | 隔离并以双来源差异进入人工 QA |

这些异常都属于内容/时点 QA；原始字节、解析、metric provenance 和候选键完整性仍然通过。

## REST 语义与已知内容异常

新增数据纳入语义审计前已完成的历史 schema v3 checkpoint 是
`/mnt/HC_Volume_106309665/american_stocks/manifests/audits/rest_semantics/full-2026-07-12-v3.json`，
SHA-256 为 `35bca7148216c76efe47a4dbd4e59d0a96d89321003cc3dfef8127a8ec3d5c75`。它覆盖
173 个权威 manifests、109,816 pages、133,109,323 条进入 endpoint-specific 规则的记录；
13-F 因超过一亿行只做 accession/date/字段合同，不做全量整行 hash。

代码现在是 schema v6：在旧规则基础上加入 13-F 的同一 EDGAR row 见证；Daily 纳入字段合同
和候选键，legacy financials 另纳入字段合同、候选键及 accession lineage。语义审计有意只
覆盖 29 个正式 REST 数据集中的 26 个；Assets、Ticker Overview 与 Ticker Events 由全量
Bronze 和各自专项规则检查。旧 schema v3 checkpoint 只作为历史证据，最终结论以下方 v6
为准。

<!-- PENDING_REST_V6_START -->
### REST semantic schema v6 最终回填点

`PENDING`：主任务在 schema v6 完成后替换本段，至少填写 26 个语义审计数据集的报告
SHA-256、状态、authoritative manifest/page/row 总量、uniqueness、taxonomy、SEC/legacy
accession coverage，以及 hard issue 和 diagnostic difference 分类。独立 legacy 深扫不能
代替统一 v6 报告，也不能把这里的 26 个数据集误写成全部 29 个 REST 数据集。
<!-- PENDING_REST_V6_END -->

既有目录中已经定位、且无须重下的主要内容差异为：

- Condition Codes 中 code `30` 有 current/legacy 映射；Silver join 必须带 `legacy`、SIP 和
  data type，不能只按整数 ID。
- EDGAR Index 有 22,032 个精确重复 excess rows，以及 6,148 个
  `(accession_number, cik)` 多 metadata 版本。联合申报允许同一 accession 对应多个 CIK。
- Risk Factors 有 16,692 个重复 hash groups、30,449 个精确重复 excess rows，全部发生在同一
  provider page；Silver 可按规范整行 hash 去重并保留 lineage。
- 10-K Sections 有 9,910 个候选键歧义和 8 个精确重复；其中 155 个候选键正文 hash 不同，
  必须保留为 distinct variants，不可按 accession 粗暴覆盖。
- Form 4 与 8-K Text 分别有 4 和 1 个精确重复 excess rows；IPO 的 2 个歧义只差历史
  exchange code 表达。以上均为 provider differences，不是 bit rot。

## Universe、13-F 与单行异常

### Assets inactive 版本行

在 2,513 个 session、5,026 个 active/inactive 请求和 69,381,182 行中，214 个 session
（2025-09-02 至 2026-07-09）出现 4,853 组重复 ticker；每组严格两行，全部来自
`active=false`。active/inactive 集合交集仍为 0。2,117 组只差 `last_updated_utc`，2,736 组
只差 `delisted_utc` 与 `last_updated_utc`，其他身份字段一致。Silver 应按确定优先级选状态行，
同时保存版本数、来源 hash 和冲突 QA；Bronze 不删除旧版本。

### Ticker Events 404 与空 ticker

正式 receipt 共 15,173 个请求：11,471 成功，3,702 个经重试稳定为 HTTP 404；另有 100 个
隔离 pilot，其中 84 个 404。此前报告中的 3,786 是正式与 pilot 合计，不是损坏文件数。
每日 point-in-time membership 仍由 active+inactive Assets 快照承担，因此这些 404 不减少
每日股票池覆盖。

成功响应中另有 193 条 `2023-11-18 ticker_change` 事件的目标 ticker 为空。193 个响应都同时
含 1–3 条合法事件；Silver 只隔离空占位行，保留同响应合法事件和 lineage。

### 13-F filing-only HR/HR-A

全量扫描发现 152 条 header-only HR/HR-A：正式计划 137 条、pilot 15 条。七个 holding 字段
整组 absent，没有 partial、负值、非法整数或非法 share type；正式 accession 全部能由同一
EDGAR row 的 date+CIK+form 见证。它只能证明 filing metadata 有效，不能解释成零持仓。
Silver 应保存 filing header 并设置 `holdings_status=not_public_or_unavailable`，不写入 holdings
fact 表。

### Float 缺 ticker

Float 当前快照只有一行缺 `ticker`：`effective_date=2026-01-29`、`free_float=3,950,100`、
`free_float_percent=20.5`。该行无法安全关联资产，Silver 必须隔离，不能猜 ticker。

## 日频因子与 Barra 输入完备性

| 模块 | Bronze 状态 | 可支持研究 | 限制 / 下一步 |
| --- | --- | --- | --- |
| 行情与成交 | Flat Minute/Day 完整；REST Daily 自 2016-07-13 完整 | 收益、动量、反转、beta、残差波动、流动性、换手 proxy、全日 VWAP QA | 分钟 bars 只能构造明确命名的开盘半小时价格代理；精确 VWAP 需 targeted Custom Bars/Trades；隔离 29 行 provider timestamp 异常 |
| PIT universe | 每日 active+inactive 的 provider-visible `locale=us, market=stocks` 正式窗口完整 | 退市与代码生命周期，控制 survivorship bias | 不是 common-stock-only universe；inactive 版本行确定性去重；不含 OTC |
| 公司行动 | 本次 catalog 正式计划完整 | split、cash dividend、IPO/listing age、复权链 | Silver 按生效日版本化复权并交叉 QA |
| 财务三表 | Legacy 历史已下载；新版 endpoints 403 | value、profitability、growth、leverage、quality 的初版研究 | 39 行 PIT quarantine；接受时间覆盖不全；旧 endpoint 需保留 provenance 与版本标识 |
| Size | 部分就绪 | 价格类与有限 float/statement proxy | 当前 float 非完整历史；仍需可复现的 PIT shares/market-cap proxy |
| Industry | 部分就绪 | 有 SIC 时做有限行业 QA/中性化 | Ticker Overview SIC 覆盖全部 lifecycle 16,682/30,739；身份匹配普通股 10,620/13,200；不是完整 PIT GICS |
| SEC/持仓/文本 | 本次 catalog 正式计划完整 | EDGAR、Form 3/4、13-F、10-K、8-K、risk、news | 8-K Disclosures 仅自 2022 有返回；必须按 filing/published time 入场并处理修订、版本、header-only 与重复 |
| 做空 | 本次 catalog 中 provider 可访问历史已下载 | short interest、short volume | short volume 仅自 2024；不能伪造更早覆盖 |
| 宏观 | 本次 catalog 正式计划完整 | 利率、通胀、预期与劳动力 regime controls | Silver 必须使用真实发布日期 lag |

Massive 官方文档显示 Stocks Advanced 的
[Income Statements](https://massive.com/docs/rest/stocks/fundamentals/income-statements)、
[Balance Sheets](https://massive.com/docs/rest/stocks/fundamentals/balance-sheets) 和
[Cash Flow Statements](https://massive.com/docs/rest/stocks/fundamentals/cash-flow-statements) 应支持
历史，但当前远程 Key 的安全 probe 均为 HTTP 403；
[Ratios](https://massive.com/docs/rest/stocks/fundamentals/ratios) 同样为 403，且该产品本身是最新截面，
不能代替 PIT 历史重算。legacy endpoint 已最大化利用当前可访问历史，但不会消除 PIT shares、
行业分类与新版产品 entitlement 的缺口。

本次 research catalog 明确排除的超大数据为逐笔 Trades 与 Quotes。十年尺度为多 TB，日频因子与本阶段
Barra-style 模型不依赖逐笔成交/报价。SMA/EMA/MACD/RSI 等指标可从 immutable bars 重算，
不应重复下载 provider 派生副本。Condition Codes 先前遗漏的 94 行已经补齐。

每日 universe 请求显式使用 `locale=us, market=stocks`。本报告证明的是 Massive 在该参数
组合下返回的 provider-visible securities universe，不等于 common-stock-only universe；
Massive 又将 `stocks` 与 `otc` 作为不同 market，因此这里也不声称 OTC 已覆盖。未来若纳入
OTC，应建立独立 active/inactive 日快照和单独审计，不可悄悄混入当前冻结 universe。

## Silver 前的强制处理规则

1. Bronze 保持不可变；所有去重、quarantine、复权和 identity stitching 只生成带 lineage 的
   Silver 产物。
2. 隔离 2019-08-12 Flat Day 的 29 行异常时间戳、39 条 `end_date > filing_date` 财务行、
   2 条 EDGAR filing-date mismatch 和 1 条 Float 缺 ticker。
3. 对 13-F filing-only、Ticker Events 404、legacy tickers/SIC/acceptance 缺失设置明确状态，
   不把 missing 解释成 0，也不猜 ticker/行业。
4. Flat Day、REST Daily 和自分钟聚合的日线是三个有版本的产品；产品差异进入 QA，不互相
   静默覆盖。
5. 因子时点使用 provider filing/published/acceptance 字段中可证明且保守的时间；财务数据
   在完成 PIT quarantine 之前不得进入回测。

## 可复现命令与代码入口

```bash
cd /opt/american_stocks

.venv/bin/ame-audit-daily-products \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-13 --end 2026-07-09 \
  --workers 2 --max-tasks-per-child 4 \
  --output manifests/audits/daily_product_crosscheck/full-2026-07-12-v4.json

.venv/bin/ame-audit-market \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --workers 1 \
  --output manifests/audits/market_crosscheck/full-2026-07-12-v5.json

.venv/bin/ame-audit-bronze \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --mode full --workers 8 \
  --output manifests/audits/bronze/full-2026-07-12-v7.json

.venv/bin/ame-audit-rest-semantics \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --output manifests/audits/rest_semantics/full-2026-07-12-v6.json
```

代码入口：

- `backend/ame_stocks_api/audit/bronze.py`
- `backend/ame_stocks_api/audit/market.py`
- `backend/ame_stocks_api/audit/daily_products.py`
- `backend/ame_stocks_api/audit/rest_semantics.py`
- `backend/ame_stocks_api/audit/row_contracts.py`
- `backend/ame_stocks_api/cli/audit.py`
- `backend/ame_stocks_api/cli/market_audit.py`
- `backend/ame_stocks_api/cli/daily_products_audit.py`
- `backend/ame_stocks_api/cli/rest_semantics_audit.py`

本报告是有边界的 2026-07-12 快照，不自动代表以后新增交易日。Flat Files 已出现
2026-07-10，它属于下一次增量下载，不是冻结窗口内漏文件。数据盘中约 2.3 GiB 的旧中断 REST
scratch 也暂时保留，但不计入正式 Bronze inventory 或上表机器报告的 durable evidence；不会
通过删除历史或 Bronze 换空间。
