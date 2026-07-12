# Ame_Stocks Bronze 全面数据审计（2026-07-12）

## 结论

审计冻结窗口为 **2016-07-11 至 2026-07-09**。结论不是“所有表彼此数值完全相等”，而是：

- Bronze 的全部已保存物理文件和 manifest 可复现；正式请求均有明确终态（包括 3,702 个已
  重试确认的 Ticker Events 404）。全量文件重新计算 SHA-256、完整解压并校验 gzip CRC 后，
  没有发现损坏、截断、成功响应漏页、错路径或记录数 mismatch。
- 分钟线和日线各有 2,513 个正式交易日文件。两种 Massive 产品的 OHLCV 存在显著口径
  差异，condition update rules 说明简单 `groupby` 不保证相等，但不能逐项证明全部 mismatch
  的原因。除已定位的 29 行上游日线时间戳异常外，两套数据分别通过结构和数值不变量检查。
- REST 表之间没有发现断裂的 SEC accession、filing date mismatch 或无法解码的 taxonomy。
  重复/候选版本表现为 provider 返回的精确重复或多版本，其中少量 10-K 正文差异的业务原因
  尚未完全判定；Float 另有 1 行缺 ticker，13-F 有 152 条只有 filing metadata 的 HR/HR-A
  记录。这些内容异常必须在 Silver 确定性去重、保留版本或隔离，但不修改不可变 Bronze。
- 当前远程 Key 实际可访问且列入本项目正式目录的 27 个 REST 数据集和 2 个 Flat File 数据集
  均已保存。本轮补齐了遗漏的 94 行 Condition Codes。没有发现另一个“当前可访问、研究需要、
  但尚未下载”的小型/中型数据集；三张财务报表和 Ratios 是已识别的 403 access 阻塞项。
- 日频价格/成交量类因子以及 price-derived Barra 风格因子已经具备 Bronze 输入。完整 classic
  Barra 尚不能声称就绪：三张历史财务报表与当前 ratios endpoint 对远程 Key 仍返回 HTTP 403；
  历史市值需要明确 shares proxy；安全 Ticker Overview 中 SIC 覆盖 16,682 / 30,739 个全部
  身份生命周期；在身份匹配的普通股子集为 10,620 / 13,200。两者都不能伪装成完整
  point-in-time 行业分类。

因此，可以进入 Silver 的清洗、去重、时点控制和复权设计；在财务 endpoint 权限恢复并回填
前，不能把平台描述成“完整 Barra 基本面模型”。

## 审计范围与方法

| 检查层 | 范围 | 方法 | 结果 |
| --- | --- | --- | --- |
| 下载计划 | 27 REST + 2 Flat File 数据集 | 从当前代码重建规范请求 ID，与 manifest/receipt 一一比对；额外 pilot 单独标记 | required request 均有终态；Ticker Events 另有 3,702 个稳定 404 |
| 物理完整性 | 56,242 manifests、232,519 artifacts | 每个文件重新计算 SHA-256；gzip 全量读取/CRC；JSON/CSV 解析；压缩前后字节数及行数核对 | 通过 |
| 分页与覆盖 | 全部 REST pages | 页号连续、last/continuation、manifest 状态、请求边界及逐页日期边界 | 通过 |
| Flat File | 2 × 2,513 sessions | header、类型、UTC 分钟边界、OHLC 不变量、唯一 `(ticker, window_start)`、manifest-bound cache | 文件与键完整；发现 29 行上游日线时间戳异常 |
| Universe | 每日 active + inactive | 两次请求身份、flag、交集、ticker 唯一性及重复版本字段级比较 | 发现可处理的上游版本行 |
| REST 语义 | 173 个权威 manifests、109,816 pages、133,109,323 行 | 候选键、整行 hash、taxonomy path、SEC accession/date，用临时 SQLite 有界聚合；13-F 不做全行 hash | 1 行 Float 缺键；有 provider differences |
| 代码 | 下载器、三套审计器及计划构造 | Ruff、210 项 pytest、边界/故障注入和多轮独立对抗复核 | 通过；13-F/EDGAR 强化规则将在 v5/v4 全量复跑 |

全量校验是只读操作。审计报告写入数据盘的 `manifests/audits/`；没有重写 Bronze、删除旧
文件或触碰 Mogikabu。

## 库存与物理完整性

完成的 v4 全量报告：

<!-- FULL_V4_START -->
- 报告：`/mnt/HC_Volume_106309665/american_stocks/manifests/audits/bronze/full-2026-07-12-v4.json`
- 报告 SHA-256：`10590cceba73891fecc0228fd010d3278419bed3cb11088d6091e989b3b8bbc4`
- 文件大小 / 耗时：53,197 bytes / 4,626.587 秒
- 状态：`failed`；`authoritative_plan=passed`、`physical_integrity=passed`、
  `semantic_consistency=failed`
- Gate issue instances：Assets 重复 ticker 214、Ticker Events 合约 193、Float 缺字段 1，另有
  13-F required/invalid 各 148 个 page-level 误报；3,786 个 404 和 3 类额外 pilot 为 warning。
<!-- FULL_V4_END -->

v4 首次把 47,306 条 13F-NT/NT-A 误报消除后，仍把 148 个 page 中的 header-only HR/HR-A
当成 holding 缺字段。补充全量扫描确认实际为 152 条 header-only 记录：正式计划 137 条、
pilot 15 条；所有 holding 字段均整组缺失，没有 partial 或真实数值/domain 错误。代码已改成
将这种互斥形态报告为 warning；只有 partial holding 或真实非法数值才失败。修正后的 v5
全量报告将在下一次复跑后替换本段状态，v4 保留为不可变审计历史。

已完成运行的稳定总量为：

| 指标 | 数值 |
| --- | ---: |
| 数据集 | 29 |
| Manifests | 56,242 |
| Artifacts / 实际验证文件 | 232,519 / 232,519 |
| REST 声明记录 / 重新解析记录 | 205,944,660 / 205,944,660 |
| Flat minute rows | 3,689,316,811 |
| Flat day rows | 24,468,470 |
| Manifest 记录压缩体积 | 59,817,850,320 bytes（约 55.71 GiB） |
| 损坏、截断、hash/bytes/row mismatch | 0 |
| Required plan 缺失 / 意外 in-progress manifest | 0 / 0 |
| 已重试并确认的 Ticker Events endpoint 404 | 3,702 个正式 request receipts |
| orphan、partial、quarantine 文件 | 0 |

这里的“验证文件”不是只核对 manifest 中已经保存的 hash，而是从磁盘重新读取内容并计算；
gzip 只有完整读到 EOF 才能通过 CRC。REST 同时重新解析 JSON 并重算每页记录数，Flat File
同时解析 CSV header 与全部数据行。

## 市场数据交叉检查

Massive 的 Day Aggregate 与 Minute Aggregate 是两个独立产品。审计按以下口径重算：

- open/high/low/close：美东常规交易时段（含交易所半日市）分钟线；
- volume/transactions：同一美东 session date 的全部分钟记录；
- 对所有分钟记录检查 UTC 分钟对齐、有限非负值、`low <= open/close <= high`；
- 对两套文件检查 ticker/时间键唯一性、ticker 缺口和“只有盘前盘后分钟、却存在日线”的情况；
- 缓存同时绑定源 manifest SHA-256 与重新读取文件得到的 SHA-256，不能用相同 size/mtime
  掩盖 bit rot。

<!-- MARKET_FINAL_START -->
最终 schema v5 报告：

```text
/mnt/HC_Volume_106309665/american_stocks/manifests/audits/market_crosscheck/
└── full-2026-07-12-v5.json
```

报告 SHA-256：`d5a2e03a2c04f9f3fc4157b5499ed14c4f7ed61ca9ad65662b0918613243009d`。

全量运行解析了 3,689,316,811 行分钟线和 24,468,470 行日线。它发现
2019-08-12 的日线中有 29 行使用下一美东自然日午夜的 `window_start`；这些行可解析且源文件
SHA 与 manifest 一致。随后从 Massive S3 独立重新下载同一对象，得到完全相同的
SHA-256 `a9e2a03ffdcdefd37aacce082cd6ba97a1143a3ad0519830f3fdec60d7409b0e`，证明这是
provider 当前文件中的行级语义异常，不是本地 bit rot。Silver 必须隔离这 29 行。

2,513 天累计有 23,842,420 个 ticker-session 同时出现在两套产品中；day-only 626,050，
minute-only 16,579，合计占 union 的 2.62458%。另有 4,893 个日线 ticker-session 没有 RTH
分钟。单日最大跨产品缺口率 5.99123%，最大无 RTH 比率 0.52029%；这两个最大值都出现在
半日市交易日。v5 使用
10% 的灾难性覆盖失败阈值（至少缺 2 个 ticker），并继续把低于阈值的差异完整报告为 QA，
避免把本次半日市观测到的 5%–6% 跨产品覆盖差异误判成文件损坏。

| 字段 | 可比较 ticker-session | Mismatch | 比率 |
| --- | ---: | ---: | ---: |
| Open | 23,837,527 | 2,315 | 0.009712% |
| High | 23,837,527 | 759,832 | 3.187545% |
| Low | 23,837,527 | 712,938 | 2.990822% |
| Close | 23,837,527 | 13,359,660 | 56.044656% |
| Volume | 23,842,420 | 23,085,932 | 96.827134% |
| Transactions | 23,842,420 | 23,085,880 | 96.826916% |

因此 v5 的总体状态为 `failed`：`source_and_row_integrity` 被上述 29 行触发；另外两道 gate
是 `different`，而不是 corruption。逐字段分母只包含两边都存在且可以比较的值。这个结果
明确禁止把 Flat File 日线与分钟线 RTH/同 session 简单聚合结果混为同一口径。
<!-- MARKET_FINAL_END -->

数值 mismatch 属于 cross-product reconciliation difference，不进入物理损坏计数。Massive
[Condition Codes](https://massive.com/docs/rest/stocks/market-operations/condition-codes/) 明确给出
不同交易条件是否更新 open/close、high/low、volume 的规则。因此 Silver 必须选定并版本化
自己的可交易/RTH 聚合口径，并保留原始日线作为独立 QA 基准。

## REST 语义检查

当前已完成的权威子集语义报告：

```text
/mnt/HC_Volume_106309665/american_stocks/manifests/audits/rest_semantics/
└── full-2026-07-12-v3.json
```

报告 SHA-256：`35bca7148216c76efe47a4dbd4e59d0a96d89321003cc3dfef8127a8ec3d5c75`。

代码中的 REST semantic schema 已升级到 v4：除 accession 和 filing date 外，还要求 Form 13-F
的 `(filing_date, filer_cik, form_type)` 能由同一条 EDGAR identity row 见证，避免多 CIK、
多 form 或多 date 行产生 split-witness 假通过。补充全量只读核验得到 0 mismatch；正式 v4
JSON 将在复跑完成后替换本段 v3 证据。

| 检查 | 结果 |
| --- | ---: |
| 正式 manifests / pages / rows | 173 / 109,816 / 133,109,323 |
| 隔离 pilot manifests | 5 |
| 缺候选键 | 1（Float 缺 ticker） |
| Splits 唯一候选键 | 26,337；0 冲突、0 精确重复 |
| Dividends 唯一候选键 | 710,559；0 冲突、0 精确重复 |
| News 唯一候选键 | 807,868；0 冲突、0 精确重复 |
| Short interest / volume 唯一候选键 | 3,781,607 / 8,302,971；0 冲突、0 精确重复 |
| Disclosure taxonomy | 119 个定义；118 个被使用；0 无法解码 |
| Risk taxonomy | 140 个定义；140 个被使用；0 无法解码 |
| 13-F / Form 3 / Form 4 唯一 accession | 329,958 / 93,020 / 1,831,837 |
| 8-K disclosure / 8-K text 唯一 accession | 203,169 / 448,167 |
| 上述 SEC 数据缺失 EDGAR accession / filing date mismatch | 0 / 0 |
| 13-F EDGAR exact date + CIK + form mismatch | 0（补充全量核验） |

这里的 133,109,323 行是纳入 endpoint-specific 语义规则的权威子集，不是物理审计读取的全部
REST 205,944,660 行。13-F 因超过一亿行，语义层验证 accession/date coverage 和 Bronze 字段
合同，不做全量整行 candidate hash；其余列出的 endpoint 按上表规则检查候选键或整行 hash。

有 68,556 个差异诊断，不是文件损坏：

- Condition Codes 中 Massive code `30` 同时存在当前/legacy 映射，展开到 data type 后有 2 个
  候选键歧义；Silver 应保留 `legacy`、SIP 和完整 update rules，而不是只按整数 ID join。
- EDGAR Index 有 22,032 个精确重复 excess rows，以及 6,148 个 `(accession_number, cik)`
  对应多个 metadata 版本。联合申报允许同一 accession 对应多个 CIK；Silver 先按规范整行
  hash 去精确重复，再保留 metadata 版本与来源，不能只按 accession 粗暴去重。
- REST v3 之外的补充逐页扫描显示，Risk Factors 有 16,692 个重复 hash group、30,449 个精确
  重复 excess rows，单组最多重复 9 次；它们全部在同一 manifest、同一 provider page 内重复，
  跨页和跨年度重复均为 0。Silver 可按规范整行 hash 去重。
- 10-K Sections 有 9,910 个候选键歧义和 8 个精确重复。歧义主要是同一 filing 的 URL/CIK
  表达或 share-class ticker 映射；164 个还涉及 `period_end` 差异，155 个候选键的正文 hash
  不同。后者应保留为 distinct variants，待 Silver 再判断修订语义。
- 补充逐行复核显示，IPO 的 2 个候选键歧义发行信息相同，只有历史 exchange code 分别为
  `OTCM/PINX` 和 `XOTC/OTCM`；保留 exchange 冲突即可。
- Form 4 和 8-K Text 分别有 4 和 1 个精确重复 excess rows。完整总账为 52,494 个精确重复
  excess rows，加 16,062 个候选键歧义，合计 68,556。

## Universe 与单行异常

### 13-F header-only HR/HR-A

补充扫描覆盖 42 个 manifests、约 1.04 亿原始行。v4 的 148 个 `required_fields_missing` 和
148 个 `invalid_form_13f_value` issue instances 实际来自同一批 148 个 pages，共 152 条记录：

- 正式计划 137 条、137 个 accession、134 页、23 个季度；132 条 `13F-HR`、5 条
  `13F-HR/A`；
- pilot 15 条、14 页，所有 accession 均重复正式集，不增加新语义；
- 每条只有 accession、file number、filer CIK、filing date/URL、film number、form type 和
  period；7 个必需 holding 字段整组 absent；
- 没有 partial holding、负值、非整数 market value/share amount 或非法 share type；正式
  accession 全部能在 EDGAR 找到，且同一 EDGAR row 的 filing date、CIK、form type 精确匹配。

这只能证明 filing metadata 有效，不能区分零公开持仓、保密省略或 provider 未解析到
information table。Silver 应保留 filing header，设置 `holdings_status=not_public_or_unavailable`，
不把它当成零持仓，也不写入 holdings fact 表。

### Assets 版本行

独立有界报告：

```text
/mnt/HC_Volume_106309665/american_stocks/manifests/audits/assets/
└── duplicate-versions-2026-07-12.json
```

SHA-256：`bf5abe8e8bde1671b69c2d1e0546212fa5b99189e660cf2cef8f0936000d3641`。

在 2,513 个 session、5,026 个 active/inactive 请求和 69,381,182 行中：

- 214 个 session（2025-09-02 至 2026-07-09）出现 4,853 组重复 ticker；每组严格两行，
  合计 4,853 个 excess rows；
- 全部来自 `active=false` 请求；active/inactive 两张表的集合交集仍为 0；
- 2,117 组只差 `last_updated_utc`；2,736 组只差 `delisted_utc` 与
  `last_updated_utc`；其余身份字段全部一致，没有第三类差异字段集。

这是 provider 返回的 inactive-security 版本更新，而非损坏。Bronze 保留原字节；Silver 按
明确优先级确定性选择一个状态行，同时保存版本数量、来源 hash 和冲突 QA。

### Float 缺 ticker 行

Float 当前快照有且只有一行缺少必需的 `ticker`：`effective_date=2026-01-29`、
`free_float=3,950,100`、`free_float_percent=20.5`。它无法安全连接资产身份，Silver 应隔离，
不推测 ticker。其余文件完整性和记录数不受影响。

### Ticker Events 空 ticker 行

正式成功响应中有 193 条统一形态的空 ticker 事件：日期均为 `2023-11-18`、类型均为
`ticker_change`，但 `ticker_change.ticker == ""`。193 个受影响响应全部同时含有 1–3 条合法
事件，没有任何响应只含异常行；100 个 pilot 中也没有这类异常。它是 provider 注入的空值
占位，而非本地损坏。Silver 只隔离这 193 行，保留同响应的合法事件和 lineage，无需重下。

### Ticker Events 404

正式 identifier receipt 有 15,173 行，其中 11,471 个请求成功、3,702 个请求经重试稳定返回
HTTP 404；另有 100 个隔离 pilot（84 个 404）。完整审计看到的 3,786 个 404 是正式与 pilot
合计，不能错误解释为 3,786 个损坏文件。事件 endpoint 是辅助身份 QA；每日 point-in-time
universe membership 仍由 active + inactive Assets 快照承担，所以这些 404 不减少每日股票池
覆盖，但永久身份 stitching 仍需 Silver 利用 FIGI/CIK、合法事件和公司行动继续 QA。

## 日频因子与 Barra 输入完备性

| 模块 | Bronze 状态 | 可支持内容 | 限制/下一步 |
| --- | --- | --- | --- |
| 行情与成交 | 冻结窗口完整 | 收益、动量、反转、beta、残差波动率、流动性、换手 proxy、执行 VWAP | Silver 需定义 RTH/全时段与复权口径 |
| Point-in-time universe | provider-visible membership 完整（正式窗口、exchange-listed stocks） | active + inactive、退市，控制幸存者偏差 | 去重 inactive 版本行；永久身份 stitching 尚需 QA；当前 `market=stocks` 不含 OTC |
| 公司行动 | 正式计划内完整 | splits、cash dividends、IPO/listing age | Silver 按生效日构造复权链并做事件 QA |
| SEC/持仓/文本 | 正式计划内完整 | EDGAR、Form 3/4、13-F、10-K、8-K、risk、news | 8-K disclosures 仅自 2022 有返回；只能按 filing/published time 入场；处理修订与重复 |
| 做空与 float | provider 可用范围完整 | short interest、short volume、当前 float QA | short volume 仅自 2024；float 不是历史序列 |
| 宏观 | 正式计划内完整 | 利率、通胀、预期、劳动力市场 regime controls | Silver 必须加入实际发布日期 lag |
| 基本面三表 | **权限阻塞** | value、profitability、growth、leverage、quality | 代码和年度 `filing_date` 计划已就绪；当前 Key 对三 endpoint 均 403 |
| Ratios | **权限阻塞且非历史** | 仅当前截面 QA | 官方 endpoint 是 latest-only，不能替代 point-in-time 历史重算 |
| 历史 size / industry | **部分就绪** | 可先做价格/成交量风格和有限 SIC 中性化 | 需 shares proxy；SIC 为全部 lifecycle 54.27%、身份匹配普通股 80.45%；无完整 PIT GICS |

Massive 官方文档显示 Stocks Advanced 对
[Income Statements](https://massive.com/docs/rest/stocks/fundamentals/income-statements)、
[Balance Sheets](https://massive.com/docs/rest/stocks/fundamentals/balance-sheets) 和
[Cash Flow Statements](https://massive.com/docs/rest/stocks/fundamentals/cash-flow-statements)
应提供 EOD 全历史，记录回溯到 2009-03-29；但当前远程 Key 的安全单行 probe 均为 HTTP 403，
形成 docs-vs-live access mismatch，原因尚不能仅凭状态码确定。
[Ratios](https://massive.com/docs/rest/stocks/fundamentals/ratios) 明确只计算最近交易日、无历史。
因此正确动作是先由 Massive 核实访问权限，恢复后运行已写好的年度下载计划，而不是回退到
退役 endpoint 或把今天的 ratios/market cap 回填到过去。

明确排除的超大数据只有逐笔 Trades 与 Quotes。它们在十年尺度为多 TB，且日频因子与本阶段
Barra-style 模型不依赖逐笔成交/报价。SMA/EMA/MACD/RSI 等可从 immutable bars 重算，也不应
重复下载 provider 副本。

当前每日 universe 请求显式使用 `locale=us, market=stocks`。Massive 把 `stocks` 与 `otc`
列为不同 market 枚举，所以本审计证明的是美股交易所上市股票 universe 完整，不声称 OTC
证券也已覆盖。OTC 对本阶段 Barra-style 股票池不是遗漏；如果未来决定纳入 OTC，应建立独立
active/inactive 日快照计划与单独审计，不能悄悄混入当前冻结 universe。

## 可复现命令与证据路径

```bash
cd /opt/american_stocks

.venv/bin/ame-audit-bronze \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --mode full --workers 8 \
  --output manifests/audits/bronze/full-2026-07-12-v5.json

.venv/bin/ame-audit-market \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --workers 1 \
  --output manifests/audits/market_crosscheck/full-2026-07-12-v5.json

.venv/bin/ame-audit-rest-semantics \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --output manifests/audits/rest_semantics/full-2026-07-12-v4.json
```

代码入口：

- `backend/ame_stocks_api/audit/bronze.py`
- `backend/ame_stocks_api/audit/market.py`
- `backend/ame_stocks_api/audit/rest_semantics.py`
- `backend/ame_stocks_api/cli/audit.py`
- `backend/ame_stocks_api/cli/market_audit.py`
- `backend/ame_stocks_api/cli/rest_semantics_audit.py`

本报告是一次有边界的 2026-07-12 快照，不会自动代表以后新增的交易日。Massive Flat Files
目前已出现 2026-07-10；它属于下一次增量下载，不是本次冻结窗口内的漏文件。若扩大到
2016-07-11 之前的全历史，也应建立新计划并重新审计，不能混入这份十年快照。
