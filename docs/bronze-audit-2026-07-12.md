# Ame_Stocks Bronze 全面数据审计（2026-07-12）

## 结论

审计冻结窗口为 **2016-07-11 至 2026-07-09**。结论不是“所有表彼此数值完全相等”，而是：

- Bronze 的物理文件、manifest 和正式下载计划完整；全量文件重新计算 SHA-256、完整解压并
  校验 gzip CRC 后，没有发现损坏、截断、漏页、错路径或记录数 mismatch。
- 分钟线和日线各有 2,513 个正式交易日文件。两种 Massive 产品的 OHLCV 存在可解释的
  provider 口径差异；这些差异与 condition update rules 一致，不能把日线简单视为分钟线
  `groupby` 的必然结果。两套数据都通过各自的结构和数值不变量检查。
- REST 表之间没有发现断裂的 SEC accession 或无法解码的 taxonomy。发现的重复均能归因于
  provider 返回的版本行或精确重复，必须在 Silver 确定性处理，但不修改不可变 Bronze。
- 已授权且列入本项目正式目录的 27 个 REST 数据集和 2 个 Flat File 数据集均已保存。
  本轮补齐了遗漏的 94 行 Condition Codes。没有发现另一个“当前有权限、研究需要、但尚未
  下载”的小型/中型数据集。
- 日频价格/成交量类因子以及 price-derived Barra 风格因子已经具备 Bronze 输入。完整 classic
  Barra 尚不能声称就绪：三张历史财务报表与当前 ratios endpoint 对远程 Key 仍返回 HTTP 403；
  历史市值需要明确 shares proxy；安全 Ticker Overview 中 SIC 覆盖 16,682 / 30,739 个身份
  生命周期，不能伪装成完整 point-in-time 行业分类。

因此，可以进入 Silver 的清洗、去重、时点控制和复权设计；在财务 endpoint 权限恢复并回填
前，不能把平台描述成“完整 Barra 基本面模型”。

## 审计范围与方法

| 检查层 | 范围 | 方法 | 结果 |
| --- | --- | --- | --- |
| 下载计划 | 27 REST + 2 Flat File 数据集 | 从当前代码重建规范请求 ID，与 manifest 一一比对；额外 pilot 单独标记 | 正式计划完整 |
| 物理完整性 | 56,242 manifests、232,519 artifacts | 每个文件重新计算 SHA-256；gzip 全量读取/CRC；JSON/CSV 解析；压缩前后字节数及行数核对 | 通过 |
| 分页与覆盖 | 全部 REST pages | 页号连续、last/continuation、manifest 状态、请求边界及逐页日期边界 | 通过 |
| Flat File | 2 × 2,513 sessions | header、类型、UTC 分钟边界、OHLC 不变量、唯一 `(ticker, window_start)`、manifest-bound cache | 通过 |
| Universe | 每日 active + inactive | 两次请求身份、flag、交集、ticker 唯一性及重复版本字段级比较 | 发现可处理的上游版本行 |
| REST 语义 | 82 个正式 manifests、7,697 pages、20,553,455 行 | 候选键、整行 hash、taxonomy path、SEC accession，用临时 SQLite 有界聚合 | 无 corruption；有 provider differences |
| 代码 | 下载器、三套审计器及计划构造 | Ruff、172 项 pytest、边界/故障注入和两轮独立对抗复核 | 通过；最终远程复跑中 |

全量校验是只读操作。审计报告写入数据盘的 `manifests/audits/`；没有重写 Bronze、删除旧
文件或触碰 Mogikabu。

## 库存与物理完整性

最新全量 Bronze 运行报告（修正多页 coverage 聚合后）将在本节完成后固化：

<!-- FULL_V3_START -->
- 最终报告：运行中
- 报告 SHA-256：运行中
- 状态：运行中
<!-- FULL_V3_END -->

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
| 正式计划缺失、failed/in-progress manifest | 0 |
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
schema v4 全量运行已解析 3,689,316,811 行分钟线和 24,468,470 行日线。它发现
2019-08-12 的日线中有 29 行使用下一美东自然日午夜的 `window_start`；这些行可解析且源文件
SHA 与 manifest 一致。随后从 Massive S3 独立重新下载同一对象，得到完全相同的
SHA-256 `a9e2a03ffdcdefd37aacce082cd6ba97a1143a3ad0519830f3fdec60d7409b0e`，证明这是
provider 当前文件中的行级语义异常，不是本地 bit rot。Silver 必须隔离这 29 行。

2,513 天累计有 23,842,420 个 ticker-session 同时出现在两套产品中；day-only 626,050，
minute-only 16,579，合计占 union 的 2.62458%。单日最大缺口率 5.99123%，所以最终 v5 使用
10% 的灾难性覆盖失败阈值（至少缺 2 个 ticker），并继续把低于阈值的差异完整报告为 QA，
避免把美股半日市常见的 5%–6% 跨产品覆盖差异误判成文件损坏。

v5 会用新增的规范日线时间、独立 source path、依赖版本 cache binding 和逐字段分母重新生成
最终 JSON；最终报告路径和精确字段 mismatch rate 在该运行完成后固化。
<!-- MARKET_FINAL_END -->

数值 mismatch 属于 cross-product reconciliation difference，不进入物理损坏计数。Massive
[Condition Codes](https://massive.com/docs/rest/stocks/market-operations/condition-codes/) 明确给出
不同交易条件是否更新 open/close、high/low、volume 的规则。因此 Silver 必须选定并版本化
自己的可交易/RTH 聚合口径，并保留原始日线作为独立 QA 基准。

## REST 语义检查

正式计划语义报告：

```text
/mnt/HC_Volume_106309665/american_stocks/manifests/audits/rest_semantics/
└── full-2026-07-12-v2.json
```

报告 SHA-256：`8fe2bd4880bab3692a39645b4b405390ae4203e0b734e0adcd190ae81866decb`。

| 检查 | 结果 |
| --- | ---: |
| 正式 manifests / pages / rows | 82 / 7,697 / 20,553,455 |
| Corruption | 0 |
| Splits 唯一候选键 | 26,337；0 冲突、0 精确重复 |
| Dividends 唯一候选键 | 710,559；0 冲突、0 精确重复 |
| News 唯一候选键 | 807,868；0 冲突、0 精确重复 |
| Disclosure taxonomy | 119 个定义；118 个被使用；0 无法解码 |
| Risk taxonomy | 140 个定义；140 个被使用；0 无法解码 |
| 8-K disclosure / 8-K text 缺失 EDGAR accession | 0 / 0 |
| Form 3 / Form 4 缺失 EDGAR accession | 0 / 0 |

有 28,182 个差异诊断，不是文件损坏：

- Condition Codes 中 Massive code `30` 同时存在当前/legacy 映射，展开到 data type 后有 2 个
  候选键歧义；Silver 应保留 `legacy`、SIP 和完整 update rules，而不是只按整数 ID join。
- EDGAR Index 有 22,032 个精确重复 excess rows，以及 6,148 个 `(accession_number, cik)`
  对应多个 metadata 版本。联合申报允许同一 accession 对应多个 CIK；Silver 先按规范整行
  hash 去精确重复，再保留 metadata 版本与来源，不能只按 accession 粗暴去重。

## Universe 与单行异常

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

### Ticker Events 404

正式 identifier receipt 有 15,173 行，其中 11,471 个请求成功、3,702 个请求经重试稳定返回
HTTP 404；另有 100 个隔离 pilot（84 个 404）。完整审计看到的 3,786 个 404 是正式与 pilot
合计，不能错误解释为 3,786 个损坏文件。事件 endpoint 是辅助身份 QA；每日 point-in-time
universe 仍由完整的 active + inactive Assets 快照承担，所以这些 404 不形成幸存者偏差缺口。

## 日频因子与 Barra 输入完备性

| 模块 | Bronze 状态 | 可支持内容 | 限制/下一步 |
| --- | --- | --- | --- |
| 行情与成交 | 完整 | 收益、动量、反转、beta、残差波动率、流动性、换手 proxy、执行 VWAP | Silver 需定义 RTH/全时段与复权口径 |
| Point-in-time universe | 完整 | active + inactive、退市、代码/身份连续性，控制幸存者偏差 | 去重 inactive 版本行；保留 lineage |
| 公司行动 | 完整 | splits、cash dividends、IPO/listing age | Silver 按生效日构造复权链并做事件 QA |
| SEC/持仓/文本 | 完整 | EDGAR、Form 3/4、13-F、10-K、8-K、risk、news | 只能按 filing/published time 入场；处理修订与重复 |
| 做空与 float | 可用范围完整 | short interest、short volume、当前 float QA | short volume 仅自 2024；float 不是历史序列 |
| 宏观 | 完整 | 利率、通胀、预期、劳动力市场 regime controls | Silver 必须加入实际发布日期 lag |
| 基本面三表 | **权限阻塞** | value、profitability、growth、leverage、quality | 代码和年度 `filing_date` 计划已就绪；当前 Key 对三 endpoint 均 403 |
| Ratios | **权限阻塞且非历史** | 仅当前截面 QA | 官方 endpoint 是 latest-only，不能替代 point-in-time 历史重算 |
| 历史 size / industry | **部分就绪** | 可先做价格/成交量风格和有限 SIC 中性化 | 需 shares proxy 政策；SIC 安全覆盖仅 54.27%，无完整 PIT GICS |

Massive 官方文档显示 Stocks Advanced 对
[Income Statements](https://massive.com/docs/rest/stocks/fundamentals/income-statements)、
[Balance Sheets](https://massive.com/docs/rest/stocks/fundamentals/balance-sheets) 和
[Cash Flow Statements](https://massive.com/docs/rest/stocks/fundamentals/cash-flow-statements)
应提供 EOD 全历史，记录回溯到 2009-03-29；但当前远程 Key 的安全单行 probe 均为 HTTP 403。
[Ratios](https://massive.com/docs/rest/stocks/fundamentals/ratios) 明确只计算最近交易日、无历史。
因此正确动作是让 Massive 修复 entitlement 后运行已写好的年度下载计划，而不是回退到退役
endpoint 或把今天的 ratios/market cap 回填到过去。

明确排除的超大数据只有逐笔 Trades 与 Quotes。它们在十年尺度为多 TB，且日频因子与本阶段
Barra-style 模型不依赖逐笔成交/报价。SMA/EMA/MACD/RSI 等可从 immutable bars 重算，也不应
重复下载 provider 副本。

## 可复现命令与证据路径

```bash
cd /opt/american_stocks

.venv/bin/ame-audit-bronze \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --mode full --workers 8 \
  --output manifests/audits/bronze/full-2026-07-12-v3.json

.venv/bin/ame-audit-market \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --workers 1 \
  --output manifests/audits/market_crosscheck/full-2026-07-12-v4.json

.venv/bin/ame-audit-rest-semantics \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --start 2016-07-11 --end 2026-07-09 \
  --output manifests/audits/rest_semantics/full-2026-07-12-v2.json
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
