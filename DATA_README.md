# Ame_Stocks 数据说明与字段字典

本文档解释 Ame_Stocks 远程数据盘中已经保存的每一种 Massive 数据、原始字段结构、
候选去重键、计划用途，以及回测中最容易造成未来数据或幸存者偏差的地方。

本文档主要记录 **Bronze 原始层**，并单独记录两个交易日已经生成的
`silver_unadjusted` 验证样本。它们还不是完成复权或可直接回测的全量 Silver/Gold 数据。
截至 2026-07-12，我们尚未执行十年全量粗聚合、复权或 Gold 特征转换。

## 1. 先说结论：现在到底下载了什么

实际历史行情只下载了两套全市场每日 Flat File：

- 十年分钟聚合线，一天一个 gzip CSV。
- 十年日聚合线，一天一个 gzip CSV，用于快速 QA 和交叉核对。

其余数据不是更多的价格副本，而是构建可靠回测所需的参考数据、公司行动、做空、
SEC、新闻和宏观数据。特别是每天的 active + inactive 股票清单，用来避免只看到今天仍
存续股票所产生的幸存者偏差。

代码中仍保留 `minute_bars` 和 `daily_bars` REST 适配能力，供少量 ticker 做抽样验证；
十年历史行情没有再按 ticker 逐个通过 REST 重复下载。

远程数据盘在：

```text
/mnt/HC_Volume_106309665/american_stocks
```

当前主要目录是：

```text
/mnt/HC_Volume_106309665/american_stocks/
├── bronze/massive/
│   ├── flatfiles/us_stocks_sip/minute_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
│   ├── flatfiles/us_stocks_sip/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
│   └── <dataset>/request_id=<request_id>/page-00000.json.gz
└── manifests/massive/
    ├── flatfiles/<dataset>/YYYY-MM-DD.json
    └── <dataset>/<request_id>.json
```

服务器上的代码目录是 `/opt/american_stocks`。本地代码目录是
`/Users/joe/dinghy/american_stocks`。数据不放进 Git，也不会从服务器复制进本地仓库。

## 2. 当前库存快照

下表来自 2026-07-12 对远程 manifest 的只读统计。`记录数` 是 Bronze 页面的原始结果数，
尚未去重；`压缩体积` 是 manifest 记录的 payload/file 字节数，不含目录和文件系统开销。
完成 13-F、8-K、Ticker Overview 和 Condition Codes 补充后，manifest 记录的 Bronze
压缩 payload 合计约 55.71 GiB；文件系统实时占用以审计报告时的 `df` 为准。

| 数据集 | 覆盖或快照日期 | 当前规模 | 压缩体积 | 状态 |
| --- | --- | ---: | ---: | --- |
| `minute_aggregates` | 2016-07-11 至 2026-07-09 | 2,513 个交易日文件 | 45.51 GB | 完成 |
| `day_aggregates` | 2016-07-11 至 2026-07-09 | 2,513 个交易日文件 | 478.65 MB | 完成 |
| `assets` active + inactive | 2016-07-11 至 2026-07-09 | 69,381,182 行；每天两次请求 | 2.53 GB | 完成；4,853 组 inactive 版本行待 Silver 归一化 |
| `ticker_overview` | 生命周期查询日 2016-07-11 至 2026-07-09 | 30,739 个生命周期响应 | 15.78 MB | 完成；0 失败 |
| `ticker_overview_safe` v2 | 同上 | 30,739 行；30,570 行身份可验证 | 3.41 MB | 完成；第一阶段白名单表 |
| `splits` | 2003-09-10 至 2026-07-09 | 26,337 行 | 1.41 MB | 完成 |
| `dividends` | 2003-09-10 至 2026-07-09 | 710,559 行 | 42.95 MB | 完成 |
| `short_interest` | 2017-12-29 至 2026-07-09 | 3,781,607 行 | 48.82 MB | 完成 |
| `short_volume` | 2024-02-06 至 2026-07-09 | 8,302,971 行 | 257.02 MB | 完成 |
| `float` | 2026-07-09 当前快照 | 6,649 行 | 90.43 KB | 完成；非历史序列；1 行缺 ticker 待隔离 |
| `ipos` | 2008-01-01 至 2026-07-09 | 5,492 行 | 273.51 KB | 完成 |
| `ticker_events` | 每个 identifier 的完整事件时间线 | 所有成功响应共 13,104 个事件 | 2.53 MB | 正式计划 11,471 个完成、3,702 个确认 HTTP 404；193 条空 ticker 待隔离；另有 100 个审计 pilot |
| `ticker_types` | 2026-07-09 当前字典 | 24 行 | 494 B | 完成 |
| `exchanges` | 2026-07-09 当前字典 | 27 行 | 1.06 KB | 完成 |
| `condition_codes` | 2026-07-09 当前字典 | 94 行 | 2.10 KB | 完成；非历史序列 |
| `edgar_index` | 2016-07-11 至 2026-07-09 | 10,977,028 行 | 254.41 MB | 完成 |
| `form_3` | 2016-07-11 至 2026-07-09 | 335,813 行 | 35.25 MB | 完成 |
| `form_4` | 2016-07-11 至 2026-07-09 | 6,270,978 行 | 1.19 GB | 完成 |
| `form_13f` | 2016-07-11 至 2026-07-09 | 正式计划 100,287,362 行、41 个季度 | 5.58 GB | 完成；另保留 3,396,312 行审计 pilot |
| `ten_k_sections` | 2016-07-11 至 2026-07-09 | 153,194 行 | 2.93 GB | 完成；候选版本保留待 Silver 规范化 |
| `eight_k_text` | 2016-07-11 至 2026-07-09 | 458,067 行 | 416.58 MB | 完成 |
| `eight_k_disclosures` | 实际返回 2022-01-03 至 2026-07-09 | 正式计划 338,778 行；另有 14,000 行审计 pilot | 49.28 MB | 完成；请求的 2016–2021 年为空响应 |
| `disclosure_taxonomy` | 2026-07-09 当前字典 | 119 行 | 6.00 KB | 完成；非历史序列 |
| `risk_factors` | 2016-07-11 至 2026-07-09 | 627,674 行 | 68.08 MB | 完成；30,449 个同页精确重复 excess rows 待去重 |
| `risk_taxonomy` | 2026-07-09 当前字典 | 140 行 | 13.43 KB | 完成；非历史序列 |
| `news` | 2016-06-22 至 2026-07-09 | 807,868 行 | 217.21 MB | 完成 |
| `treasury_yields` | 1962-01-02 至 2026-07-09 | 16,113 行 | 194.32 KB | 完成 |
| `inflation` | 1947-01-01 至 2026-07-09 | 953 行 | 21.81 KB | 完成 |
| `inflation_expectations` | 1982-01-01 至 2026-07-09 | 534 行 | 15.03 KB | 完成 |
| `labor_market` | 1948-01-01 至 2026-07-09 | 942 行 | 7.06 KB | 完成 |

`ticker_events` 正式计划中的 3,702 个失败项已经全部重试并稳定返回 HTTP 404；它们是实验
endpoint 对 identifier 的不可用覆盖，不是成功文件损坏，也不是中断后漏下。正式计划使用
15,173 行不可变 identifier receipt；另有 100 个审计 pilot，不进入正式覆盖率。`form_13f`
表中只列 41 个正式季度；额外 pilot 保持隔离。`eight_k_disclosures` 的 4 个非权威 pilot
合计 14,000 行；另一个非权威 manifest 是 13-F pilot。Silver 仍必须按 filing/holding 语义
处理修订和行级重复，不能把 Bronze 原始行数直接当成最终唯一持仓数。

### 2.1 2026-07-12 全面审计结果

本次对 29 个数据集、56,242 个 manifest 和 232,519 个实际文件进行了独立全量校验：重新
计算 SHA-256、把每个 gzip 读到 EOF 以验证 CRC、解析 JSON/CSV、重算记录数、核对分页与正式
下载计划。物理损坏、截断、hash/bytes/row mismatch、计划内漏项、orphan/partial 文件均为 0。
REST 声明的 205,944,660 行与重新解析行数完全一致；分钟和日线分别解析出 3,689,316,811 与
24,468,470 行。

审计也发现了必须在 Silver 显式处理、但不应改写 Bronze 的 provider 语义差异：Assets 有
4,853 组 inactive 版本行；Float 有一行缺 ticker；Ticker Events 有 193 条空 ticker 占位行；
EDGAR Index 有 22,032 个精确重复 excess rows 和 6,148 个候选 metadata 版本；Risk Factors
有 30,449 个来自 provider 同一页面的精确重复 excess rows；10-K Sections 有 9,910 个候选键
版本；13-F 有 152 条只有 filing metadata、holding 字段整组缺失的 HR/HR-A 记录。所有受检
SEC accession 均能在 EDGAR 找到且 filing date 一致；Form 13-F 的 CIK/form type 也能由同一条
EDGAR identity row 精确见证。两套 taxonomy 也都可解码。

Day Aggregate 与分钟线自行聚合存在很大的产品口径差异；行情 Flat File 中唯一行级硬异常是
2019-08-12 日线中的 29 个非规范 session timestamp。独立重新下载同一 S3 对象后 SHA 完全
一致，证明不是本地 bit rot。完整方法、精确计数、证据路径和 Barra 完备性结论见
[Bronze 全面数据审计](docs/bronze-audit-2026-07-12.md)。

## 3. 通用格式与类型约定

### 3.1 REST Bronze 响应外壳

除 Flat Files 外，每个 `.json.gz` 解压后都是 Massive 的原始 JSON 响应。大部分 endpoint
具有下面的外壳：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `status` | string | Massive 对本次响应给出的状态，通常为 `OK`。 |
| `request_id` | string | Massive 为单次 HTTP 响应分配的请求 ID；不要与我们的确定性 manifest `request_id` 混淆。 |
| `count` | integer，可选 | 当前响应或请求返回的数量；并非每个 endpoint 都提供。 |
| `next_url` | string，可选 | 下一页游标 URL；最后一页或单页响应没有该字段。 |
| `results` | array[object] | 绝大多数 endpoint 的数据行数组。 |
| `results` | object | `ticker_events` 是例外，结果是带 `events[]` 的对象。 |

JSON 中很多字段是可选字段。字段缺失不等于数值为零，也不等于 `false`；Silver 必须保留
“未知/未提供”和“真实为零”的区别。

本文使用以下类型：

- `integer`：整数。
- `number`：可能是整数或小数的数值。
- `string(date)`：`YYYY-MM-DD` 日期。
- `string(datetime)`：带时区或以 `Z` 结尾的时间戳。
- `array[T]`：元素类型为 `T` 的数组。
- `object`：嵌套对象。

### 3.2 时间和主键的总原则

- `ticker` 不是永久公司 ID。代码变化时必须结合每日 security master、FIGI、CIK 和
  `ticker_events`。
- 本文给出的“候选键”用于设计 Silver 去重，不代表 Massive 对唯一性的合同保证。
- `filing_date`、`published_utc`、`settlement_date`、`period_end` 含义不同。用于回测时，必须
  选择市场当时真正已经知道的时间，而不是经济事件所属期间。
- 只有日期、没有具体发布时间的数据，默认在该日收盘后才可用，除非之后补充可靠的发布
  日历和时间戳。

## 4. 行情 Flat Files

官方说明：[Stocks Flat Files overview](https://massive.com/docs/flat-files/stocks/overview)、
[Minute Aggregates](https://massive.com/docs/flat-files/stocks/minute-aggregates)、
[Day Aggregates](https://massive.com/docs/flat-files/stocks/day-aggregates)。

Massive 明确说明 Stocks Flat Files 的价格和成交量没有针对拆股、分红或其他公司行动做
调整。因此 Bronze 保留原样，Silver 才会用 `splits` 和 `dividends` 构造复权序列。

### 4.1 `minute_aggregates`

- 粒度：一行是一个 ticker 在一个有合格成交的分钟窗口内的 OHLCV。
- 物理分区：一个交易日一个 gzip CSV。
- 候选键：`(ticker, window_start)`。
- 主要用途：计算日内特征、t+1 日 09:30–10:00 ET VWAP、检查停牌和分钟缺口。

### 4.2 `day_aggregates`

- 粒度：一行是一个 ticker 的日聚合 OHLCV。
- 物理分区：一个交易日一个 gzip CSV。
- 候选键：`(ticker, window_start)`。
- 主要用途：快速日频研究、与分钟线自行聚合结果交叉核对。
- 正式回测的日频特征仍应从定义明确的分钟 RTH 窗口派生，不能默认 provider 日线与我们的
  RTH/半日市口径完全一致。

两种文件具有相同字段：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ticker` | string | 该条 bar 使用的交易代码。 |
| `volume` | number | 窗口内合格成交量；可能因碎股等原因不是严格整数。 |
| `open` | number | 窗口第一笔合格成交的价格。 |
| `close` | number | 窗口最后一笔合格成交的价格。 |
| `high` | number | 窗口最高成交价。 |
| `low` | number | 窗口最低成交价。 |
| `window_start` | integer | 窗口起点的 Unix 纳秒时间戳。必须按 UTC 解析后转换为 `America/New_York`。 |
| `transactions` | integer | 参与该聚合窗口的成交笔数。 |

重要注意事项：

- 文件没有为“无成交分钟”补空 bar；缺一行可能代表无合格成交，而不是下载失败。
- Flat File 可能包含常规时段之外的活动。Silver 必须依据美股交易日历和 ET 时间过滤
  09:30–16:00，半日市使用当天真实收盘时间。
- 不能用文件中是否出现某 ticker 来推断它当日 active/inactive；上市状态来自 `assets`。
- 当前文件是长表，不是 Pandas pickle，也没有 pivot 成“一行一只股票、分钟为列”的宽表。
  目前只有 2016-07-11 和 2021-07-12 两个验证日已经转换成每日 Parquet。全量粗聚合阶段
  仍会沿用这种稳定存储；Pandas/Polars 只是读取和处理接口。需要矩阵运算时再在内存中
  pivot，避免股票池和交易分钟变化导致物理 schema 每天改变。

## 5. 每日股票池与参考字典

### 5.1 `assets`：每日 active + inactive security master

官方说明：[All Tickers](https://massive.com/docs/rest/stocks/tickers/all-tickers)。

对每个交易日分别请求 `active=true` 和 `active=false`，合并后得到当日 provider 在
`locale=us, market=stocks` 下可见的交易所上市 ticker 集合。这是避免幸存者偏差的核心输入，
而不是行情数据；Massive 的 `market=otc` 是另一套 universe，当前未纳入。

- 粒度：一行是某个查询日、某个 ticker 的参考状态。
- 分区信息：查询日和 `active=true/false` 在 manifest 的 `request` 中，不在每一行重复。
- 候选键：`(as_of_date, ticker)`；合并两次请求时还要验证同一 ticker 不同时出现在两边。
- 回测用途：先按信号日构造当日 universe，再与当天/次日行情连接。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `active` | boolean | 在查询日期是否仍处于活跃交易状态；`false` 通常表示已退市。 |
| `ticker` | string | 查询日期对应的交易代码。 |
| `name` | string | 证券或发行人名称。 |
| `market` | string | 市场类别；本项目请求固定为 `stocks`。 |
| `locale` | string | 地域；本项目请求固定为 `us`。 |
| `type` | string，可选 | Massive ticker type 代码，需用 `ticker_types` 解码。 |
| `currency_name` | string，可选 | 交易计价货币名称。 |
| `primary_exchange` | string，可选 | 主要上市地 MIC，应与 `exchanges.mic` 连接。 |
| `cik` | string，可选 | SEC Central Index Key，通常是零填充字符串。 |
| `composite_figi` | string，可选 | Composite FIGI；用于跨 ticker 变化辅助识别同一证券。 |
| `share_class_figi` | string，可选 | Share Class FIGI；区分不同股份类别。 |
| `delisted_utc` | string(datetime)，可选 | provider 记录的最后交易日期/退市时间，仅 inactive 记录常见。 |
| `last_updated_utc` | string(datetime)，可选 | 该参考信息更新到的时间。 |
| `base_currency_name` | string，可选 | 官方 schema 的基准货币名称；当前 U.S. stock 样本未观察到。 |
| `base_currency_symbol` | string，可选 | 官方 schema 的基准货币代码；当前样本未观察到。 |
| `currency_symbol` | string，可选 | 官方 schema 的 ISO 货币代码；当前样本未观察到。 |

注意：同一个 `ticker` 可被不同实体在不同历史时期复用，FIGI/CIK 也各有缺失或层级差异。
Silver 不会选择某一个字段当万能永久主键，而会保存带生效区间的内部 `asset_id` 映射及冲突
QA。

### 5.2 `ticker_types`

官方说明：[Ticker Types](https://massive.com/docs/rest/stocks/tickers/ticker-types)。

这是当前参考字典，不是历史变化表。候选键为 `(asset_class, locale, code)`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `asset_class` | string | 资产大类，如 `stocks`。 |
| `locale` | string | 地域，如 `us`。 |
| `code` | string | Massive 在 `assets.type` 中使用的短代码。 |
| `description` | string | 该类型的可读说明，例如普通股、ETF 等。 |

### 5.3 `exchanges`

官方说明：[Exchanges](https://massive.com/docs/rest/stocks/market-operations/exchanges)。

这是当前交易场所字典，不是历史交易所成员表。候选键为 `id`，对外连接优先使用 `mic`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | integer | Massive 的交易场所内部 ID。 |
| `name` | string | 交易场所名称。 |
| `acronym` | string，可选 | 常用缩写。 |
| `mic` | string，可选 | ISO 10383 Market Identifier Code。 |
| `operating_mic` | string，可选 | 运营该场所的机构 MIC。 |
| `participant_id` | string，可选 | SIP 使用的参与者代码。 |
| `type` | string | 场所类型，如 `exchange`、`TRF` 或 `SIP`。 |
| `asset_class` | string | 资产大类。 |
| `locale` | string | 地域。 |
| `url` | string，可选 | 交易场所网站。 |

### 5.4 `condition_codes`

官方说明：[Condition Codes](https://massive.com/docs/rest/stocks/market-operations/condition-codes/)。

这是当前交易、报价条件码字典，不是历史逐笔数据，也不是一份历史版本序列。它解释上游
CTA、UTP、FINRA 等条件如何影响 open、high、low、close 和 volume。当前 94 行快照主要用于
理解 provider 聚合口径、记录未来逐笔研究的过滤规则；本项目没有因此下载被明确排除的
逐笔 trades/quotes。

条件 `id` 需要在对应 data type 下解释，但当前快照证明 `(asset_class, data_type, id)` 仍可能
同时对应当前定义与 `legacy=true` 的旧定义。一行也可以适用于多个 `data_types`。Silver 应先
展开数组，再保留 `legacy`、`name` 和 SIP mapping 作为消歧字段；不能按三字段键静默覆盖。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | integer | Massive 条件 ID；按 data type 解释。 |
| `name` | string | 条件名称。 |
| `type` | string | 条件类别，如 sale condition、quote condition 或 financial status。 |
| `asset_class` | string | 资产类别；本项目请求固定为 `stocks`。 |
| `data_types` | array[string] | 条件适用的数据类型，如 trade、quote。 |
| `exchange` | integer，可选 | 仅对特定交易场所有效时的交易所 ID。 |
| `legacy` | boolean，可选 | 是否为已不再使用的旧 SIP 条件。 |
| `sip_mapping` | object | Massive 条件到不同 SIP 原始符号的映射。 |
| `update_rules` | object | 该条件是否更新 OHLC、volume 等聚合字段的规则。 |

这份字典也解释了为什么 provider 的 Day Aggregate 不必等于我们把 Minute Aggregate 简单
`groupby` 后的结果：不同条件可能采用不同的聚合更新规则。该差异需要在 Silver 明确定义
自己的 RTH 口径，而不是把跨产品差异自动判成 gzip 损坏。

### 5.5 `ticker_events`

官方说明：[Ticker Events](https://massive.com/docs/rest/stocks/corporate-actions/ticker-events)。

当前只请求 `ticker_change` 事件，主要用 Composite FIGI 批量查询。这个 endpoint 本身返回某个
identifier 的完整事件时间线；manifest 中的 `start/end` 用于本项目的请求身份和审计，并不
是该 endpoint 的服务端日期过滤条件。

`results` 不是数组，而是以下对象：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `results.name` | string | 资产名称。 |
| `results.cik` | string，可选 | 响应中观察到的 SEC CIK。 |
| `results.composite_figi` | string，可选 | 响应中观察到的 Composite FIGI。 |
| `results.events` | array[object] | 与该 identifier 关联的事件数组。 |
| `results.events[].date` | string(date) | 事件生效日期。 |
| `results.events[].type` | string | 事件类型；当前下载目标为 `ticker_change`。 |
| `results.events[].ticker_change` | object | ticker 变化的嵌套对象。 |
| `results.events[].ticker_change.ticker` | string | 该事件后对应的 ticker。 |

候选去重键为 `(composite_figi, events[].date, events[].type,
events[].ticker_change.ticker)`。它用于验证 ticker 连续性，但每日能否交易仍以 `assets` 的
point-in-time 状态为准。

### 5.6 `ticker_overview` 与第一阶段安全表

官方说明：[Ticker Overview](https://massive.com/docs/rest/stocks/tickers/ticker-overview)。

Ticker Overview 是逐 ticker 的详情 endpoint，不适合按每天、每只股票重复请求。本项目先读取
十年 `assets active=true` 快照，将同一 ticker 下反复出现的相同 FIGI/CIK 合并成一个证券
生命周期，再在该生命周期的 `last_active_date` 请求一次 Overview。最终 v2 生命周期表包含
30,739 行、25,381 个唯一 ticker；单 ticker 最多 7 个不同身份生命周期。

权威 v2 路径是：

```text
/mnt/HC_Volume_106309665/american_stocks/
├── staging/ticker_overview/schema=v2/
│   └── window=2016-07-11_2026-07-09/
│       ├── lifecycles.parquet
│       └── requests.csv
├── bronze/massive/ticker_overview/request_id=<request_id>/page-00000.json.gz
└── silver_unadjusted/reference/ticker_overview_safe/schema=v2/
    └── window=2016-07-11_2026-07-09/ticker_overview.parquet
```

数据盘还保留了未带 `schema=v2` 的早期 QA 产物。早期生命周期算法会把 A→B→A 的重复身份
错误拆成三个生命周期，产生 40,229 行；它只用于审计修复过程，不能用于下载或研究。旧的
安全投影 v1 同样被 v2 取代，没有删除是为了保持产物不可变和可追溯。

Overview 原始 Bronze 完整保存 provider 响应。实际响应可能包含以下字段：

| 字段 | 类型 | 第一阶段策略 |
| --- | --- | --- |
| `ticker` | string | 身份字段；与精确查询日和生命周期连接。 |
| `name` | string | 证券/发行人名称；身份辅助字段。 |
| `type` | string | 证券类型代码；身份/筛选辅助字段。 |
| `market`、`locale` | string | 市场和地域身份上下文。 |
| `active` | boolean | 查询日附近的 provider 状态；不能替代每日 `assets` universe。 |
| `primary_exchange` | string | 主要交易所 MIC；身份辅助字段。 |
| `currency_name` | string | 交易币种；身份辅助字段。 |
| `cik` | string，可选 | SEC 发行人身份。 |
| `composite_figi` | string，可选 | Composite FIGI。 |
| `share_class_figi` | string，可选 | Share Class FIGI。 |
| `sic_code` | string，可选 | SEC Standard Industrial Classification 代码；第一阶段允许。 |
| `sic_description` | string，可选 | SIC 描述；第一阶段允许。 |
| `list_date` | string(date)，可选 | provider 记录的上市日期；第一阶段允许。 |
| `ticker_root`、`ticker_suffix` | string，可选 | ticker 结构字段；只用于身份 QA。 |
| `delisted_utc` | string(datetime)，可选 | 退市时间；只用于生命周期 QA。 |
| `market_cap` | number，可选 | **Bronze-only 隔离字段**；历史 as-of 语义不足，禁止进入第一阶段。 |
| `weighted_shares_outstanding` | number，可选 | **Bronze-only 隔离字段**；禁止进入第一阶段。 |
| `share_class_shares_outstanding` | number，可选 | **Bronze-only 隔离字段**；禁止进入第一阶段。 |
| `round_lot` | number，可选 | Bronze 保留，当前不进入第一阶段。 |
| `description`、`homepage_url`、`phone_number`、`total_employees` | 多种 | Bronze 保留，当前不进入第一阶段。 |
| `address`、`branding` | object，可选 | Bronze 嵌套描述对象，当前不进入第一阶段。 |

安全 Parquet 不复制任意未知原始字段，而是使用固定 allowlist。输出列包括：

- 生命周期审计：`lifecycle_id`、`source_request_id`、`query_ticker`、`query_date`、
  `first_active_date`、`last_active_date`。
- 身份校验：`identity_type`、`identity_value`、`identity_match`、
  `identity_match_basis`。
- 响应身份：`ticker`、`name`、`type`、`market`、`locale`、`active`、
  `primary_exchange`、`currency_name`、`cik`、`composite_figi`、
  `share_class_figi`、`delisted_utc`、`ticker_root`、`ticker_suffix`。
- 第一阶段研究字段：`sic_code`、`sic_description`、`list_date`。

QA 结果：30,570 / 30,739 行至少有一个可比的 ticker/CIK/FIGI 身份一致且没有冲突；
169 行只有相同 ticker、响应未提供可比较的 CIK/FIGI，因此 `identity_match=false`，第一阶段
必须排除或人工复核。SIC code 覆盖 16,682 行，上市日期覆盖 23,417 行；没有一行的
`list_date > query_date`。安全表中不存在 `market_cap` 或任何 shares-outstanding 列。

即使传入历史 `date`，Overview 中来自 SEC 的描述字段也不能自动视为精确 filing-time
point-in-time 数据。当前安全表是带审计标记的参考输入，不是已经获准直接回填到每日因子的
完整历史基本面表。

## 6. 公司行动

### 6.1 `splits`

官方说明：[Splits](https://massive.com/docs/rest/stocks/corporate-actions/splits)。

- 粒度：一行是一次拆股、并股或股票股利事件。
- 候选主键：`id`。
- 回测用途：调整价格、成交量、持仓股数，并检查拆股日的机械跳变。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | string | provider 为事件分配的唯一 ID。 |
| `ticker` | string | 事件记录使用的 ticker。 |
| `execution_date` | string(date) | 股数和价格按新比例生效的日期。 |
| `adjustment_type` | string | `forward_split`、`reverse_split` 或 `stock_dividend` 等事件分类。 |
| `split_from` | number | 比例中的旧股数，即分母。 |
| `split_to` | number | 比例中的新股数，即分子。2:1 拆股通常是 from=1、to=2。 |
| `historical_adjustment_factor` | number | provider 给出的累计历史价格调整因子，用于归一到当前股份基础。 |

`historical_adjustment_factor` 是累计/当前口径，不应被当成当日事件比例再重复相乘。Silver 会
同时保留 provider 因子和依据 `split_from/split_to` 自行推导的事件因子，逐事件验证后才生成
复权价格。

### 6.2 `dividends`

官方说明：[Dividends](https://massive.com/docs/rest/stocks/corporate-actions/dividends)。

- 粒度：一行是一次现金分红事件。
- 候选主键：`id`。
- 回测用途：计算总收益、现金流和分红因子。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | string | provider 为分红事件分配的唯一 ID。 |
| `ticker` | string | 发行分红的 ticker。 |
| `cash_amount` | number | 原始每股现金分红金额。 |
| `split_adjusted_cash_amount` | number | 按后续拆股换算到当前股份基础的每股分红。 |
| `currency` | string | 分红币种。 |
| `declaration_date` | string(date)，可选 | 公司正式宣布分红的日期。 |
| `ex_dividend_date` | string(date) | 除息日；当日开始买入不再获得本次分红。 |
| `record_date` | string(date)，可选 | 股权登记日。 |
| `pay_date` | string(date)，可选 | 实际支付日。 |
| `frequency` | integer | 预计年度支付频率；0 表示非经常/不规则，4 通常表示季度。 |
| `distribution_type` | string | `recurring`、`special`、`supplemental`、`irregular` 或 `unknown` 等。 |
| `historical_adjustment_factor` | number | provider 给出的累计历史价格调整因子。 |

做收益复权时主要依据除息日和现金金额；做“分红公告”因子时只能从 `declaration_date` 之后
使用。不能把 `pay_date`、`record_date` 或今天回看得到的累计因子误当成历史已知信息。

## 7. 做空与流通盘

### 7.1 `short_interest`

官方说明：[Short Interest](https://massive.com/docs/rest/stocks/fundamentals/short-interest)。

这是 FINRA 经纪商报告的未平仓空头数量，通常每两周一次，不是每日成交量。

- 粒度：一个 ticker 在一个 settlement date 的汇总。
- 候选键：`(ticker, settlement_date)`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ticker` | string | 股票 ticker。 |
| `settlement_date` | string(date) | 做空余额统计的结算日期。 |
| `short_interest` | integer，可选 | 尚未回补/平仓的卖空股数。 |
| `avg_daily_volume` | integer | 计算 days-to-cover 使用的平均日成交量。 |
| `days_to_cover` | number | `short_interest / avg_daily_volume`，估计回补所需交易日。 |

关键限制：数据只有 `settlement_date`，没有实际对市场发布的时间戳。Silver 必须补充 FINRA
发布日历或采用保守延迟，绝不能在 settlement date 当天就把数值用于交易。

### 7.2 `short_volume`

官方说明：[Short Volume](https://massive.com/docs/rest/stocks/fundamentals/short-volume)。

这是 FINRA/ATS 等报告场所的每日卖空成交量，不等于整个市场的未平仓 short interest，也
不应与 SIP 总成交量直接假设为同一覆盖口径。

- 粒度：一个 ticker 一个交易日。
- 候选键：`(ticker, date)`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ticker` | string | 股票 ticker。 |
| `date` | string(date) | 成交活动日期。 |
| `short_volume` | number | 所覆盖场所报告的总卖空成交量。 |
| `exempt_volume` | number | 标记为 Regulation SHO exempt 的卖空成交量。 |
| `non_exempt_volume` | number | 非 exempt 卖空量，通常等于 `short_volume - exempt_volume`。 |
| `total_volume` | number | 这些报告场所的总成交量。 |
| `short_volume_ratio` | number | `short_volume / total_volume × 100`，单位是百分数。 |
| `adf_short_volume` | integer | ADF 报告的非 exempt 卖空量。 |
| `adf_short_volume_exempt` | integer | ADF 报告的 exempt 卖空量。 |
| `nasdaq_carteret_short_volume` | integer | Nasdaq Carteret 非 exempt 卖空量。 |
| `nasdaq_carteret_short_volume_exempt` | integer | Nasdaq Carteret exempt 卖空量。 |
| `nasdaq_chicago_short_volume` | integer | Nasdaq Chicago 非 exempt 卖空量。 |
| `nasdaq_chicago_short_volume_exempt` | integer | Nasdaq Chicago exempt 卖空量。 |
| `nyse_short_volume` | integer | NYSE 场所非 exempt 卖空量。 |
| `nyse_short_volume_exempt` | integer | NYSE 场所 exempt 卖空量。 |

### 7.3 `float`

官方说明：[Float](https://massive.com/docs/rest/stocks/fundamentals/float)。

这是下载日能看到的最新自由流通盘快照，不是历史 float 时间序列。

- 粒度：一个 ticker 一条最新测量。
- 候选键：`(ticker, effective_date)`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ticker` | string | 股票 ticker。 |
| `effective_date` | string(date) | 当前 float 测量的生效日期。 |
| `free_float` | integer | 可自由交易股数，排除战略、控制、锁定等长期持股。 |
| `free_float_percent` | number | 自由流通股占总发行股数的百分比。 |

这份快照不能回填到过去十年。当前阶段只能用于数据展示、当下截面或方法验证；把今天的
float 用在历史规模中性化会造成明显未来数据。

全量审计发现其中一行没有 `ticker`（`effective_date=2026-01-29`、
`free_float=3950100`、`free_float_percent=20.5`）。原始行和文件校验均正常，但无法安全连接
资产身份；Silver 必须隔离该行，不能按数值或日期猜测 ticker。

## 8. IPO 与上市事件

### 8.1 `ipos`

官方说明：[Initial Public Offerings](https://massive.com/docs/rest/stocks/corporate-actions/ipos)。

- 粒度：一行是一个 IPO/DPO 事件记录。
- 没有 provider event ID；Silver 候选键应使用稳定字段组合后生成 row hash，并保留修订。
- 主要用途：上市年龄、IPO cohort、新股过滤和发行规模因子。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ticker` | string | IPO 事件对应的 ticker。 |
| `issuer_name` | string | 发行人名称。 |
| `announced_date` | string(date)，可选 | IPO 事件首次宣布日期。 |
| `listing_date` | string(date)，可选 | 首个交易日。 |
| `last_updated` | string(date/datetime) | provider 最后修改该事件的时间。 |
| `ipo_status` | string | `rumor`、`pending`、`new`、`history`、`postponed`、`withdrawn`、`direct_listing_process` 等。 |
| `currency_code` | string | 报价/发行币种。 |
| `lowest_offer_price` | number，可选 | 发行价区间下限。 |
| `highest_offer_price` | number，可选 | 发行价区间上限。 |
| `final_issue_price` | number，可选 | 最终发行价。 |
| `min_shares_offered` | integer，可选 | 拟发行股数下限。 |
| `max_shares_offered` | integer，可选 | 拟发行股数上限。 |
| `total_offer_size` | number，可选 | 发行募集总额。 |
| `shares_outstanding` | integer，可选 | 发行后总流通/已发行股数。 |
| `lot_size` | integer，可选 | 最小交易单位。 |
| `primary_exchange` | string，可选 | 主要上市交易所 MIC。 |
| `security_type` | string，可选 | 证券类型代码。 |
| `security_description` | string，可选 | 证券描述。 |
| `isin` | string，可选 | International Securities Identification Number。 |
| `us_code` | string，可选 | provider 返回的 9 位北美证券识别码字段。 |

历史下载得到的是今天查询时 provider 保存的最终/修订状态，不等于当年每一天市场看到的
IPO 预期。`listing_date` 适合上市年龄；若研究 rumor/pending 状态，则必须另有历史快照或
`last_updated` 的严格 point-in-time 逻辑。

## 9. SEC filing 数据

所有 SEC 类数据的首要可用时间是 `filing_date`，不是财务或持仓所属的 `period` /
`period_end`。当前 endpoint 主要提供日期而非精确提交时刻，因此默认在 filing date 收盘后
才进入因子，下一交易日成交。

### 9.1 `edgar_index`

官方说明：[SEC EDGAR Index](https://massive.com/docs/rest/stocks/filings/index)。

这是 SEC filing 总目录。联合申报时，同一份 accession 可以对应多个 registrant/CIK，因此
`accession_number` 单独不是行主键。第一阶段候选键为 `(accession_number, cik)`，同时使用
规范化整行 hash 去除 provider 的精确重复；不能误删同 accession 下的合法联合申报主体。

正式计划语义审计在 10,977,028 行中找到 22,032 个精确重复 excess rows，以及 6,148 个
`(accession_number, cik)` 对应多个 metadata 版本的候选键。后者常见于 issuer metadata 更新，
不是断裂 accession。Silver 必须先按规范整行 hash 去除精确重复，再保留版本来源；所有
13-F、8-K、Form 3 和 Form 4 accession 均能在 EDGAR Index 中找到，filing date 也一致。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `accession_number` | string | SEC filing 的唯一 accession number。 |
| `cik` | string | 提交主体的 SEC CIK。 |
| `ticker` | string，可选 | provider 映射的 ticker。 |
| `issuer_name` | string | 提交主体名称。 |
| `form_type` | string | 表单类型，如 `10-K`、`10-Q`、`8-K`、`4` 等。 |
| `filing_date` | string(date) | 文件提交到 SEC 的日期。 |
| `filing_url` | string | SEC EDGAR 原始 filing 链接。 |

### 9.2 `form_3` 和 `form_4` 共同字段

官方说明：[Form 3](https://massive.com/docs/rest/stocks/filings/form-3)、
[Form 4](https://massive.com/docs/rest/stocks/filings/form-4)。

Form 3 建立内幕人士首次受 Section 16 约束时的持仓基线；Form 4 记录之后的交易和持仓变化。
一份 filing 会拆成多行证券/交易记录，因此 `accession_number` 单独不是行主键。Silver 会以
完整规范化行生成 hash，并另外保存 filing-level 主表。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `accession_number` | string | SEC filing 的 accession number。 |
| `form_type` | string | `3`/`3/A` 或 `4`/`4/A` 等标准表单与修订表单。 |
| `filing_date` | string(date) | 提交 SEC 的日期，是回测可用性锚点。 |
| `filing_url` | string | SEC 原始 filing 链接。 |
| `date_of_original_submission` | string(date)，可选 | 修订 filing 对应的原始提交日期。 |
| `period_of_report` | string(date) | filing 报告的事件日期；不能代替 `filing_date`。 |
| `issuer_cik` | string | 发行人零填充 CIK。 |
| `issuer_name` | string | filing 中的发行人名称。 |
| `tickers` | array[string] | 发行人 ticker 列表，可能包含多个 share class。 |
| `owner_cik` | string | 报告所有人的 CIK。 |
| `owner_name` | string | 报告所有人姓名或实体名。 |
| `is_director` | boolean | 是否为董事。 |
| `is_officer` | boolean | 是否为高管。 |
| `is_ten_percent_owner` | boolean | 是否为 10% 及以上持有人。 |
| `is_other` | boolean | 是否为其他类型的报告关系。 |
| `officer_title` | string，可选 | 高管职务。 |
| `not_subject_to_section_16` | boolean，可选 | 是否不受 Section 16 约束；Form 3 官方可选 schema 中存在，但当前 Form 3 样本未观察到。 |
| `aff_10b5_one` | boolean，可选 | 是否关联 Rule 10b5-1 计划；Form 4 实际可见，Form 3 官方可选 schema 中存在但当前样本未观察到。 |
| `security_title` | string | 证券名称，如 Common Stock、Stock Option。 |
| `security_type` | string | `non-derivative` 或 `derivative`。 |
| `direct_or_indirect` | string | `D` 表示直接持有，`I` 表示间接持有。 |
| `nature_of_ownership` | string，可选 | 间接持有方式，如 trust、spouse。 |
| `exercise_date` | string(date)，可选 | 衍生证券开始可行权日期。 |
| `exercise_price` | number，可选 | 衍生证券行权/转换价格。 |
| `underlying_security_title` | string，可选 | 衍生证券对应的底层证券名称。 |
| `underlying_security_shares` | number，可选 | 衍生证券对应的底层股数。 |
| `footnotes` | array[object]，可选 | 与当前行有关的 filing 脚注。 |
| `footnotes[].id` | string | 脚注标识。 |
| `footnotes[].description` | string | 脚注正文。 |
| `remarks` | string，可选 | filing 备注。 |

Form 3 额外字段：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `shares_owned` | number | 首次报告时对该证券的实益持有数量。 |

Form 4 额外字段：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `record_type` | string | 该行是 `transaction` 还是 `holding` 等记录类型。 |
| `transaction_date` | string(date)，可选 | 实际交易日期。 |
| `deemed_execution_date` | string(date)，可选 | 与交易日期不同的视同执行日期。 |
| `transaction_code` | string，可选 | SEC 交易代码，如 `P` 买入、`S` 卖出、`A` 授予、`M` 行权/转换。 |
| `transaction_acquired_disposed` | string，可选 | `A` 表示获得，`D` 表示处置。 |
| `transaction_shares` | number，可选 | 交易涉及股数。 |
| `transaction_price_per_share` | number，可选 | 每股交易价格。 |
| `transaction_value` | number，可选 | provider 计算的交易总值；价格或股数缺失时也会缺失。 |
| `transaction_timeliness` | string，可选 | `O` 为按时，`L` 为迟报。 |
| `shares_owned_following_transaction` | number，可选 | 交易后实益持有数量。 |
| `equity_swap_involved` | boolean，可选 | 交易是否涉及 equity swap。 |
| `expiration_date` | string(date)，可选 | 衍生证券到期日。 |

交易因子的符号不能只看 `transaction_code`；需要结合 `record_type`、
`transaction_acquired_disposed`、证券类型、脚注和修订 filing。市场真正得知交易的时间仍是
`filing_date`，不是 `transaction_date`。

### 9.3 `form_13f`

官方说明：[13-F Filings](https://massive.com/docs/rest/stocks/filings/13-f-filings)。

`13F-HR` / `13F-HR/A` 响应行是 information table 的持仓项，不是一份 filing；候选去重键
必须包含 `accession_number`、CUSIP、证券类别、put/call、discretion 等行级字段或直接使用
规范化行 hash。`13F-NT` / `13F-NT/A` 是 filing-level notice 行，没有持仓明细，因此下表的
issuer/CUSIP/market-value/share/voting 字段对 NT 不适用、允许缺失。13F 修订表需要另外定义
替换/追加语义。

实际 Bronze 还包含 152 条 `13F-HR` / `13F-HR/A` header-only 记录：正式计划 137 条，pilot
15 条。它们只有 filing metadata，7 个 holding 字段整组不存在；没有任何 partial holding 或
真实坏数值。它们必须进入 filing header 表并标记
`holdings_status=not_public_or_unavailable`，不能当成零持仓或写入 holding fact 表。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `accession_number` | string | SEC filing accession number。 |
| `filer_cik` | string | 申报机构零填充 CIK。 |
| `form_type` | string | `13F-HR`、`13F-HR/A`、`13F-NT` 或 `13F-NT/A`。 |
| `filing_date` | string(date) | 向 SEC 提交日期，是回测可用性日期。 |
| `filing_url` | string | SEC filing 链接。 |
| `period` | string(date) | 持仓所属季度末；不能从季度末当天使用。 |
| `file_number` | string | 13F filing file number。 |
| `film_number` | string | SEC EDGAR film number。 |
| `issuer_name` | string | 被持有证券的发行人名称。 |
| `cusip` | string | 被持有证券 CUSIP。 |
| `title_of_class` | string | 证券类别描述，如 `COM`、`CL A`。 |
| `market_value` | integer | Massive 标准化后的持仓市值，官方字段口径为 USD。 |
| `shares_or_principal_amount` | integer | 持有股数或本金数量。 |
| `shares_or_principal_type` | string | `SH` 表示股数，`PRN` 表示本金。 |
| `put_call` | string，可选 | 正式 Bronze 实际值为大小写敏感的 `Put`、`Call`；官方文档也列出 `PUT`、`CALL`，普通股票通常缺失。 |
| `investment_discretion` | string | 正式 Bronze 实际值为 `SOLE`、`DFND`、`OTR`；审计也接受官方文档列出的 `SHARED`。 |
| `other_managers` | array[string] | 共同拥有投资裁量权的其他 manager。 |
| `voting_authority_sole` | integer | 单独投票权股数。 |
| `voting_authority_shared` | integer | 共享投票权股数。 |
| `voting_authority_none` | integer | 无投票权股数。 |

HR/HR-A 的合同允许两种互斥形态：完整 holding 字段并通过数值/domain 校验，或 holding 字段
整组缺失并作为 header-only warning；部分缺失仍是 error。NT/NT-A 只要求 accession、filer
CIK、form type、filing date 和 period 等 filing-level 字段。13-F 通常在季度末之后才提交。
任何 crowding/机构持仓因子都必须按 `filing_date` 逐份进入，
不能把整季持仓回填到 `period` 日期。

### 9.4 `ten_k_sections`

官方说明：[10-K Sections](https://massive.com/docs/rest/stocks/filings/10-k-sections)。

一行是一份 10-K 的一个标准化叙述章节。候选键为 `(cik, filing_date, section)`，同时保留
正文 hash 处理修订。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `cik` | string | 发行人 CIK。 |
| `ticker` | string | provider 映射的 ticker。 |
| `filing_date` | string(date) | 10-K 提交日期。 |
| `period_end` | string(date) | 财务报告期末；不是可用日期。 |
| `section` | string | 标准化章节名，如 `business`、`risk_factors`。 |
| `text` | string | 清洗/解析后的章节全文，仍包含标题和部分格式。 |
| `filing_url` | string | SEC 原始 filing 链接。 |

### 9.5 `eight_k_text`

官方说明：[8-K Text](https://massive.com/docs/rest/stocks/filings/8-k-text)。

一行是一份 8-K/8-K-A 的解析正文。候选主键为 `accession_number`，修订表单单独处理。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `accession_number` | string | SEC filing accession number。 |
| `cik` | string | 发行人 CIK。 |
| `ticker` | string | provider 映射的 ticker。 |
| `form_type` | string | `8-K` 或 `8-K/A` 等。 |
| `filing_date` | string(date) | 提交日期。 |
| `items_text` | string | 解析出的 Item 编号、标题和正文。 |
| `filing_url` | string | SEC 原始 filing 链接。 |

### 9.6 `eight_k_disclosures`

官方说明：[8-K Disclosures](https://massive.com/docs/rest/stocks/filings/8-k-disclosures)。

一行是 provider 从一份 8-K 中识别出的一个标准化事件披露片段。同一 accession number 可以
对应多个分类和多段 `supporting_text`，所以 accession number 单独不是行主键。Silver 应按
filing 身份、三级分类和正文 hash 去重，并保留原始行。

本项目请求了 2016-07-11 至 2026-07-09，但 endpoint 实际只返回 2022-01-03 之后的数据；
2016–2021 的年度请求均成功完成、返回空数组。这是 provider 覆盖边界，不应把 2022 之前
解释成“没有 8-K 事件”。最终共有 338,778 行、203,169 个 accession number、9,441 个 CIK、
10,905 个 ticker；精确整行重复为 0。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `accession_number` | string | SEC filing accession number。 |
| `cik` | string | 提交主体 CIK。 |
| `filing_date` | string(date) | 8-K 提交日期，是事件因子的可用性锚点。 |
| `filing_url` | string | SEC 原始 filing 链接。 |
| `tickers` | array[string] | provider 映射的 ticker 列表，可能有多个 share class。 |
| `primary_category` | string | 一级披露类别。 |
| `secondary_category` | string | 二级披露类别。 |
| `tertiary_category` | string | 最细三级披露类别。 |
| `supporting_text` | string | 支持该事件分类的 filing 原文片段。 |

逐年原始行数为：2022 年 67,591；2023 年 78,641；2024 年 75,128；2025 年 77,419；
2026 年截至 7 月 9 日为 39,999。日期全部落在各自请求范围内。

### 9.7 `disclosure_taxonomy`

官方说明：[Disclosure Categories](https://massive.com/docs/rest/stocks/filings/disclosure-categories)。

这是当前分类体系快照，不是历史 taxonomy 版本表。119 行全部具有唯一的三级类别路径，并且
338,778 条 `eight_k_disclosures` 都能按
`(primary_category, secondary_category, tertiary_category)` 匹配成功。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `taxonomy` | string | 当前观察到的分类体系版本，例如 `1.0`。 |
| `primary_category` | string | 一级披露类别。 |
| `secondary_category` | string | 二级披露类别。 |
| `tertiary_category` | string | 三级披露类别。 |
| `description` | string | 该事件类别的定义和示例。 |

做历史因子时仍要用 8-K 自身的 `filing_date` 控制可用时间；今天下载的 taxonomy 只负责解码，
不能证明分类定义在历史上从未变化。

### 9.8 `risk_factors`

官方说明：[Risk Factors](https://massive.com/docs/rest/stocks/filings/risk-factors)。

一行是 provider 从 filing 中抽取并分类的一个风险片段。候选键需要 filing 身份、三级分类和
正文 hash；仅 `(cik, filing_date)` 不唯一。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `cik` | string | 发行人 CIK。 |
| `ticker` | string | provider 映射的 ticker。 |
| `filing_date` | string(date) | 来源 filing 的提交日期。 |
| `primary_category` | string | 一级风险类别。 |
| `secondary_category` | string | 二级风险类别。 |
| `tertiary_category` | string | 最细三级风险类别。 |
| `supporting_text` | string | 支持该分类的 filing 文本片段。 |

### 9.9 `risk_taxonomy`

官方说明：[Risk Categories](https://massive.com/docs/rest/stocks/filings/risk-categories)。

这是当前分类字典快照，用来解码 `risk_factors`，不是历史 point-in-time taxonomy。候选键为
`(taxonomy, primary_category, secondary_category, tertiary_category)`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `taxonomy` | number | 分类体系版本号。 |
| `primary_category` | string | 一级类别。 |
| `secondary_category` | string | 二级类别。 |
| `tertiary_category` | string | 三级类别。 |
| `description` | string | 类别定义、例子和潜在影响。 |

## 10. 新闻

### 10.1 `news`

官方说明：[Ticker News](https://massive.com/docs/rest/stocks/news)。

- 粒度：一行是一篇 provider 聚合的新闻文章元数据。
- 候选主键：`id`。
- 主要用途：注意力、新闻覆盖、provider 情绪和事件研究。

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `id` | string | provider 文章唯一 ID。 |
| `title` | string | 文章标题。 |
| `description` | string，可选 | 文章摘要/描述，不保证是完整正文。 |
| `author` | string，可选 | 作者。 |
| `published_utc` | string(datetime) | UTC 发布时刻，是新闻回测的主要可用时间。 |
| `article_url` | string | 原始文章 URL。 |
| `amp_url` | string，可选 | AMP 移动页面 URL。 |
| `image_url` | string，可选 | 文章图片 URL。 |
| `tickers` | array[string] | provider 关联的 ticker 列表。 |
| `keywords` | array[string]，可选 | 来源或 provider 关联的关键词。 |
| `publisher` | object | 新闻来源信息。 |
| `publisher.name` | string | 来源名称。 |
| `publisher.homepage_url` | string，可选 | 来源主页。 |
| `publisher.logo_url` | string，可选 | 来源 logo。 |
| `publisher.favicon_url` | string，可选 | 来源 favicon。 |
| `insights` | array[object]，可选 | provider 对文章和 ticker 的结构化洞察；远程文件中确实存在，但不是每篇都有。 |
| `insights[].ticker` | string | 该洞察对应的 ticker。 |
| `insights[].sentiment` | string | provider 分类的情绪，例如 positive/negative/neutral。 |
| `insights[].sentiment_reasoning` | string | provider 给出的情绪理由。 |

同一文章可能关联多个 ticker。Silver 应拆出 article 表和 article-ticker bridge，不能把整篇
文章重复计数为多条独立新闻。公开网站也不能向用户提供 Massive 原始新闻数据下载，只能
展示许可范围内的衍生统计。

## 11. 宏观数据

宏观 endpoint 的 `date` 通常是观测期，不是市场拿到数据的精确发布时间，也不包含 vintage/
revision 历史。用于历史回测前必须补充 release calendar、发布日期和修订规则；否则 CPI、
就业等数据非常容易产生未来数据。

### 11.1 `treasury_yields`

官方说明：[Treasury Yields](https://massive.com/docs/rest/economy/treasury-yields)。

- 粒度：一个日历日期一行。
- 候选键：`date`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `date` | string(date) | 收益率观测日期。 |
| `yield_1_month` | number，可选 | 1 个月美国国债常数期限收益率。 |
| `yield_3_month` | number，可选 | 3 个月常数期限收益率。 |
| `yield_6_month` | number，可选 | 6 个月常数期限收益率；官方 schema 有，当前 payload 未观察到。 |
| `yield_1_year` | number，可选 | 1 年常数期限收益率。 |
| `yield_2_year` | number，可选 | 2 年常数期限收益率。 |
| `yield_3_year` | number，可选 | 3 年常数期限收益率；官方 schema 有，当前 payload 未观察到。 |
| `yield_5_year` | number，可选 | 5 年常数期限收益率。 |
| `yield_7_year` | number，可选 | 7 年常数期限收益率；官方 schema 有，当前 payload 未观察到。 |
| `yield_10_year` | number，可选 | 10 年常数期限收益率。 |
| `yield_20_year` | number，可选 | 20 年常数期限收益率；官方 schema 有，当前 payload 未观察到。 |
| `yield_30_year` | number，可选 | 30 年常数期限收益率。 |

不同期限开始日期不同，所以早期行自然缺少后出现的期限；Silver 不会用零填充。

### 11.2 `inflation`

官方说明：[Inflation](https://massive.com/docs/rest/economy/inflation)。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `date` | string(date) | 观测期日期。 |
| `cpi` | number | 非季调 headline CPI 指数。 |
| `cpi_core` | number | 剔除食品和能源的 Core CPI 指数。 |
| `cpi_year_over_year` | number | Headline CPI 同比百分比变化。 |
| `pce` | number | PCE price index。 |
| `pce_core` | number | 剔除食品和能源的 Core PCE price index。 |
| `pce_spending` | number | 名义个人消费支出，官方口径为十亿美元。 |

候选键为 `date`。各列频率和修订机制可能不同，不能因为它们在同一行就假定同日同刻发布。

### 11.3 `inflation_expectations`

官方说明：[Inflation Expectations](https://massive.com/docs/rest/economy/inflation-expectations)。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `date` | string(date) | 观测日期。 |
| `market_5_year` | number | 5 年 breakeven inflation rate。 |
| `market_10_year` | number | 10 年 breakeven inflation rate。 |
| `forward_years_5_to_10` | number | 5y5y forward inflation expectation。 |
| `model_1_year` | number | Cleveland Fed 模型 1 年通胀预期。 |
| `model_5_year` | number | Cleveland Fed 模型 5 年通胀预期。 |
| `model_10_year` | number | Cleveland Fed 模型 10 年通胀预期。 |
| `model_30_year` | number | Cleveland Fed 模型 30 年通胀预期。 |

候选键为 `date`。市场 breakeven 与模型估计的生成时点不同，Silver 要分别维护可用时间。

### 11.4 `labor_market`

官方说明：[Labor Market](https://massive.com/docs/rest/economy/labor-market)。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `date` | string(date) | 观测期日期。 |
| `unemployment_rate` | number | 美国民用失业率，百分比。 |
| `labor_force_participation_rate` | number | 劳动力参与率，百分比。 |
| `avg_hourly_earnings` | number，可选 | 私营非农雇员平均时薪，美元。 |
| `job_openings` | number，可选 | 非农职位空缺数，单位为千人。 |

候选键为 `date`。早期历史没有后创建的序列，字段缺失是正常现象，不能做前向回填到序列
诞生之前。

## 12. Manifest 字段结构

Manifest 是本项目生成的下载审计记录，不是 Massive 数据本身。它使任务能够断点续传、
校验原始文件并保证 Bronze 不可变。

### 12.1 REST manifest

路径：

```text
/mnt/HC_Volume_106309665/american_stocks/manifests/massive/<dataset>/<request_id>.json
```

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `manifest_schema_version` | integer | 本项目 REST manifest schema 版本。 |
| `provider` | string | `massive`。 |
| `provider_version` | string | 下载时 Massive adapter 的代码版本。 |
| `provider_contract_version` | string | 项目统一 DataProvider 合同版本。 |
| `dataset` | string | 数据集名称。 |
| `request_id` | string | 根据规范化请求确定性计算的 ID；相同请求可幂等恢复。 |
| `request.dataset` | string | 请求的数据集。 |
| `request.start` | string(date) | 请求开始日期。 |
| `request.end` | string(date) | 请求结束日期。 |
| `request.asset_ids` | array[string] | ticker、FIGI 等请求标识；全市场请求为空数组。 |
| `request.adjusted` | boolean | 请求是否要求 provider 复权；当前 Bronze 原始行情设计为 `false`。 |
| `request.parameters` | object | active、types、filter 等额外参数。 |
| `status` | string | downloader 状态：`pending`、`in_progress`、`complete` 或 `failed`。 |
| `checkpoint` | object/null | 未完成分页的 `continuation` 和 `next_sequence`；完成后为 null。 |
| `artifacts` | array[object] | 已原子写入的每一页信息。 |
| `created_at` | string(datetime) | manifest 创建时间。 |
| `updated_at` | string(datetime) | 最近状态更新时间。 |
| `completed_at` | string(datetime)，可选 | 完成时间。 |
| `failure.error_type` | string，可选 | 失败异常类型。 |
| `failure.message` | string，可选 | 脱敏后的安全失败说明，不包含 API Key 或响应正文。 |

每个 `artifacts[]`：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `sequence` | integer | 从 0 开始的连续页号。 |
| `path` | string | 相对 data root 的 gzip JSON 路径。 |
| `content_type` | string | provider 响应 Content-Type。 |
| `record_count` | integer | 当前页 `results[]` 行数；ticker events 统计 `events[]` 数量。 |
| `raw_bytes` | integer | 压缩前响应字节数。 |
| `compressed_bytes` | integer | gzip 文件字节数。 |
| `raw_sha256` | string | 原始响应字节 SHA-256。 |
| `stored_sha256` | string | 确定性 gzip 文件 SHA-256。 |
| `is_last` | boolean | 是否为最后一页。 |
| `next_continuation` | string/null | 下一页游标；最后一页为 null。 |

### 12.2 Flat File manifest

路径：

```text
/mnt/HC_Volume_106309665/american_stocks/manifests/massive/flatfiles/<dataset>/YYYY-MM-DD.json
```

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `flat_file_manifest_schema_version` | integer | Flat File manifest schema 版本。 |
| `dataset` | string | `minute_aggregates` 或 `day_aggregates`。 |
| `session_date` | string(date) | 文件对应的交易日。 |
| `bucket` | string | Massive S3 bucket 名。 |
| `endpoint` | string | S3-compatible endpoint。 |
| `object_key` | string | Massive 远端对象键。 |
| `object_id` | string | 远端对象身份的确定性 hash。 |
| `remote.content_length` | integer | 远端声明字节数。 |
| `remote.etag` | string | 远端 ETag。 |
| `remote.last_modified` | string(datetime) | 远端对象最后修改时间。 |
| `status` | string | `in_progress`、`complete` 或 `failed`。 |
| `partial_bytes` | integer | 断点续传临时文件已有字节数；完成后为 0。 |
| `output.path` | string | data root 下的最终相对路径。 |
| `output.bytes` | integer | 完整文件字节数。 |
| `output.sha256` | string | 最终 gzip CSV 的 SHA-256。 |
| `output.csv_header` | array[string] | 下载后实际校验到的 CSV header。 |
| `created_at` / `updated_at` / `completed_at` | string(datetime) | 生命周期时间。 |

Manifest 的 `status` 是下载器内部状态，不是平台业务任务的
`queued → running → awaiting_review → succeeded/failed → published` 状态，两者不要混用。

## 13. 已有 `silver_unadjusted` 验证样本

远程已经对两个代表日执行过离线转换和 coverage QA：2016-07-11 与 2021-07-12。这些文件
用于验证 schema、幂等性、active/inactive 合并和 Flat File 转换，不是十年全量 Silver。

```text
/mnt/HC_Volume_106309665/american_stocks/silver_unadjusted/
├── minute/date=2016-07-11/bars.parquet
├── minute/date=2021-07-12/bars.parquet
├── universe/date=2016-07-11/tickers.parquet
├── universe/date=2021-07-12/tickers.parquet
├── coverage/date=2016-07-11/ticker_coverage.parquet
└── coverage/date=2021-07-12/ticker_coverage.parquet
```

| 日期 | Minute Parquet | Universe Parquet | Coverage Parquet |
| --- | ---: | ---: | ---: |
| 2016-07-11 | 1,248,941 bars；7,920 tickers | 20,793 tickers | 20,984 tickers，含 191 个 bars-without-reference |
| 2021-07-12 | 1,483,365 bars；10,251 tickers | 27,627 tickers | 27,627 tickers |

### 13.1 `minute/date=.../bars.parquet`

候选键为 `(ticker, timestamp_utc)`。这是 Bronze CSV 的类型规范化版本，仍未复权，也尚未
加入 ET session/RTH 标签。

| 字段 | Parquet 类型 | 含义 |
| --- | --- | --- |
| `session_date` | Date | 源 Flat File 对应的交易日分区。 |
| `timestamp_utc` | Datetime(ns, UTC) | 从 `window_start` 解析出的 UTC 纳秒时间戳。 |
| `ticker` | String | 原始 ticker。 |
| `open` | Float64 | 未复权分钟开盘价。 |
| `high` | Float64 | 未复权分钟最高价。 |
| `low` | Float64 | 未复权分钟最低价。 |
| `close` | Float64 | 未复权分钟收盘价。 |
| `volume` | Float64 | 未复权分钟成交量。 |
| `transactions` | Int64 | 分钟成交笔数。 |

### 13.2 `universe/date=.../tickers.parquet`

同一天的 `active=true` 和 `active=false` Bronze 响应被合并成一张表。候选键为
`(snapshot_date, ticker)`。

| 字段 | Parquet 类型 | 含义 |
| --- | --- | --- |
| `snapshot_date` | Date | point-in-time 查询日。 |
| `active_on_date` | Boolean | 合并规则得到的该日活跃状态，是研究使用列。 |
| `provider_active` | Boolean | Massive 原始 `active` 值，保留用于 QA。 |
| `ticker` | String | 当日 ticker。 |
| `type` | String | Massive ticker type。 |
| `name` | String | 证券名称。 |
| `market` | String | 市场类型。 |
| `locale` | String | 地域。 |
| `primary_exchange` | String | 主要交易所 MIC。 |
| `currency_name` | String | 计价货币名称。 |
| `cik` | String | SEC CIK。 |
| `composite_figi` | String | Composite FIGI。 |
| `share_class_figi` | String | Share Class FIGI。 |
| `delisted_utc` | String | provider 原始退市/最后交易时间字段。 |
| `last_updated_utc` | String | provider 原始参考信息更新时间。 |

Parquet String 列仍可为 null；类型为 String 不代表每行都有值。

### 13.3 `coverage/date=.../ticker_coverage.parquet`

这是 universe 与分钟 bar 按 ticker 做 outer join 后得到的 QA 表，不是新行情。

| 字段 | Parquet 类型 | 含义 |
| --- | --- | --- |
| `ticker` | String | QA ticker。 |
| `active_on_date` | Boolean | universe 中该日 active 状态。 |
| `type` | String | ticker type。 |
| `name` | String | 证券名称。 |
| `primary_exchange` | String | 主要交易所 MIC。 |
| `delisted_utc` | String | 退市/最后交易时间字段。 |
| `minute_count` | UInt32 | 当日 Flat File 中该 ticker 的分钟 bar 数。 |
| `first_bar_utc` | Datetime(ns, UTC) | 当日第一条 bar 时间。 |
| `last_bar_utc` | Datetime(ns, UTC) | 当日最后一条 bar 时间。 |
| `has_minute_bar` | Boolean | 是否至少有一条分钟 bar。 |
| `reference_missing` | Boolean | bar 中出现但当日 active/inactive reference 都没有该 ticker。 |
| `active_without_bars` | Boolean | reference 显示 active，但全天没有分钟 bar。可能是停牌、无成交或数据问题。 |
| `inactive_with_bars` | Boolean | reference 显示 inactive，但 Flat File 仍有 bar，需要进一步核对状态边界。 |

对应审计文件位于：

```text
/mnt/HC_Volume_106309665/american_stocks/manifests/materialized/
├── flatfiles/minute_aggregates/<date>.json
├── universe/<start>_<end>.json
└── coverage/<date>.json
```

它们记录 `sources`、`source_digest`、输出路径、`row_count`、字节数、SHA-256、重复数、null
统计和 coverage 异常计数，使验证样本可以幂等重跑并追溯到 Bronze。

## 14. 哪些没有下载

| 数据 | 当前不下载的原因 |
| --- | --- |
| 逐笔 `trades` | 用户明确排除，十年规模预计为多 TB，远超当前磁盘。 |
| `quotes` | 用户按“过大数据”原则排除；规模显著大于 aggregates，日频因子和 Barra 不需要逐笔报价。 |
| Financial statements / ratios | 官方显示 Stocks Advanced 应有权限，但当前远程 Key 对四个新 v1 endpoint 均返回 403；下载适配器已就绪，等待 Massive 核实并恢复 live access。 |
| OTC active/inactive universe | `market=otc` 不属于当前 exchange-listed Barra 股票池；若未来扩展，必须独立下载每日 active/inactive 快照并单独审计。 |
| SMA / EMA / MACD / RSI | 可由 immutable bars 确定性重算，没有必要保存 provider 副本。 |
| 全量逐 ticker REST minute bars | 与已经下载的全市场 Flat Files 重复；REST 只保留抽样 QA 能力。 |
| Live snapshot / movers / last trade | 只描述当前市场，不是历史回测输入。 |
| Related tickers | 当前关系图不是 point-in-time 历史，直接回填会泄漏未来关系。 |
| Ticker overview 全历史回填 | 某些 SEC 字段按报告期而非实际提交时间返回，容易产生未来数据。 |
| Benzinga partner feeds | 独立付费扩展，不属于当前 Stocks 套餐。 |

更完整的下载选择理由见
[docs/massive-research-catalog.md](docs/massive-research-catalog.md)。

## 15. Bronze 到 Silver/Gold 的计划

当前状态：远程 Key 当前可访问且在正式清单内的 Bronze 原始层已经保存，13-F 正式 41 个季度
也已完成。三张历史财务报表和当前 ratios 快照因 live access 403 尚未保存。两个验证日已经
生成未复权 `silver_unadjusted`，但以下十年全量转换、复权和 Gold 尚未启动。

### Silver 计划

1. 校验每个 manifest、SHA-256、页连续性和 Flat File header。
2. 将每日分钟 CSV 流式转换为类型稳定的每日 Parquet；保留原始 ticker、UTC ns 和未复权
   OHLCV，不覆盖 Bronze。
3. 按 ET 和交易日历标记 pre-market、RTH、after-hours、半日市及缺失分钟。
4. 合并每日 active/inactive 请求，建立带有效区间的 `asset_id` / ticker / FIGI / CIK 映射。
5. 将 splits 和 dividends 规范化为事件因子，生成可验证的价格、成交量和总收益复权列。
6. 对 filing、short interest、宏观等字段建立严格的 `available_at`，不能只用报告期日期。
7. 对 pilot、分页重试、修订 filing 和多 ticker 新闻做确定性去重，同时保留 lineage。

### Gold 计划

- 从 Silver minute bars 生成日频特征和 t+1 09:30–10:00 ET VWAP。
- 生成可交易性、停牌、上市年龄、退市和数据质量标记。
- 保存带数据版本、因子版本和回测参数的因子值及回测结果。

选择 Parquet 而不是 Pandas pickle 的原因是：Parquet 有明确列类型、压缩好、可跨语言读取、
支持只读部分列，也比 pickle 更适合长期版本管理。Pandas 仍然是主要分析接口：
`pandas.read_parquet()` 直接得到 DataFrame。

## 16. 最重要的回测防泄漏检查表

| 数据 | 可用于信号的最早时间原则 |
| --- | --- |
| 分钟/日行情 | bar 窗口结束之后；日收盘因子从下一交易日成交。 |
| 每日 `assets` | 使用信号日对应快照，不能使用今天的 active 状态。 |
| splits | 从 execution date 的经济生效规则处理；累计当前复权因子不能当历史已知事件。 |
| dividends | 总收益按 ex-date；公告因子从 declaration date 之后。 |
| short interest | settlement date 不是发布日期，必须加入 FINRA 发布延迟。 |
| short volume | 当日汇总默认收盘后可用。 |
| Form 3/4 | 用 filing date，不用 transaction/period date 提前。 |
| 13-F | 用每份 filing date，不用 quarter-end `period` 提前。 |
| 10-K/8-K/risk | 用 filing date，不用 period end 提前。 |
| news | 用 `published_utc`，转换到 ET 后判断能否进入当日信号。 |
| macro | `date` 是观测期；必须另外建立 release/vintage 时间。 |
| float / dictionaries | 当前快照不能伪装成十年历史。 |

任何字段在这里被列出，只表示它存在于 Bronze，不表示已经安全到可以直接放入因子。
