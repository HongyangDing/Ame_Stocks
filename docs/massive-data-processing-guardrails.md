# Massive 数据处理护栏与时点规则

本文记录 Ame_Stocks 在处理 Massive 美股数据时必须遵守的约束，供后续 Bronze →
Silver → Gold、Barra-style 因子、回测和数据质量页面共同使用。

本文是处理规则，不是下载完成情况清单。任何批处理开始前，都必须以
`/mnt/HC_Volume_106309665/american_stocks/manifests` 中的 manifest 为实际状态来源。

## 1. 适用范围

当前已下载或已纳入下载/处理范围的数据主要包括：

- 全市场分钟和日频 Aggregate Flat Files；
- 每个交易日的 active/inactive ticker reference；
- splits、dividends、IPO 和 ticker events；
- Ticker Overview 身份、SIC 和上市信息；
- short interest、short volume 和最新 free float；
- EDGAR index、Forms 3/4/13F、10-K、8-K 和风险文本；
- news、Treasury yields、通胀和劳动力市场等数据。

其中一部分数据可能处于下载中、失败或仅有最新快照。目录存在不代表数据可用于研究。

## 2. 不可违反的原则

1. **Bronze 永久不可变。** 不清洗、不复权、不改字段、不覆盖原文件。
2. **只处理 `status=complete` 的 manifest。** `in_progress`、`failed` 或缺 manifest 的数据不得进入 Silver。
3. **所有时点都按当时可见信息处理。** 报告期、交易日期和公开日期不得混用。
4. **永远不把 ticker 本身当永久 `asset_id`。** ticker 只是带有效期的别名。
5. **保留 Massive 标识符的原始大小写。** `BCPC` 和 `BCpC` 可能是不同证券。
6. **行情是否存在不能决定股票是否属于当日股票池。** 股票池以当日 reference 为左表。
7. **缺失不等于零。** 无分钟线、停牌、无成交和下载缺失必须区分。
8. **复权结果不得覆盖未复权数据。** Raw、split-adjusted 和 total-return 必须是不同字段或不同数据层。
9. **所有衍生产物必须可追溯。** 保存来源 manifest、代码版本、参数、行数、校验和和 schema 版本。
10. **同样输入必须得到同样输出。** 排序、去重、缺失处理和冲突处理必须确定性执行。

## 3. 数据层职责

### 3.1 Bronze

Bronze 保存供应商响应本身：

- Flat Files 保留 Massive 原始 gzip CSV 字节；
- REST 页面保留响应 JSON，外层只做 gzip 压缩；
- 保存请求参数、分页顺序、原始和压缩后 SHA-256、行数和完成状态；
- 下载采用临时文件和原子发布；
- 不允许为了节省空间删除 Bronze，也不能用 Silver 反向重建 Bronze。

读取 Bronze 前必须验证：

- manifest 状态为 `complete`；
- 分页 sequence 连续且最后一页确实结束；
- 文件存在，gzip/CRC 正常；
- stored/raw SHA-256 与 manifest 一致；
- Flat File object key、交易日期和实际内容一致。

### 3.2 Silver Unadjusted

Silver Unadjusted 只负责结构化，不负责经济含义上的复权：

- 强类型解析；
- UTC 时间戳及纽约交易日期；
- ticker 原样保留；
- active/inactive universe；
- 身份映射和覆盖率 QA；
- 明确记录重复、缺失和冲突，不静默修复。

当前约定的主要路径包括：

```text
silver_unadjusted/
├── minute/date=YYYY-MM-DD/bars.parquet
├── daily/date=YYYY-MM-DD/bars.parquet
├── universe/date=YYYY-MM-DD/tickers.parquet
├── coverage/date=YYYY-MM-DD/ticker_coverage.parquet
└── reference/
```

### 3.3 Silver Adjusted

未来 Silver Adjusted 应单独保存：

- stable `asset_id`；
- 拆股调整因子；
- `price_raw`、`price_split_adjusted`、`price_total_return_adjusted`；
- 对应口径的 volume 和 shares；
- corporate-action 事件来源；
- 调整规则版本。

不能只有一个含义不明确的 `adjusted_close`。

### 3.4 Gold

Gold 保存日频特征、因子暴露、因子收益、风险模型和回测结果。每个 Gold 产物至少记录：

- 数据版本和最大可用时间；
- universe 版本；
- adjustment 版本；
- factor ID 和 factor version；
- 计算参数、回看期和标准化规则；
- 输入、输出行数和校验和；
- Git commit。

## 4. Manifest 和批次完成性

### 4.1 目录存在不代表完成

一个 dataset 目录可能同时包含：

- 完整批次；
- 可续传的 `in_progress` 批次；
- 404、权限或分页错误产生的 `failed` 批次；
- 成功页面和未完成 checkpoint。

因此禁止通过 `find` 到文件或 `du` 看到大小就认定下载完成。

### 4.2 批处理入口条件

进入 Silver 前必须生成输入清单，并确认：

- 预期交易日数与 complete manifest 数一致；
- 每个交易日只有一个预期 Flat File；
- daily active 和 inactive 两类 reference 都存在；
- 没有 `.part` 文件被当成正式输入；
- 本次输入范围、截止日和 provider contract version 已冻结。

失败条目只能：

- 重试；
- 标记为明确缺失；
- 进入人工 review/quarantine。

禁止把失败条目从分母中删除后宣称覆盖完整。

## 5. 永久身份和 ticker 生命周期

### 5.1 ID 层级

最终身份模型使用：

| 层级 | 标识 | 用途 |
| --- | --- | --- |
| 可交易证券 | 内部 UUID `asset_id` | 平台永久主键 |
| 美国市场证券 | `composite_figi` | 主要外部证券身份依据 |
| 全球 share class | `share_class_figi` | 跨市场 share-class 关联 |
| 公司/发行人 | 内部 `issuer_id`，映射 CIK | 基本面和公司事件 |
| 某段时间的名称 | ticker + `valid_from/valid_to` | 行情和事件连接 |

CIK 不能作为 `asset_id`，因为同一发行人可以有多个普通股类别、优先股或 ADR。

### 5.2 身份处理规则

- ticker 改名、公司改名和普通拆股通常保持同一 `asset_id`；
- 相同 ticker 在不同时间对应不同 FIGI 时，必须拆成不同 `asset_id`；
- 同一 FIGI 的非重叠 ticker 生命周期可以作为同一资产的候选映射；
- 同一 ticker、同一日期出现相互冲突的 FIGI/CIK 时必须 quarantine；
- 缺 FIGI 时生成 provisional 内部 UUID，不能回退为 ticker 主键；
- 仅凭公司名称相似、ticker root 相同或 CIK 相同，不自动合并证券；
- `lifecycle_id` 是处理阶段的可复现标识，不应自动等同于最终 `asset_id`。

### 5.3 大小写

Massive ticker 是 case-sensitive 标识符。任何处理层都禁止：

```text
ticker.upper()
ticker.lower()
不区分大小写的 join
```

如果前端需要统一显示，应增加 display 字段，不能修改 join key。

## 6. 股票池和存活偏差

### 6.1 当日 universe

信号日 `t` 的研究股票池必须从 `active_on_date=true` 的 reference 开始，再按当时可见的：

- security type；
- primary exchange；
- 上市日期；
- 价格、流动性和最短历史；
- 因子所需字段完整性

进行筛选。

禁止使用：

- 今天仍然 active 的股票列表重建历史；
- 因子计算结束后才知道的数据筛选当日股票池；
- “当天有 bar 的 ticker 集合”替代 universe；
- 最终回测存活股票反向筛选历史样本。

### 6.2 三类覆盖率异常

每日 coverage full join 必须保留：

- `active_without_bars`：reference 认为 active，但没有分钟聚合；
- `inactive_with_bars`：reference 认为 inactive，但当日仍有行情；
- `bars_without_reference`：有行情，但 active/inactive reference 都没有该 ticker。

2016-07-11 pilot 的已验证结果是：

```text
active_without_bars = 597
inactive_with_bars  = 54
bars_without_reference = 191
```

2021-07-12 pilot 是：

```text
active_without_bars = 612
inactive_with_bars  = 0
bars_without_reference = 0
```

这些数字是 QA 信号，不是自动删除规则：

- active 无 bar 可能是停牌、极度不活跃、刚上市或数据缺口；
- inactive 有 bar 可能是状态切换、盘外成交、修正成交或旧 reference 语义差异；
- bar 缺 reference 需要身份解析，未解决前保留数据但不进入正式新开仓股票池。

### 6.3 下单后不得重新排名

在 `t` 日收盘冻结订单后，若某只股票在 `t+1` 的执行窗口没有价格：

- 订单记为未成交并保留现金；
- 不得删除该股票后重新对剩余股票排序或补入下一名；
- 未成交原因和金额必须进入回测结果。

否则会把 `t+1` 才知道的信息用于修改 `t` 日组合。

## 7. 交易时间和分钟线

### 7.1 时区

- `window_start` 按 Unix nanoseconds、UTC 解析；
- 使用 `America/New_York` 转换交易时段和 `session_date`；
- 禁止写死 UTC-4 或 UTC-5，必须正确处理 DST；
- Flat File 文件名日期应与纽约时间下的交易日期一致；
- 所有库表保留 `timestamp_utc`，需要时另派生本地时间字段。

### 7.2 交易时段

Massive Flat Files 可能包含 04:00–20:00 ET 的盘前、RTH 和盘后活动。处理时必须明确口径：

- RTH：通常为 09:30–16:00 ET；
- 半日市：通常提前至 13:00 ET 收盘；
- 执行窗口：项目约定为次日 09:30–10:00 ET；
- provider day aggregate 与我们自行聚合的 RTH day bar 不能假定相同。

交易日和半日市必须使用交易所日历，不能只通过 weekday/节假日硬编码。

### 7.3 Canonical 分钟形态

Canonical 分钟数据建议按交易日分区、long format 保存：

```text
session_date, timestamp_utc, asset_id, open, high, low, close, volume, transactions
```

不把 390 个分钟槽永久 pivot 成数百列。原因包括：

- 半日市只有 210 个 RTH 分钟；
- 停牌和无成交导致分钟天然稀疏；
- schema 不会因交易时段变化而改变；
- Polars/Parquet 更容易按列扫描和增量处理。

页面或研究临时需要矩阵时再 pivot。

### 7.4 分钟缺失和重复

- 无 bar 不等于零成交量，也不等于上一分钟价格；
- 不为所有 active 股票强制填满 390 行；
- 停牌、无成交和数据缺失应使用不同状态字段；
- 原始重复 key 为 `(ticker, timestamp_utc)`，身份解析后的正式 key 为
  `(asset_id, timestamp_utc)`；
- 重复不得用“随便保留第一条”处理，应先记录重复率和差异，再使用版本化规则解决；
- 任何 forward-fill 只能用于明确的估值用途，不能伪装成真实成交 bar。

### 7.5 行情字段有效性

至少检查：

- price 为正数；
- volume、transactions 非负；
- `high >= max(open, close, low)`；
- `low <= min(open, close, high)`；
- timestamp 落在文件对应的纽约交易日期；
- 同一 key 唯一；
- 每日 ticker 数、行数和成交量分布是否突然断层。

## 8. 日频粗聚合

分钟数据的第一步粗聚合必须同时保留两种日频来源：

1. Massive 原始 Day Aggregates；
2. 从 Minute Aggregates 独立生成的 `derived_rth_daily`。

两者不能覆盖彼此。它们用于交叉验证：

- open/close 是否由同一交易时段定义；
- high/low 是否包含盘前盘后；
- volume 差异是否来自交易时段或 eligible trade 规则；
- 拆股日和异常日是否出现数量级跳变。

日频 canonical 表仍推荐 long format：

```text
session_date, asset_id, open, high, low, close, volume, transactions, source_scope
```

若某证券当天无成交，保留 universe 记录和 missing reason，不虚构 OHLC。

## 9. 拆股、分红和收益率

Massive Flat Files 是未复权行情。未处理 corporate actions 前，不得直接用于：

- 动量和反转；
- 波动率和 Beta；
- 横截面收益回归；
- 净值曲线；
- 价格异常检测。

### 9.1 拆股

- 以 execution date 对齐；
- 2:1、1:10 reverse split 和 stock dividend 都要有手算测试；
- price 与 volume/shares 的调整方向相反；
- 调整前后市值应在无其他信息时近似连续；
- 不得同时应用 Massive adjustment factor 和自行 split ratio，避免双重复权。

### 9.2 分红

- 总收益在 ex-dividend date 体现，不用 pay date；
- `cash_amount`、`split_adjusted_cash_amount` 和价格口径必须一致；
- special、supplemental、irregular dividend 不得按普通季度分红处理；
- 外币分红必须保存 currency，未经汇率转换不能直接加入美元收益；
- 推荐同时保存 price return 和 total return，不能混为一个 `return` 字段。

### 9.3 调整字段

建议显式保存：

```text
split_factor
dividend_cash
close_raw
close_split_adjusted
close_total_return_adjusted
return_price
return_total
adjustment_version
```

## 10. Ticker Overview 和时点泄漏

Ticker Overview 可以提供身份、SIC、上市日期、市值和 shares outstanding，但其历史 `date`
参数不能自动视为“投资者在该日已经知道”。供应商可能按 SEC report period 选择资料，而财报在之后才提交。

因此当前规则是：

- Bronze 完整保留所有响应字段；
- Silver allowlist 可以使用身份、SIC、list/delist 等参考字段，但标记为 provisional；
- `market_cap`、`share_class_shares_outstanding`、`weighted_shares_outstanding`
  进入 quarantine，不直接进入因子；
- SEC 衍生字段只有经过独立 filing/availability date 验证后才能用于历史因子；
- daily market cap 最终应由“当时已公开的 shares × 当日价格”计算。

禁止把当前 market cap、当前 shares 或当前 SIC 回填到整个历史区间。

## 11. 事件、披露和替代数据的可用时间

每条非行情数据应至少区分：

```text
observation_date / period_end
event_date
filing_date
published_at / received_at
available_session
```

因子只能在 `available_session` 及以后使用。

### 11.1 SEC 和基本面

- 财报 `period_end` 不是可用日期；
- 使用 filing date/time；若只有日期，采用保守的下一交易日可用规则；
- 收盘后提交的文件只能用于下一交易日信号；
- amendment 不得无痕覆盖原 filing，应版本化；
- 8-K、10-K 文本和风险因子按公开时间生效；
- 未来财务三表必须自己计算历史 ratio，不使用当前 ratio 回填。

### 11.2 13F、Forms 3/4

- 13F 的 quarter end 不是持仓可见时间，只能在 filing 公开后使用；
- Form 4 的 transaction date 与 filing date 分开；
- 原始 filing 更正、重复 accession 和 amendment 必须去重并保留版本；
- CIK 连接 issuer，不应直接连接某一 share class。

### 11.3 Short interest 和 short volume

- short-interest settlement date 不是公开日；
- 使用 dissemination/publication date，无法获得时采用保守 lag；
- short volume 是成交活动指标，不等于空头存量；
- 两者不能在同一字段或同一因子定义中混用。

### 11.4 Float

当前 Massive Float 是 latest snapshot：

- 只能用于最新截面或展示；
- 不得回填历史；
- 历史 turnover 优先使用当时已公开 shares outstanding；
- 如果将来按期保存 float snapshot，每个快照都要记录 effective/capture date。

### 11.5 News

- 使用文章首次 publication timestamp；
- 去重 syndicated/canonical URL 和重复文章；
- 修订版不能把新文本回填到原始发布时间；
- sentiment 模型版本、输入文本版本和 ticker mapping 都必须记录。

### 11.6 宏观数据

- observation period 不等于 release date；
- Massive 返回的历史宏观序列可能包含后续修订；
- 没有 vintage/realtime-start 信息时，只能标记为 revised-history，不得声称严格 point-in-time；
- Barra 核心股票风格模型可以先不依赖宏观数据。

## 12. Ticker events、退市和公司重组

Massive Ticker Events 当前主要解决 ticker change，不能假定覆盖所有：

- 合并；
- 现金收购；
- 破产；
- share-class conversion；
- 退市经济支付。

处理规则：

- ticker change 通常不产生投资收益；
- inactive transition 只表示状态变化，不提供持仓最终 payoff；
- 退市持仓不能永远 forward-fill，也不能默认收益为零；
- 有明确现金/换股条款时按条款处理；
- 条款缺失时使用文档化、保守且可重复的 fallback，并单独报告；
- 新旧证券通过 `asset_events` 连接，不强行共用一个 asset_id。

## 13. VWAP 和执行价格

Minute Aggregate 只有 OHLCV 和 transactions，没有逐笔成交，也没有可用于精确重建的 bar VWAP 字段。

项目约定的 09:30–10:00 执行价必须明确选择：

- 精确/优先方案：针对需要的证券请求 Massive 聚合 VWAP；
- 近似方案：使用 volume-weighted minute close proxy。

近似值字段和图表必须带 `proxy`，不能标记为精确 VWAP。

如果执行窗口没有足够数据：

- 不用未来分钟补齐；
- 不用全天 VWAP 替代；
- 订单保持未成交/现金；
- 单独统计 unavailable execution rate。

## 14. Barra-style 因子特别规则

### 14.1 当前可安全推进的因子

完成复权和身份解析后，可以优先实现：

- Momentum；
- Short/Long Reversal；
- Beta；
- Residual Volatility；
- Downside Risk；
- Amihud Liquidity；
- Seasonality。

### 14.2 暂不能假定可用的因子

- Size：需要经过 filing-time 验证的历史 shares；
- Turnover：需要历史 shares/float；
- Industry：需要 point-in-time SIC 映射和行业版本；
- Value、Growth、Profitability、Quality、Leverage：需要 point-in-time 财务三表；
- Analyst/Sentiment 类：需要对应数据及公开时点。

缺数据时因子应显示 `unavailable`，不能用当前值回填或用 0 代替。

### 14.3 因子输出

所有因子插件保持统一最小输出：

```text
signal_date, asset_id, raw_value
```

另外通过 manifest 保存：

- required fields；
- lookback；
- source data version；
- availability cutoff；
- winsorization、standardization 和 neutralization 参数；
- missing-value policy。

截面去极值、z-score 和行业/规模中性化必须只使用当日 eligible universe。

## 15. 回测时点规则

当前标准流程是：

1. `t` 日收盘后，用 `t` 日收盘前已公开的数据计算信号；
2. 在 `t` 日 universe 上去极值、标准化和排序；
3. 冻结订单；
4. `t+1` 日 09:30–10:00 ET 执行；
5. 未成交订单保留现金，不重新排名；
6. 分别报告 gross 和单边 5/10/20 bps 成本结果。

自动测试至少应证明：

- 因子窗口不读取 `t+1`；
- filing 晚于 cutoff 时不进入信号；
- 拆股和分红没有形成虚假收益；
- 费用只按真实成交和换手计算；
- 退市和无执行价格路径不会静默删除持仓。

## 16. QA 严重级别和处理动作

| 级别 | 示例 | 动作 |
| --- | --- | --- |
| Critical | checksum 错、manifest 不完整、主键重复、未来数据、双重复权 | 停止批次 |
| High | FIGI 冲突、大面积 reference join 失败、日期错位、公司行动不连续 | quarantine 并人工 review |
| Medium | active 无 bar、inactive 有 bar、局部缺分钟、字段稀疏 | 保留并标注，按规则决定资格 |
| Low | 展示名称/描述缺失、非研究字段缺失 | 记录，不阻塞核心处理 |

稳定且应自动化的检查包括：

- manifest 完整性和 checksum；
- schema 和 required columns；
- `(asset_id, timestamp)` 唯一性；
- 时间戳与纽约 session date；
- active/inactive reference 内部一致性；
- identity join coverage；
- 复权连续性；
- `available_at <= factor_cutoff`；
- 输入 manifest 到输出 manifest 的 lineage；
- 同参数重跑 checksum 一致。

不能用固定绝对阈值误伤正常市场变化。行数、ticker 数、成交量和缺失率应结合日期、半日市、IPO、停牌和市场制度变化分段比较。

## 17. 小样本验收集合

全市场转换前，固定保存并手工核算以下案例：

- 正常完整交易日；
- 美股半日市；
- 2:1 split；
- reverse split；
- 普通和特殊现金分红；
- 分钟缺失和停牌；
- ticker change；
- 退市和无明确 payoff；
- 同 ticker 被新证券复用；
- `bars_without_reference`；
- 财报 period end 早于 filing date；
- 09:30–10:00 无法形成执行价格；
- 大小写相近但不同的 Massive ticker。

每个案例应有输入样例、期望输出和自动测试，不只依靠图表目测。

## 18. 存储和运行安全

- 所有数据只写入 `/mnt/HC_Volume_106309665/american_stocks`；
- 不修改挂载盘根目录权限，不接触 Mogikabu 数据；
- 剩余空间低于 60G 预警；
- 预计会让剩余空间低于 40G 的任务拒绝启动；
- Silver 转换前同时估计最终产物和临时峰值空间；
- 不通过删除 Bronze、旧项目或旧 Docker Volume 腾空间；
- 临时输出完成校验后再原子发布；
- schema 或逻辑变化时提高版本，禁止无痕覆盖旧产物。

## 19. 每次处理前检查清单

- [ ] 输入 manifest 全部 complete，checksum 通过；
- [ ] 本次日期范围和交易所 calendar 已冻结；
- [ ] ticker 大小写原样保留；
- [ ] asset identity 规则和 unresolved 数量已记录；
- [ ] universe 来自 signal date，而不是今天；
- [ ] RTH/extended-hours 口径明确；
- [ ] corporate-action 口径明确且未双重复权；
- [ ] 所有非行情字段有 availability date；
- [ ] 当前快照字段没有回填历史；
- [ ] 缺失、停牌和无成交没有被写成 0；
- [ ] t+1 执行失败不会触发重新排名；
- [ ] 输出路径、schema version 和 Git commit 已记录；
- [ ] 临时空间和 40G safety floor 通过；
- [ ] 小样本和自动测试通过后才扩大到全市场。

## 20. 相关文档

- [Massive downloader review guide](massive-downloader.md)
- [Massive non-trade research data catalog](massive-research-catalog.md)
- [Massive Stocks Flat Files](https://massive.com/docs/flat-files/stocks/overview)
- [Massive Stocks REST API](https://massive.com/docs/rest/stocks)
- [Massive Ticker Overview](https://massive.com/docs/rest/stocks/tickers/ticker-overview)
- [OpenFIGI allocation rules](https://www.openfigi.com/assets/local/figi-allocation-rules.pdf)
