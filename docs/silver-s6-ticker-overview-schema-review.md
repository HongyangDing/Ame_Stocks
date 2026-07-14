# S6 Ticker Overview：schema review 与发布证据

## 1. 范围与硬边界

S6 只把已下载的 Massive `ticker_overview` Bronze 转成可审计的 lifecycle 级身份参考证据。
本阶段不重新请求 Massive，不修改 Bronze，不生成永久 `asset_id`、`issuer_id`、ticker 有效区间或
回测股票池，也不启动 S7。

正式表为 `identity/ticker_overview_safe`，一行只代表 S4 daily Assets 派生出的一个既有
identity lifecycle。请求确实把 lifecycle `query_date` 作为 provider 的 `date` 参数发送，但响应是
在 2026-07-11 事后抓取的 retrospective historical query，并没有当日归档 vintage。`active`、
SIC、list date 等字段因此仍只能作为参考证据，不能直接当成当时可获得的 PIT universe、PIT 行业
或历史可交易状态。所有 DATA 行固定
`identity_evidence_scope=evidence_only_pending_s7`、`backtest_identity_eligible=false`。

`market_cap`、`weighted_shares_outstanding`、`share_class_shares_outstanding` 只保留在不可变
Bronze；正式 schema 没有这些列，也不会把未知响应字段自动透传。

## 2. 正式 source profile

正式范围只绑定带显式 `schema=v2` 的 lifecycle、requests 和 safe-oracle 路径；历史遗留的
unversioned 副本不参与 S6。

| 项目 | 结果 |
| --- | ---: |
| lifecycle / request / Bronze response | 30,739 / 30,739 / 30,739 |
| 唯一 `lifecycle_id` | 30,739 |
| 唯一 `source_request_id` | 30,739 |
| 唯一 `(ticker, query_date)` | 30,739 |
| query date 范围 | 2016-07-11 至 2026-07-09，共 2,374 个日期 |
| identity match | 30,570 |
| identity evidence unresolved | 169 |
| SIC code 非空 / 缺失 | 16,682 / 14,057 |
| list date 非空 / 缺失 | 23,417 / 7,322 |
| `list_date > query_date` | 0 |
| failed Bronze request | 0 |
| request ticker / response ticker mismatch | 0 |
| manifest、gzip、stored/raw SHA、JSON 损坏 | 0 |

固定源身份：

- lifecycle manifest：
  `manifests/materialized/ticker_overview_lifecycles/schema=v2/2016-07-11_2026-07-09.json`，
  SHA-256 `62a0cb055b92836e2b8c85d1f9c6c9d87899da9f45fbd5ebe2b9295b20d7785b`；
- lifecycle Parquet：
  `staging/ticker_overview/schema=v2/window=2016-07-11_2026-07-09/lifecycles.parquet`，
  SHA-256 `8288f2c88190d8048fa6687a3ce0ed7aedbac0a62acb1e8028df1e8860dd8544`；
- request CSV：同目录 `requests.csv`，SHA-256
  `c39a6a9a54cd6b181a11d6a4af065760e55656ff7393ab85b41232a5718614a0`；
- provisional safe v2 oracle manifest：
  `manifests/materialized/ticker_overview_safe/schema=v2/2016-07-11_2026-07-09.json`，
  SHA-256 `a0c08afc566cc080704db9454a8c2224d47947e84d92e2eb15cb165fe6b2c9f5`；
- provisional safe v2 oracle Parquet：
  `silver_unadjusted/reference/ticker_overview_safe/schema=v2/window=2016-07-11_2026-07-09/ticker_overview.parquet`，
  3,574,866 bytes，SHA-256
  `0094448bae7e238779ee100d85818ec150b958fb69d3c897058b5b036de159aa`。

全量 profile 重新读取并验证了 30,739 份 manifest 与 gzip，不只信任旧 materialization manifest。
旧 safe v2 只作为逐列 oracle：正式转换必须从 lifecycle + Bronze 重算，并在 profile 中与 oracle
对账；`silver_unadjusted` 不能注册为正式 Silver source，也不能直接改名发布。

## 3. 169 条身份问题的解释与处理

169 条不是 requested ticker 与 returned ticker 冲突，也没有任何可比 CIK/FIGI 值冲突。profile
分别冻结 `identity_conflict_rows=0` 与 `identity_no_comparable_rows=169`，并把 mismatch 主身份类型
锁定为 145 条 share-class FIGI、21 条 CIK、3 条 composite FIGI。它们是
Overview response 缺少 lifecycle 所需的全部可比身份字段：145 条 lifecycle 主身份为
share-class FIGI，21 条为 CIK，3 条为 composite FIGI。ticker fallback 的 728 条全部匹配。

因此 S6 不猜测链接、不降级成 ticker match，也不把 `identity_match=false` 行留在 DATA：

- 30,570 条匹配 lifecycle 进入正式 DATA；
- 169 条各生成一个标准 High quarantine record，issue code 为
  `identity_evidence_unresolved`；
- quarantine 保留 lifecycle、请求和逐文件 lineage，S7 才能结合 S4/S5 证据重新裁决。

## 4. 字段与时点语义

正式输出保留四类字段：

1. lifecycle/request：`lifecycle_id`、query ticker/date、first/last active date、原 lifecycle
   identity type/value；
2. allowlisted response：ticker/name/type/market/locale/active/exchange/currency、CIK/FIGI、SIC、
   list date、delisting/ticker root/suffix；其中 query date 是 provider 历史查询参数，但不是当日
   真实采集 vintage；
3. 保守时点：source capture date/time，以及严格晚于 capture 的第一个 XNYS open 作为
   operational availability；
4. 完整 lineage：request ID、manifest path/SHA、artifact path/stored/raw SHA、page/row ordinal、
   provider request ID 和 result hash。

SIC 和 list date 的缺失保持 null，不从其他 source 填充。SIC 的 research availability 只能跟随本次
Overview capture，不能被描述成 filing-time-safe；`list_date <= query_date` 只是合理性检查，不会把
list date 变成 universe membership 来源。

正式 schema 共 47 列、26 个 QA rules：

- contract ID：`f4e873e6595fee0a66362a0d39b3f7c36176b95354ecad93453613f7ac84ca3c`；
- candidate/resource file SHA-256：
  `d66befb20d8567088555f223e5e49baf73cc3218e5618226d8c6252fab103bca`；
- Arrow schema digest：
  `228404866f33e709fc75e2b50f1ce022602e1b833b84f63315e009a3e07a8643`；
- 主键：`lifecycle_id`；分区：`source_capture_date`；
- 22 个 Critical hard gates、1 个 High warning 和 3 个 Medium warnings。

## 5. Preview / Full / Publish 策略

30,739 个响应都是小型单页 reference payload，正式 preview 直接覆盖完整 scope，样例仍限制为最多
100 行。`projection_multiplier=1.0`，Full 必须在同一 coverage receipt、两个 source inventory、
schema、代码 commit 和参数下重新计算；不需要另建 FullRunPlan。

正式 lineage 使用同一份 immutable coverage receipt 同时绑定：

- `control_manifest` inventory：正式 lifecycle/request plan；
- `bronze` inventory：30,739 个 Overview gzip payload 及其 manifest binding。

因为每个 Bronze payload 恰好有一个 result，正式 BuildIntent 以 30,739 个 Bronze ArtifactRef 作为
直接输入，row funnel 仍是 30,739；该 inventory 同时把 coverage receipt 与 30,739 份原始 Bronze
manifest 声明为 upstream，control inventory 则作为同一 receipt 绑定的辅助 lifecycle 证据。这样
发布后的 workflow trust-chain 重放会重新校验每个原始 manifest 和 payload，而不会把两份 inventory
同时计入并错误得到 61,478 行。

只有完整 source profile、schema、row funnel、所有未 waiver QA、169 条精确 quarantine、preview/full
重算一致和发布后读取验证都通过，workflow 才能进入 `published`。任何新 mismatch、文件损坏、
非预期 warning/quarantine 或未来数据错误都 fail closed。

## 6. 发布证据

运行完成后在此记录 contract、coverage receipt、source inventories、workflow、preview/full、release、
DATA/QA/quarantine SHA、实际运行时间和磁盘变化。

## 7. S7 硬停

S6 发布不解决跨日期 ticker identity churn，也不把 S4/S5/S6 的 raw evidence 自动合并。S7 必须回到
逐步审批模式，单独审查永久 ID、ticker alias interval、issuer/share-class 边界与每日 universe join。
