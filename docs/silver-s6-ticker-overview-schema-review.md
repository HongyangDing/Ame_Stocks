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

2026-07-14 的正式运行已通过上述全部 gate；以下证据只发布 evidence-only S6 行，不改变 S7 的
永久身份审批边界。

## 6. 发布证据

### 6.1 固定输入与控制面

- 运行代码 commit：`9b9841d65d85bcbfe32903c5203e1e14d7aeb9ed`；source profile SHA-256：
  `43d3573579f216695fe1ff8b3a97b9aa6510487476705e6dd850d6a45ea4dc79`；
- coverage receipt ID：
  `01b34fb0f08df51d67ef5124154a2e9026ed5a3621ec060f298440a0ac608a6b`，路径
  `manifests/silver/source-coverage/ticker_overview/coverage-01b34fb0f08df51d67ef5124154a2e9026ed5a3621ec060f298440a0ac608a6b.json`，
  SHA-256 `b771d67e3c0d6139a31766c2b2ffb431292d1d896a4e593a7c100fcaec552ae7`；receipt
  精确绑定 30,739 个 artifact 和 30,739 个原始 manifest ref；
- lifecycle control inventory ID：
  `b566cd78a7d65d9d986edbb3d538b567b03dd1b6efe898b3df994c35f5668076`，路径
  `manifests/silver/source-inventories/ticker_overview/inventory-b566cd78a7d65d9d986edbb3d538b567b03dd1b6efe898b3df994c35f5668076.json`，
  SHA-256 `321dfe2c548609b23a2defa9bb2792c4aa5d2943adc4c96be2e0eaecab5d965a`；
- Bronze inventory ID：
  `5503057d5e575e3827bf53599ee342f7ad6d2d8328cf20a127b08ec5c1fc8c03`，路径
  `manifests/silver/source-inventories/ticker_overview/inventory-5503057d5e575e3827bf53599ee342f7ad6d2d8328cf20a127b08ec5c1fc8c03.json`，
  SHA-256 `822eeaa395e327f11c2b59472619e6b13425ccbf0a16eee5219c664fa50f62e7`；它含
  30,739 个直接 artifact，并以 receipt + 30,739 份原始 manifest 共 30,740 个 upstream
  manifest 固定完整信任链。

### 6.2 Workflow、build、审批与 release

- workflow ID：`bb474b8a62d8d4f316b906ca082197800a3ca4917512fbe6f8e31a0a950a85c6`；
  最终为 `published` sequence 9，event SHA-256
  `aa582e199c21a299a31530175a903179cf79cb10dcf39366f10b864d956ab706`；九个状态按顺序为
  `planned → schema_review → code_ready → preview_ready → awaiting_review → approved_full_run →`
  `full_ready → awaiting_publish → published`；
- preview build ID：`d9d40f14475916a4a83f442281fbfcb85793947da2a071eb033abf012264ed8c`，
  manifest SHA-256 `64cbbca04c0304b0a7979a3f338f6b47810096abb27a8468043b101587028639`；
- full build ID：`f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0`，
  manifest SHA-256 `b616b32bac23124d367dc7e5493130c101f76f222d179b6049c5ac813e1390e0`；
- release ID：`8715f90d0e01f990e9738b9266edfeb2830a76d59a00ae4fb7490d9f077092a5`，路径
  `manifests/silver/releases/release_id=8715f90d0e01f990e9738b9266edfeb2830a76d59a00ae4fb7490d9f077092a5.json`，
  manifest SHA-256 `a830ad88706393db8b28534379538149aa676e254ca87fd9cbb046ce4d2b51fe`；
- schema approval ID `3d34ee6359b84b178cae6f30fb287e8f0c8b05e4a95372bd21a4319d7a3b1642`
  （文件 SHA-256 `e103413d341ef5e907ff0e946fdfbcc5f6edf88e4fa022c785f7ae55fbb06261`）；
  full approval ID `112d1949e874843e4b655f16842ddd87077b787ff2b3de0bfcb71f2b2319d47f`
  （文件 SHA-256 `301baeda7b0e37a1e9649fa1e5525335d0cb44d15706b77c5d86c45e2e368fc9`）；
  publish approval ID `0c8fb0d498f026bd91e8198884b6270302159bdca0da59e6eb67c79eb7e4b53f`
  （文件 SHA-256 `11cef601470b86d0c8673a239b303becb4eb6324aff81a040ff056f55d3f3a2f`）。
  后两份审批各自只接受其 subject build 绑定的 4 个 warning result ID 和 169 个 quarantine issue
  ID；preview/full 的语义集合与数量通过 parity，但 build-bound ID 本身不相同。

### 6.3 Row funnel、QA 与正式产物

正式 funnel 为 `30,739 input → 30,570 accepted DATA + 169 quarantined`，精确重复、unmapped 和
version-preserved 都为 0。26 个 QA 中 22 个 Critical 检查通过；只有以下四项按精确分子/分母
waive：

| warning | 严重度 | 分子 / 分母 |
| --- | --- | ---: |
| `unresolved_identity_rows` | High | 169 / 30,739 |
| `sic_code_missing_rows` | Medium | 14,057 / 30,739 |
| `list_date_missing_rows` | Medium | 7,322 / 30,739 |
| `retrospective_query_without_archived_vintage_rows` | Medium | 30,570 / 30,570 |

169 条 quarantine 均为 `identity_evidence_unresolved`、High、`review_status=pending`；接受这些
issue 只授权本次 evidence-only publication，不表示身份已经解决。

| 角色 | 路径 | 行数 | bytes | SHA-256 |
| --- | --- | ---: | ---: | --- |
| DATA | `silver/schema=v1/identity/ticker_overview_safe/build_id=f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0/data/source_capture_date=2026-07-11/part-00000.parquet` | 30,570 | 13,197,588 | `807de48ccf3d9dec6e461e32ab12a87f73dc15bb5c00297f6cf1244bb4f73767` |
| QA | `silver/schema=v1/identity/ticker_overview_safe/build_id=f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0/qa/qa-check-result.parquet` | 26 | 5,122 | `87986239c6f254dd18773aa249c23208c6faf9f94dbac8f2bdc4109c5350ceab` |
| quarantine | `silver/schema=v1/identity/ticker_overview_safe/build_id=f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0/quarantine/quarantine-record.parquet` | 169 | 20,063 | `b12b8bae3b154f31a2e7ca46010db4dcdef72004d6151e443729116f74fe9b05` |

Preview DATA 位于
`staging/silver/ticker_overview_safe/build_id=d9d40f14475916a4a83f442281fbfcb85793947da2a071eb033abf012264ed8c/data/source_capture_date=2026-07-11/part-00000.parquet`，
大小和 SHA-256 与 Full DATA 完全相同；row funnel、QA 指标、quarantine 语义行和逐行 DATA 的
独立重算 parity 全部通过。QA/quarantine 文件本身含各自 build ID，因此 preview 与 full 的文件
SHA 不要求相同。

### 6.4 发布后独立审计与资源

生命周期命令 exit 0：wall time 22:48.20，user/system time 1252.61/31.78 秒，平均 CPU 93%，
最大 RSS 1,050,288 KiB（约 1.00 GiB），0 swap，未触发 2 GiB 上限。发布后又以固定的
workflow/build/release ID 独立执行 `verify_workflow_trust_chain(..., verify_artifacts=True)` 和
`PublishedSilverReader` 重放；审计 exit 0，wall time 4:09.57，最大 RSS 631,264 KiB。

独立审计确认：正式表 47 列、30,570 个唯一 lifecycle；`identity_match=true`、
`backtest_identity_eligible=false`、`identity_evidence_scope=evidence_only_pending_s7`；三项不安全
市值/股本字段不存在；169 条 quarantine 与四个 warning 精确一致；未发现任何 S7 table、workflow、
build 或 release 路径。

本次新增 34 个 S6 控制面/preview/full/release 文件，共 160,219,242 bytes（约 152.8 MiB）。数据盘
`df -h` 运行前后均为 78G used、109G available、42%；运行后精确 available 为
116,022,562,816 bytes。运行前记录的两棵 Massive source tree 最大文件 mtime 为最后一份 request
manifest 的 `1783793890433013161` ns，运行后完全相同；最后一份 raw page 仍为
`1783793890416013103` ns，且 30,739 份 manifest/payload SHA 均经信任链重验。

## 7. S7 硬停

S6 发布不解决跨日期 ticker identity churn，也不把 S4/S5/S6 的 raw evidence 自动合并。S7 必须回到
逐步审批模式，单独审查永久 ID、ticker alias interval、issuer/share-class 边界与每日 universe join。
当前检查点为：**S6 complete；S7 planned / not started**。S6 的连续完成授权不包含任何 S7 操作；
S7 必须重新从只读 combined-source profile 与 schema proposal 开始，得到显式批准后才可写代码和
fixture，再按 bounded preview → review → 单独批准 full/publish 的节奏推进。
