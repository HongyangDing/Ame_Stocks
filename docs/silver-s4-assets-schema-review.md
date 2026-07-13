# Silver S4 `assets` source profile, approved schemas, and bounded-preview evidence

## 1. 当前状态与硬边界

2026-07-13，S4 的三个独立 workflow 已进入 **Phase 2 / `awaiting_review`（sequence 5）**。本检查点完成：

- 对十年 active/inactive Assets Bronze 做全量只读 streaming profile；
- 核对字段、类型、空值、时间关系、每日 active/inactive 互斥、重复版本和身份冲突；
- 冻结并逐字批准三张目标表的 grain、字段、键、PIT 限制、选择规则和 QA；
- 将 candidate 逐字节封装为 package resources，并登记三个 remote schema-v1 workflow；
- 把同一条用户批准原文绑定为三个 immutable schema receipts，零 QA waiver、零 accepted
  quarantine issue；
- 实现 manifest-bound source reader、按 session 有界的纯转换及 fail-closed synthetic fixtures；
- 在单独授权下处理 2026-05-11 的完整 active/inactive request pair，创建一个共享 SourceInventory、
  三个不可变 preview builds 和完整 staging evidence；
- 精确幂等重放 sequence-5 checkpoint，确认 build/event 不变后停下等待人工 review。

本检查点**没有**：

- 运行十年 full build、创建或批准 `FullRunPlan`、请求 publish 或创建 S4 release；
- 写入或修改 Bronze、正式 Silver 数据路径；
- 调用 Massive/API 或下载任何新数据；
- 修改旧 materializer、Docker、Caddy 或 Mogikabu。

远程 registry 对同一 `domain/table/schema-v1` 的 contract digest 是不可变的。三个批准版本现已由
Git candidate bytes、package resource bytes、registry document SHA 和 workflow chain 四重绑定；
任何字段、顺序、类型、nullability、key、QA 或描述变更都必须重新 review 并升 schema version。

| Approved contract source | 字段 | QA | `contract_id` | Arrow schema digest |
| --- | ---: | ---: | --- | --- |
| [`identity/asset_observation_daily`](silver/contracts/identity/asset_observation_daily.schema-v1.candidate.json) | 32 | 35 | `dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045` | `402d0ea624dc26e43ea63974572ede5a46ae20e0741e97a3d01d07075a71bc1e` |
| [`identity/asset_observation_version`](silver/contracts/identity/asset_observation_version.schema-v1.candidate.json) | 24 | 25 | `14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc` | `4c797ca373d697078b2061b9a76696dc036a1d2db0a5f8e1fe3ce2dac4b6bb4b` |
| [`reference/universe_source_daily`](silver/contracts/reference/universe_source_daily.schema-v1.candidate.json) | 38 | 31 | `9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58` | `78b799cd5a2621b5a78e4ed8c23c090f6aea686fcd786366e5c258e81ad278a5` |

批准原文的 UTF-8 SHA-256 为
`74895ce20e9e82415e9381e47583ba7963414049cbbb17875ce371d723330e01`，无尾随换行。运行代码固定在
Git commit `cf0a9d1cdc83f41475be16fa3d79e5b26269f279`；本地、GitHub 与
`/opt/american_stocks` 当时均为该 SHA 且 worktree clean。

后续 bounded preview 由 Git commit `35797a59836cd3220634cca0dad048d816aca7ed` 运行；该 commit
实现独立 `FullRunPlan` review/approval gate，保证单日 preview 不能隐式授权更大的十年 scope。

### 1.1 远程 registry 与批准证据

- `asset_observation_daily`：workflow
  `c1bae241ed90e49aed1ae8a98b6801f511d6abaac2cef93c66ccba59d33775ec`；schema-review event
  `84749ab1a7a1cac80b636dbb4be9fb58af8ce22e2b34656044d7f34ed848d5cd`；registry document
  `2efd0476eb15b2d39ef0317607a21de5e08551e6c49062c47ca0264e18f2eb24`；approval ID
  `ad9718d73d0918ac1152480d677b00f02b9effa0a113d373bc4e78daf98331ce`；receipt SHA
  `a1ed01b12b84ec7b35497adeb2b1ebb3c96b8f0e5b67f1e4aef6b3e4bed26041`；code-ready event
  `5c74b31676c709e6d9455da0c8ef8ec76fb4337754c2bc08c613be7dd9d89ef3`。
- `asset_observation_version`：workflow
  `989c8c513905e2710714c0b6f94352119e8fb1128147d8c2db9486c1e03df6da`；schema-review event
  `c3ff6ef36cc5533bf6838912ee25aac0d9fa30ffc0bda3fbc0b387e90e027911`；registry document
  `d093c894983436c58b512edbf9e7a63d28cba50ad2c07a34bf95b9a492345b1e`；approval ID
  `a95e0377258d6ee9aa6e683ddf6a7c941473fe90c9a6dcc3d75db806aacc9915`；receipt SHA
  `c73b8baad721a8c197050bf7d79b559688745814c0fdea9d0591134515ca0744`；code-ready event
  `3655311e84140d523af72e2ac7bcc9e4602c135f8292f7548111fcc186c7b9b2`。
- `universe_source_daily`：workflow
  `918ebc04d2eded87243387804d58fa9f24e4282ee27a8a26ac6ac22f4390b755`；schema-review event
  `57f357d158dd9856d0fda46262dee70308d7b9b30f0ce864954fc62c83703dbb`；registry document
  `141c947595569ddebbbda3a21c9826055d3aed6c69c62fe2e825512a6607adeb`；approval ID
  `488f8b56c6d3f7360c62008b846b29fe49ff1712babe4aad93a3679aedff3e28`；receipt SHA
  `ecb580c682e032358bb7b05e21b80db58c0aacb88e5018dd12ca8f3568d68077`；code-ready event
  `d3ac371c080fb9f7317dbc66e7ae0673875d08b66826d13b063847d73a297067`。

三个 workflow 的 exact approval command 已重放，receipt、event path/SHA 与 sequence 均未变化。
以上是 preview 前的 code-ready checkpoint；随后只有这三条 S4 chain 增加 preview-ready 和
awaiting-review 两个 event。S1、S2 和两个 S3 workflow 保持 `published` sequence 9。

### 1.2 2026-05-11 bounded preview 运行证据

精确输入为 active request
`9e1ab3e3c1d4c09ea91e346c8eaeaf07279b698b1f1d8ae14c6437992b1b15ff` 的 13 pages / 12,582 rows
与 inactive request
`f7c3f67c5966c307f470ff7468af78fb7848d83b7d5f2e25e7cda1d36dfaf90f` 的 24 pages / 23,065 rows。
共享 SourceInventory：

- inventory ID `d61a9eb9ff52f721f61e931cdf0ec3460b1f361e619b8f731b13562f875adc25`；
- document SHA-256 `8d5ebdf262dacc1549f2671d348295172869cb7c86a08bcb1b0301a29b34407f`；
- 37 artifacts / 35,647 rows；Git commit `35797a59836cd3220634cca0dad048d816aca7ed`；
- upstream lineage 精确包含 active/inactive 两个 Bronze manifests，以及 S1 exchanges release
  `feab0e1f32a5685d1115a6e4e87aab8ff50c18b99c6336a8790ecba44464d838`（manifest SHA
  `d8789e6cf760ffb6274077736c18e37bd69330139ea1c6ecf2f420bb56f93f07`）和 S2 ticker-types release
  `11a62f9c06ea5c609c159a7d619ba94cabbe39d3b07518fec279fa4758c882f6`（manifest SHA
  `5568a905bb1cdfe791a300f5b12fdd1e2041e3e1c1aacfbf6cc78f4890b95f47`）。

| Table | Build / manifest SHA-256 | Row funnel | QA / fixed cases | Awaiting-review event |
| --- | --- | --- | --- | --- |
| `asset_observation_daily` | `baaf04a909973984f51eaaeccfd3e2408763acd6aa76403cdf62017edd0422ba` / `5ce4d35c06cfd1ed87e0f847baa2f6d7a95258ddee7b8c913c0a3f5791a11a58` | 35,647 input = accepted = output；unmapped 0；version preserved 82 | 35 checks；6 Medium warnings；0 blocking；0 quarantine；3 fixed cases | `4d172aa12ff368e0dd42f77df83eeeadcba6c51a800baac10ab4fdda11e7e53c` |
| `asset_observation_version` | `1c560bbaffbb7a838fbcbccf90d0da83e4c69f2866515bf860f0c05eb1406e8f` / `fced8a5bb82ed0ab6e0850ed7680397709e78d5d47c58b097309977adf547f65` | 35,647 input = accepted；82 output；35,565 unmapped；version preserved 82 | 25 checks；2 Medium warnings；0 blocking；0 quarantine；2 fixed cases | `b0fe4549477f079fb92f75cc05732baa5a7de04820c40bfca659c37a7b195c47` |
| `universe_source_daily` | `442ac3894e68e14332621b73de6b4eb83e362c549328223c57b63f80828dc755` / `ef502a1d759b58017411a6686b23d0376a741566950a54aa7a7da5a7272d8b65` | 35,647 input = accepted；35,606 output；41 unmapped；version preserved 82 | 31 checks；7 Medium warnings；0 blocking；0 quarantine；3 fixed cases | `d9d993eafa729de1f88b785ee1752f0144e7a3a5ebb6f9fc082a0e611c564b76` |

Warning 是保留供 review 的真实 provider/identity diagnostics，不是损坏或阻断：

- observation：123 casefold collision groups、1/15 current type dictionary miss、1 exact duplicate
  occurrence diagnostic、507 inactive-without-delisted rows、12,590 metadata-after-session rows和
  1 optional-string whitespace row；
- version：41 groups 中 31 组改变 `delisted_utc`，1 组是 exact duplicate；
- universe：复用上述 casefold/type/delisting/metadata diagnostics，另有 3,571 rows 缺少稳定 identity
  evidence、1,064 composite-FIGI multi-ticker groups 和 1,126 share-class-FIGI multi-ticker groups。

`exact_duplicate_excess_rows=1` 是“发现一个额外 occurrence”的诊断；v1 不删除它，而是保留在
observation/version evidence，所以正式 row funnel 的 `exact_duplicate_excess=0`。三张表的 Critical/
High checks 全部通过。共享 transform elapsed 24.587s，Python traced peak 146,672,215 bytes，进程
最大 RSS 约 617 MB；三份 DATA Parquet 分别为 4,320,422、22,384 和 3,964,738 bytes。Build 目录
无遗留 `.tmp-*`，trust-chain artifact 校验全部通过。

用相同 inputs、Git commit 和三个 sequence-5 event SHA 再次运行后正常退出，build ID、manifest
SHA、event SHA 和 sequence 全部不变。三个表均没有 full build、正式 `silver/<table>` 目录或 S4
release；数据盘仍约有 124G 可用。

Approved resource loader、只读 source reader 与纯转换分别位于
[`asset_contract.py`](../backend/ame_stocks_api/silver/asset_contract.py)、
[`asset_source.py`](../backend/ame_stocks_api/silver/asset_source.py) 和
[`assets.py`](../backend/ame_stocks_api/silver/assets.py)。Schema-review 与 approval-only CLI 分别为
[`silver_assets_schema_review.py`](../backend/ame_stocks_api/cli/silver_assets_schema_review.py) 和
[`silver_assets_schema_approval.py`](../backend/ame_stocks_api/cli/silver_assets_schema_approval.py)。完整测试集、
Ruff 和 Git diff check 均通过；approval CLI 不提供 SourceInventory、preview、build、release 或 publish
参数。

## 2. 权威 source scope 与只读方法

数据根：`/mnt/HC_Volume_106309665/american_stocks`

### 2.1 已有物理完整性证据

| 证据 | 路径 / digest | 结论 |
| --- | --- | --- |
| Bronze full audit v9 | `manifests/audits/bronze/full-2026-07-12-v9.json`；SHA-256 `a23fdd2aa4c613274dfe0dcca611e8ed1bd62153146f787ecd415c345c1a15d6` | authoritative plan 与 physical integrity passed；逐 manifest/page 校验 bytes、stored/raw SHA、gzip、JSON 和 row count |
| Assets duplicate audit | `manifests/audits/assets/duplicate-versions-2026-07-12.json`；SHA-256 `bf5abe8e8bde1671b69c2d1e0546212fa5b99189e660cf2cef8f0936000d3641` | 发现 4,853 个同日 inactive duplicate groups；本次 profile 进一步修正其中两个 exact groups 的分类 |
| manifest inventory | 5,026 entries；digest `43da9c7cd2adc2a69e1badffb947807e5db04b45a627619765986b7d85bc1853` | 2,513 个 session × active=true/false 两请求 |
| artifact inventory | 72,038 entries；digest `3a019c3a1568d16dc873bff79010b5afcbeff490779215abddb75599e7c0f11b` | manifest-declared gzip pages；约 2.531 GB gzip / 19.187 GB raw JSON |
| versioned profile summary | [`assets-full-2026-07-13.json`](silver/source-profiles/assets-full-2026-07-13.json)；file SHA-256 `5d813c13d6e79c8da43d230b223b19e3d6aebb9846f865be1236e4299e6e48a6` | 机器可读字段/null/type、hard-gate numerator、duplicate funnel、time、case 与 identity 统计 |

### 2.2 本次 full streaming profile

本次另行逐页、逐行只读扫描全部 5,026 manifests、72,038 gzip pages 和 **69,381,182 rows**：

- 4 个 worker 仅以 read mode 打开 manifest/page；无临时输出、无文件写模式、无数据根变更；
- 每页检查 envelope、结果数组、request ID 与 manifest page row count；
- 按 exact case-sensitive ticker 统计每日 active/inactive、duplicate、identity 和 casefold 关系；
- 对 13 个 provider 字段统计 presence、explicit null、empty、native type 与时间戳可解析性；
- 对 S1/S2 published current-only reference 只做 coverage diagnostic，不做 enrichment；
- elapsed `1104.032s`，69,381,182/69,381,182 rows 与 72,038/72,038 pages 完成，进程正常退出；
- 原始运行汇总只写 stdout，未在数据根生成 profile artifact；review 后将完整聚合值转录为上面的
  versioned machine-readable summary，并对该 Git 文件计算真实 SHA。它不是伪装成原 stdout bytes
  的 digest；可复算边界仍由两个 inventory digest、Bronze audit digest 和只读 profiler 固定。

本次 profile 与 v9 的物理完整性职责不同：v9 负责逐文件 checksum；profile 在已验证输入上负责
schema/domain/relationship 统计。两份证据都必须通过，不能用 profile 替代 checksum audit。

可复算只读实现：

- streaming profiler：
  [`asset_source_profile.py`](../backend/ame_stocks_api/silver/asset_source_profile.py)；
- stdout-only CLI：
  [`silver_asset_source_profile.py`](../backend/ame_stocks_api/cli/silver_asset_source_profile.py)；
- multi-worker reducer / no-write fixture：
  [`test_silver_asset_source_profile.py`](../tests/test_silver_asset_source_profile.py)。

```bash
/opt/american_stocks/.venv/bin/python \
  -m ame_stocks_api.cli.silver_asset_source_profile \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --workers 4
```

CLI 对 manifest/page 的 bytes、stored/raw SHA、envelope、count、field profile、duplicate selection、
domain distinct、case/identity map 和 inventory definition 一次 streaming 输出 canonical JSON 及
`profile_sha256`；不创建 cache、temp、inventory 或数据根 report。Fixture 证明 `workers=1` 与 `2`
产生 byte-equivalent logical report，并专门覆盖 exchange/type distinct-set merge。

权威时间范围与运行 envelope 也完整对账：

- 2,513 sessions，从 2016-07-11 到 2026-07-09；每个 session 精确一对 active/inactive requests；
- manifest status：5,026 complete、0 failed、0 in-progress；
- active：25,630,067 rows / 27,014 pages；inactive：43,751,115 rows / 45,024 pages；
- manifest `created_at` 从 `2026-07-11T12:29:41.671172Z` 到
  `2026-07-11T16:23:34.452338Z`；
- `completed_at` 从 `2026-07-11T12:29:46.302322Z` 到
  `2026-07-11T16:23:47.148540Z`；
- page `results/count/status/provider request_id` 与 manifest row count mismatch 全为 0。

## 3. Massive `date` / `active` 的真实语义

Massive [`GET /v3/reference/tickers`](https://massive.com/docs/rest/stocks/tickers/all-tickers) 的 `date`
参数用于“取得该日可用的 tickers”，`active` 表示 ticker 在查询日是否 actively traded。当前 provider
实际发送：

```text
date=<session_date>
active=true | false
limit=1000
locale=us
market=stocks
sort=ticker
order=asc
```

因此 `session_date` 与 S1/S2 的本地 download label 不同：它确实被发送给 provider，可作为
**provider reconstructed historical membership effective date**。但这些历史日是在 2026 年回溯下载，
不是当时归档的 response vintage。v1 必须同时保留两个时间轴：

1. `session_date`：provider 历史 date query 的 membership effective date；
2. `source_capture_at_utc`：本项目真实取得该 response 的 manifest `completed_at`；
3. `source_available_*`：严格晚于 capture 的首个 XNYS open，只描述本地 operational ingestion；
4. `last_updated_at_utc`：provider metadata revision evidence，绝不替代 membership 或 research
   availability。

固定 scope marker：

```text
reference_time_scope = provider_historical_date_membership_snapshot_v1
metadata_time_scope = metadata_as_returned_at_source_capture_not_historical_vintage_v1
source_availability_quality = reconstructed_historical_snapshot_without_archived_vintage
```

回测可以用 `active_on_date` 构造 vendor-reconstructed historical universe，从而降低只用今天 active
ticker 的 survivorship bias；但报告必须披露它不是历史时点归档 vintage。`name/type/exchange/FIGI/CIK`
等描述字段不能仅因出现在历史 date response 中就被宣称为当日已知元数据。

## 4. 全量字段 profile

active rows 为 25,630,067，inactive rows 为 43,751,115，合计 69,381,182。13 个 provider 字段
没有 native-type 混杂；所有 present value 都是下表类型，explicit null 与 empty string 均为 0。

| Provider field | Present | Missing key | Native type | Silver 处理 |
| --- | ---: | ---: | --- | --- |
| `ticker` | 69,381,182 | 0 | string | non-null；原大小写，不 trim/uppercase/casefold |
| `active` | 69,381,182 | 0 | boolean | non-null；必须与 request active flag 相等 |
| `market` | 69,381,182 | 0 | string | nullable forward-compatible；当前全为 `stocks` |
| `locale` | 69,381,182 | 0 | string | nullable forward-compatible；当前全为 `us` |
| `currency_name` | 69,381,182 | 0 | string | nullable forward-compatible；当前全为 `usd` |
| `last_updated_utc` | 69,381,182 | 0 | string | 保留 raw + strict parsed UTC；不是 availability |
| `name` | 69,353,805 | 27,377 | string | nullable；原值保留 |
| `primary_exchange` | 58,457,063 | 10,924,119 | string | nullable；重命名 `primary_exchange_mic`，不补值 |
| `type` | 51,276,110 | 18,105,072 | string | nullable；重命名 `type_code`，不粗分类 |
| `cik` | 58,408,707 | 10,972,475 | string | nullable；不补零、不当 share-class key |
| `composite_figi` | 25,704,384 | 43,676,798 | string | nullable identity evidence |
| `share_class_figi` | 24,773,728 | 44,607,454 | string | nullable identity evidence |
| `delisted_utc` | 43,134,820 | 26,246,362 | string | 保留 raw + strict parsed UTC；不推断 missing date |

当前全量完整不代表 provider 永远保证这些 optional field non-null。除 `ticker` 和已验证的 native
`active` 外，业务字段保持 nullable，让后续新增 session 不必因合法 optional-key absence 升 schema。
present 但类型错误、非法时间或必填字段不可用仍由 Critical/High QA 阻断。

本次所有结构/类型 hard-gate numerator 均已显式重算为 0：

| Gate | Numerator |
| --- | ---: |
| manifest structural/status issue | 0 |
| page `results` not list / non-OK status / missing provider request ID | 0 |
| page envelope count / manifest record count mismatch | 0 |
| required ticker missing/empty/whitespace | 0 |
| provider `active` native-type or request-flag mismatch | 0 |
| active/inactive same-day exact ticker overlap | 0 |
| unexpected provider result-object field | 0 |
| present optional field wrong native type | 0 |
| `market != stocks` / `locale != us` / `currency_name != usd` | 0 / 0 / 0 |
| invalid `last_updated_utc` / `delisted_utc` timestamp | 0 / 0 |
| `last_updated_at_utc` / `delisted_at_utc` after source capture | 0 / 0 |
| explicit JSON null / empty string across all reviewed fields | 0 / 0 |

`name_trim_mismatch=1,913` 是唯一 whitespace 内容诊断；它不是结构损坏，原值保留并产生 Medium
warning。后续机器 profile summary 必须继续输出这些零值，避免只展示 headline 后遗漏 fail gate。

### 4.1 Domain 与 current-reference diagnostic

- `market=stocks`、`locale=us`、`currency_name=usd` 各 69,381,182/69,381,182；
- 非空 exchange 只有 `ARCX, BATS, IEXG, XASE, XBOS, XNAS, XNYS`；与当前 S1 published MIC 的
  coverage 为 58,457,063/58,457,063 = 100%；
- 非空 `type` 有 15 个 code；当前 S2 覆盖 50,087,233/51,276,110 = 97.6814%；唯一 unmatched
  code 为 `INDEX`，共 1,188,877 rows；
- 18,105,072 rows 的 `type` absent；不能把 missing 或 `INDEX` 映射为 `OTHER`，也不能删除。

S1/S2 是 2026 年捕获的 current-only dictionary，晚于全部 S4 session（S4 截止 2026-07-09）。这些
coverage 只用于检查 provider code spelling；不能把 current label 回填为过去的 PIT 分类，更不能据此
决定 common-stock/ETF eligibility。

### 4.2 时间关系

`last_updated_utc` 69,381,182 个值全部可解析，且都不晚于实际 source capture：

| 相对 `session_date` | Rows |
| --- | ---: |
| after session | 61,106,281 |
| same calendar date | 23,471 |
| before session | 8,251,430 |

约 88% 的 row metadata 更新时间晚于 query session，直接证明它不能作为 query-date 可用时间。

`delisted_utc` present values 43,134,820 个全部可解析且 `<= session_date`：before 43,121,748、
same date 13,072、after 0。所有 active rows 都没有 `delisted_utc`；inactive rows 中 43,134,820
present、616,295 missing。缺失不等于“没有退市”，因此保留 inactive membership 并产生 warning，
不凭最后出现日制造 delisting date。

### 4.3 Case 与 whitespace

- 含 lowercase 的 ticker observations：7,456,564；
- 同日 casefold collision：240,771 group-instances，涉及 126 个 distinct casefold keys；
- `name` 有 1,913 个 leading/trailing-whitespace observations；原值保留并 warning；
- ticker 不能复用旧 materializer 的 `.strip()`，更不能 uppercase。任何 ticker whitespace 都保留在
  observation evidence，但以 High QA 阻止进入 source universe。

## 5. Duplicate version profile 与选择规则

分组键固定为 `(session_date, requested_active, exact ticker)`。全量结果：

- 4,853 duplicate groups / 4,853 duplicate excess；每组严格两行；
- 9,706 source rows 将进入 `asset_observation_version`；singleton 不复制进版本表；
- 全部 duplicate groups 来自 `requested_active=false`；active duplicate groups 为 0；
- active/inactive same-day exact ticker overlap 为 0；
- 2 groups 的 canonical provider result objects 完全相同；
- 2,115 groups 只差 `last_updated_utc`；
- 2,736 groups 只差 `delisted_utc` 与 `last_updated_utc`；
- duplicate identity fields 无同日 FIGI/CIK/share-FIGI 冲突。

旧 duplicate audit 将前两个 exact groups 合并在 2,117 个 `last_updated` bucket 中；本次逐 raw-row
重算把它修正为 **2 exact + 2,115 last-updated-only**。总 group/excess、受影响 session 和 Bronze
完整性结论不变；Silver 使用本次更精细分类。

选择规则 `s4_asset_source_version_selection_v1`：

1. canonical-JSON-equivalent provider result object 才允许按稳定 source pointer 选一个物理
   occurrence；两个 occurrence 都保留在 version table，row funnel 记录 exact excess；
2. payload 不同前，先要求 `active,ticker,type,name,market,locale,primary_exchange,currency_name,cik,
   composite_figi,share_class_figi` 的 exact identity signature 一致；
3. 语义版本必须每行 `last_updated_utc` 可解析并有唯一最大值，才选择该最大值；
4. `delisted_utc` 只作为差异证据，不使用“日期越晚越正确”的排序；
5. 最大更新时间并列、身份字段冲突、未 review 的 difference-field set 或时间证据不足时，整组
   `unresolved`，不生成 universe row；
6. row hash 只验证 exact payload / 稳定 source occurrence，绝不能替语义冲突决定 winner。

合同 digest 同时绑定 exact status domain：resolved 只有 `resolved_exact_duplicate` 与
`resolved_unique_latest_last_updated`；unresolved 只有 `unresolved_identity_conflict`、
`unresolved_timestamp_missing_or_invalid`、`unresolved_timestamp_tie`、
`unresolved_difference_set`。Universe 只接受 `singleton` 和上述两个 resolved status。

本次真实 group selection profile 为：

- 4,851 个非 exact groups 的全部 `last_updated_utc` 都可解析，且组内两值不同；unique maximum
  4,851/4,851，可按 provider latest revision 选择；
- 2 个 exact groups 的 timestamp 和 canonical provider result object 都相同；只按最小 `(page_sequence,
  source_row_ordinal)` 选择物理 occurrence；
- identity-field conflict 0、非 exact timestamp tie 0、当前 unresolved group 0。

两个 exact duplicate 都位于 1,000-row pagination boundary：2026-01-20 的一组跨 page 16/17，
2026-05-11 的一组跨 page 9/10。这证明 `source_record_id` 必须包含 page/ordinal：canonical row hash
可以证明两行相同，却不能唯一定位两个物理 occurrence。即使当前全可解析，合同仍保留 fail-closed
unresolved 分支，避免未来 source drift 被静默覆盖。

## 6. Identity profile：为什么 S4 不生成 provisional `asset_id`

同一 session + exact ticker 的多 `composite_figi`、多 `share_class_figi`、多 CIK 均为 0；但反向
关系并不唯一：

| Relationship | 全量 group-instances / distinct key |
| --- | ---: |
| same-session Composite FIGI → multiple tickers | 1,397,034 / 1,652 FIGIs |
| same-session Share-class FIGI → multiple tickers | 1,569,364 / 1,649 FIGIs |
| full-history ticker → multiple Composite FIGIs | 2,199 tickers |
| full-history ticker → multiple CIKs | 3,345 tickers |
| full-history ticker → multiple Share-class FIGIs | 523 tickers |
| full-history Composite FIGI → multiple tickers | 1,692 FIGIs |
| full-history Share-class FIGI → multiple tickers | 1,678 FIGIs |
| full-history CIK → multiple Composite FIGIs | 2,647 CIKs |
| full-history CIK → multiple tickers | 5,966 CIKs |

因此原计划中“`asset_id` 可暂为 provisional”与真实数据发生冲突：仅用 Composite FIGI 会在大量
同日 active/inactive alias/lifecycle 行上产生相同 provisional ID，仅用 ticker 会跨生命周期误合并，
CIK 更是 issuer 而非 security key。为了方便后续正确处理，v1 **不生成 `candidate_asset_id` 或
`asset_id`**；只保留 raw identity evidence 和 `identity_link_status`。S5 Ticker Events、S6 Overview
完成后由 S7 结合有效区间生成永久 identity。

`identity_link_status` 也不是自由文本：根据 selected row 中 Composite FIGI、Share-class FIGI、CIK
三个字段的非空数量，精确取 `multi_identifier_evidence_pending_s7`、
`single_identifier_evidence_pending_s7` 或 `insufficient_identity_evidence_pending_s7`。

这是一项有意偏离初始草案的决定：少一个看似方便但会双计/误合并的 ID，比在 S7 修复已经进入
回测的错误 identity 更适合量化下游。

## 7. 三张 approved contract

### 7.1 `identity.asset_observation_daily`

- grain：一个 manifest-bound provider result object；不按 ticker 去重；
- primary key：`(session_date, source_record_id)`；
- partition：`(session_year, session_date)`；每个交易日形成独立物理 partition；
- sort：`session_date, ticker, requested_active, source_page_sequence, source_row_ordinal`；
- 当前预期行数 `O = 69,381,182`；
- 保留 request/provider active、全部 provider 字段、raw/parsed timestamp、双时间 scope、capture /
  operational availability 和完整 row lineage。

这是 lossless semantic staging：重复版本不丢，非法结构才 quarantine。新的 reader 会逐 manifest/page
验证并流式产出 source records，纯转换每次只物化一个完整 session pair，不把十年 69M rows 一次性
放入内存。1.2 已记录 bounded preview 的真实 Parquet 体积、24.587s transform、traced peak 和 RSS；
十年 full 的分区级峰值与累计体积仍必须通过单独 `FullRunPlan` 外推并审批。

### 7.2 `identity.asset_observation_version`

- grain：`group_size > 1` 的每个 source observation member；
- primary key：`(session_date, version_group_id, source_record_id)`；
- partition：`(session_year, session_date)`；
- 当前预期 `V = 9,706` rows，不复制 69,371,476 个 singleton rows；
- 保存 exact identity signature、difference fields、last-updated/delisted evidence、rank/status/reason、
  selected ID 和 parent lineage。

### 7.3 `reference.universe_source_daily`

- grain：active/inactive 完整配对并完成版本选择后，每 `(session_date, exact ticker)` 一行；
- primary key：`(session_date, ticker)`；
- partition：`(session_year, session_date)`；
- active 和 inactive 都保留，研究代码必须显式筛 `active_on_date=true`；
- 不生成永久或 provisional asset ID，不做 eligibility；
- 每行保存 `active_source_request_id`、`inactive_source_request_id` 与二者连同 session 计算的
  `source_pair_id`，因此 pair completion 不依赖猜测 selected-row lineage；
- 每日完整可用时间使用
  `max(active_manifest.completed_at, inactive_manifest.completed_at)`，不能只继承 selected row；
- 若当前所有 version group resolved，预期
  `U = O - Σ(group_size - 1) = 69,381,182 - 4,853 = 69,376,329`。

三表 cross-contract funnel：

```text
O = accepted asset_observation_daily rows
E = Σ(version_count - 1) = 4,853
V = Σ(version_count where version_count > 1) = 9,706
U = distinct exact (session_date, ticker) = O - E
```

任一 parent coverage、selection count 或公式不相等都为 Critical failure。

## 8. QA、quarantine 与不允许的自动修复

三份 approved schema 的精确 QA 列表在 JSON 中冻结。关键 Critical/High gate 包括：

- authoritative request plan、manifest/page/hash/count/envelope/pagination；
- 每个 XNYS session 精确一对 active=true/false complete requests；
- request date、calendar coverage、active snapshot non-empty；
- provider native Boolean `active == requested_active`；
- exact ticker active/inactive overlap = 0；
- optional field native type、provider market/locale scope 与所有 parsed source timestamps
  `<= source_capture_at_utc`；
- 未 review provider field 为 High failure，不能只靠 row hash 后丢弃新字段仍声称 lossless；
- schema、PK、lineage、availability、row funnel；
- duplicate projection、difference fields、identity signature、selection count；
- identity conflict/timestamp tie/hash-only semantic selection不得产生 winner；
- version/observation/universe parent coverage 与三表行数公式；
- S1/S2 current dictionary 不得 backfill、filter 或决定历史 eligibility。

Optional provider field 若未来出现非字符串 native type，不会被 coercion，也不会因方便而整行
quarantine：typed nullable 输出暂写 null，原始 Bronze bytes、page/ordinal、row hash 与 source pointer
仍保留，同时 `optional_field_type_invalid_rows` High QA 阻断 preview 进入后续批准。必须 review source
drift 并按需要升级 contract，不能在失败状态下发布。

只 quarantine 结构不可用的 source row，例如 non-object result、ticker 缺失/非字符串/blank、非法
request active 或 provider active 矛盾。以下内容必须保留并由 QA 展示，不能用 quarantine 隐藏：

- 合法 duplicate versions 或 exact occurrences；
- identity conflict / unresolved selection；
- `INDEX` 或 current dictionary miss；
- inactive 但没有 `delisted_utc`；
- casefold collision、name whitespace、跨日 identity churn；
- 同日 FIGI 对多 ticker。

明确禁止：ticker trim/uppercase/casefold merge；用 name/CIK/ticker root 猜 identity；把 missing type
映射 `OTHER`；用 S1/S2 当前字典历史回填；把 `last_updated_utc` 当 signal availability；按
`delisted_utc` 最大值选版本。

## 9. 与旧 materializer 和每日文件要求的关系

旧 `ame-materialize`：

- 会对 ticker `.strip()`；
- 要求单个 active/inactive snapshot 内 ticker 唯一；
- 遇到当前 4,853 duplicate groups 会直接失败；
- 没有 version evidence、双时间 scope 或 release workflow。

因此它不进入 S4 正式路径，也不被原地放宽。新的 manifest-bound reader 与 session-bounded 纯转换
已经独立实现。输出仍符合“每天一个逻辑文件/partition”的目标：Parquet 以
`session_year/session_date` 分区；内部采用 long table（一行一个 ticker observation），而不是把
390 个分钟位置横向摊成极宽 pandas object。这个结构更适合 predicate pushdown、版本 join、QA 和
后续 daily factor engine。

## 10. 当前硬停点

本轮严格停在三个 workflow 的 `awaiting_review / sequence 5`。2026-05-11 的完整 37-page pair、共享
SourceInventory、三张 build manifests、Parquet、QA、quarantine 和 fixed-case samples 已全部登记并
通过 checksum/schema/row-count/trust-chain 重验。没有 full build、full-run approval、正式 Silver 或
S4 release。

当前只能进行人工 review：

1. 检查本文件 1.2 的 row funnel、warnings 和资源实测；
2. 查看每张 build 下的 `input-sample.json`、`output-sample.json` 与 `fixed-cases.json`；
3. 确认 reconstructed historical membership、metadata vintage、case-sensitive ticker、duplicate
   preservation 和 identity limitations 的披露是否可接受；
4. 对 preview 作出明确接受或要求修正的决定。

即使 preview 被接受，也不能直接运行十年 full build。方案 1 的独立 gate 要求先为每张表创建一个
不可变 `FullRunPlan`，至少冻结：

- 被 review 的 preview build ID、manifest SHA 和 sequence-5 event SHA；
- 十年完整 SourceInventory 的精确 inputs、行数、bytes 和 source digest；
- full transform version、Git commit、exchange-calendar version 与所有逻辑参数；
- 基于本次实测的 runtime、内存、staging/正式输出和磁盘余量预测。

`FullRunPlan` 进入 `full_run_plan_review` 后仍需用户单独批准，才可能进入
`approved_full_run`。本轮没有创建 plan，也没有授权 full run 或 publish；是否加入 earliest、
first-duplicate、latest 等额外 preview boundary day，同样需要新的精确 scope，而不是复用本次批准。
