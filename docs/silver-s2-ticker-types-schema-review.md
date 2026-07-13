# Silver S2 `ticker_types` approved schema and code-ready checkpoint

## 1. 当前状态与硬边界

2026-07-13，用户逐字批准精确 contract
`b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d`，S2 从
`schema_review` 进入 **Phase 1 / `code_ready`**。Bronze 只读画像、字段语义、17 字段 schema、
20 项 QA、manifest-bound source reader 和纯转换现已冻结；实现目前只在 synthetic fixtures 上
运行。

本检查点已完成：

- 只读检查 manifest 明确绑定的一份 `ticker_types` Bronze 当前快照；
- 冻结目标表的 grain、字段、类型、键、PIT availability、lineage、重复和 quarantine 规则；
- 登记并批准精确 contract，将 workflow 推进到 `code_ready`；
- 将批准内容封装为 package resource，并实现只读 source inventory/reader 与无 I/O 的纯转换；
- 用 synthetic fixtures 验证 schema、lineage、PIT、temporal QA、重复、quarantine 和 domain QA。

本检查点**不授权真实数据运行**：没有读取真实 24 行 source 来执行转换，没有生成 S2 preview，
没有运行 full build，没有批准或发布 release，也没有写入任何 S2 Silver 数据。它同样不读取或
修改 S4 `assets`。下一硬停点在真实 24 行 preview **之前**；代码和 synthetic tests 通过不等于
数据已经进入 Silver。

已批准合同的 review source：
[`ticker_type_dim.schema-v1.candidate.json`](silver/contracts/reference/ticker_type_dim.schema-v1.candidate.json)

- candidate `contract_id`：
  `b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d`
- schema digest：
  `b402318f8b67120fd0bf71fe1b67f56acba31b2ec70915d9b7e57acba84b1957`
- domain/table：`reference/ticker_type_dim`
- schema version：`1`

这两个 digest 由合同内容确定；任何字段、顺序、类型、nullability、key 或 QA 变更都会产生新 ID，
必须重新 review，不能沿用本次批准。

### 1.1 Packaged contract 与 code-ready 模块

- package resource：
  [`ticker_type_dim.schema-v1.json`](../backend/ame_stocks_api/silver/schema_resources/ticker_type_dim.schema-v1.json)
- 固定加载与 contract ID guard：
  [`ticker_type_contract.py`](../backend/ame_stocks_api/silver/ticker_type_contract.py)
- manifest/checksum/envelope-bound source inventory 与 reader：
  [`ticker_type_source.py`](../backend/ame_stocks_api/silver/ticker_type_source.py)
- 纯 Arrow 转换、row funnel、quarantine、PIT 与 20 项 QA：
  [`ticker_types.py`](../backend/ame_stocks_api/silver/ticker_types.py)
- synthetic contract/transform tests：
  [`test_silver_ticker_type_contract.py`](../tests/test_silver_ticker_type_contract.py) 与
  [`test_silver_ticker_types.py`](../tests/test_silver_ticker_types.py)

packaged resource 与批准的 candidate 解析为同一 `TableContract`；运行时 loader 对精确
`contract_id` fail closed。Source reader 只接受 inventory 明确列出的 canonical manifest/page，
逐项重验 status、路径、bytes、stored/raw SHA、gzip、JSON envelope 和 row count；纯转换只接收
已验证的内存 batch，本身不写 staging/Silver，也不推进 workflow。

## 2. 只读证据边界

远程数据根：`/mnt/HC_Volume_106309665/american_stocks`

### 2.1 权威输入

| 对象 | 相对数据根路径 | SHA-256 / 结果 |
| --- | --- | --- |
| Bronze manifest | `manifests/massive/ticker_types/b1e581dac57b064039555580a56d6179b8ecf3a3d00dce7e2ade8cf8abc6dea6.json` | `14e997a8ffd89ee5061bdf6d8c63db1974a9e257b2bb8c3b42d2f08bb3952825` |
| manifest 声明的唯一 page | `bronze/massive/ticker_types/request_id=b1e581dac57b064039555580a56d6179b8ecf3a3d00dce7e2ade8cf8abc6dea6/page-00000.json.gz` | stored `b074aea89befa8bc6795bbd10c34d86448e32b7dec39708a2d4a9983b26e6af6` |
| page 解压内容 | 同上 | raw `9adc3ba97d3d50ef8444a512e0433e60fdde2b140f3d1256ea5d144bc2d6c4f` |
| Bronze audit v9 | `manifests/audits/bronze/full-2026-07-12-v9.json` | `ticker_types`：1 manifest、1 page、24 verified rows，无 missing/extra/corrupt |
| REST semantic audit v7 | `manifests/audits/rest_semantics/full-2026-07-12-v7.json` | `ticker_types`：24 candidate-key rows，0 key conflict，0 exact duplicate excess |

manifest 为 `complete`，大小 1,167 bytes，只声明一个 494-byte gzip page；page 解压后
2,198 bytes。manifest 的 stored/raw checksum、page `record_count=24`、response `count=24` 和
实际 `results` 24 行全部相符。统一 Bronze v9 报告本身的 SHA-256 是
`a23fdd2aa4c613274dfe0dcca611e8ed1bd62153146f787ecd415c345c1a15d6`；REST semantic v7
报告的 SHA-256 是 `95366ec4abcdc9903b0c1aea972e2cf9f14da008f931bdfc3111523addfae301`。

### 2.2 Request label 不是历史日期

canonical request 记录了：

```text
start = 2026-07-09
end = 2026-07-09
adjusted = false
parameters = {}
```

但 endpoint 没有历史日期参数；`2026-07-09` 只是下载计划 label，不能进入业务表，不能解释为
类型字典在该日已经生效。权威时间证据是：

- 本地 canonical request ID：
  `b1e581dac57b064039555580a56d6179b8ecf3a3d00dce7e2ade8cf8abc6dea6`；
- provider response request ID：`23c11b57f67f3f339fa53f3121e04cfc`；
- manifest `created_at = 2026-07-11T15:37:40.092017Z`；
- manifest `completed_at = 2026-07-11T15:37:40.425142Z`。

v1 使用与 S1 一致的保守 PIT 规则：

1. `source_capture_at_utc = manifest.completed_at`；
2. `capture_date` 是该时刻的 `America/New_York` 日期，即 `2026-07-11`；
3. `available_session` 是 XNYS open 严格晚于捕获时刻的第一个 session，即 `2026-07-13`；
4. `available_at_utc = 2026-07-13T13:30:00Z`；
5. `availability_rule = first_xnys_open_after_source_capture_v1`；
6. `snapshot_scope = current_reference_snapshot`，禁止向更早历史回填。

因此这张表是逐次捕获、append-only 的当前字典快照，不是 provider type 分类的历史有效期表。

## 3. Bronze envelope、字段和 24 个当前 code

唯一 response envelope 的键精确为 `count, request_id, results, status`；`status=OK`，
`count=24`。每个 result object 都精确包含四个 string 字段：`asset_class, locale, code,
description`。

| Bronze 字段 | 实际非空类型 | 缺失/null/空白 | distinct | 观察结果 |
| --- | --- | ---: | ---: | --- |
| `asset_class` | string × 24 | 0 | 1 | 全部 `stocks` |
| `locale` | string × 24 | 0 | 1 | 全部 `us` |
| `code` | string × 24 | 0 | 24 | 候选键内唯一；无 trim/case collision |
| `description` | string × 24 | 0 | 24 | 当前全有可读标签；v1 仍允许未来缺失 |

额外检查：exact duplicate excess = 0；候选键 `(asset_class, locale, code)` 为 24/24 唯一；
没有 leading/trailing whitespace，没有空字符串或显式 null，没有未登记字段。

| `code` | provider `description` |
| --- | --- |
| `CS` | Common Stock |
| `PFD` | Preferred Stock |
| `WARRANT` | Warrant |
| `RIGHT` | Rights |
| `BOND` | Corporate Bond |
| `ETF` | Exchange Traded Fund |
| `ETN` | Exchange Traded Note |
| `ETV` | Exchange Traded Vehicle |
| `SP` | Structured Product |
| `ADRC` | American Depository Receipt Common |
| `ADRP` | American Depository Receipt Preferred |
| `ADRW` | American Depository Receipt Warrants |
| `ADRR` | American Depository Receipt Rights |
| `FUND` | Fund |
| `BASKET` | Basket |
| `UNIT` | Unit |
| `LT` | Liquidating Trust |
| `OS` | Ordinary Shares |
| `GDR` | Global Depository Receipts |
| `OTHER` | Other Security Type |
| `NYRS` | New York Registry Shares |
| `AGEN` | Agency Bond |
| `EQLK` | Equity Linked Bond |
| `ETS` | Single-security ETF |

上述 24 行只是本次捕获观察值，不冻结成“永远必须正好 24 个 code”的 hard QA。未来新增、消失
或改描述都保留并产生 temporal warning，而不是被旧 allowlist 删除。

## 4. 候选输出合同

### 4.1 Grain、键和物理组织

- table：`reference/ticker_type_dim`
- grain：一个 America/New_York 捕获日中的一个 provider ticker-type classification
- primary key：`(capture_date, asset_class, locale, type_code)`
- partition：`capture_date`
- sort：`capture_date, asset_class, locale, type_code`
- source dataset：`ticker_types`

每个 `capture_date` 最多接受一份权威 current-only source request。同日出现第二份不同快照时，
不自动选“较新”或拼接，而以 Critical source-cardinality failure 阻断 build。

### 4.2 精确 17 个字段

| # | Silver 字段 | Arrow 类型 | Nullable | 来源 / 规则 |
| ---: | --- | --- | ---: | --- |
| 1 | `capture_date` | `date32` | 否 | manifest `completed_at` 的纽约日历日期 |
| 2 | `asset_class` | `string` | 否 | provider 原值 |
| 3 | `locale` | `string` | 否 | provider 原值 |
| 4 | `type_code` | `string` | 否 | `code` 原值，不归一化、不转 eligibility |
| 5 | `description` | `string` | **是** | provider 原值；key absent/null → null |
| 6 | `snapshot_scope` | `string` | 否 | 固定 `current_reference_snapshot` |
| 7 | `source_capture_at_utc` | `timestamp_ns_utc` | 否 | manifest `completed_at` |
| 8 | `available_session` | `date32` | 否 | 第一个 open 严格晚于 capture 的 XNYS session |
| 9 | `available_at_utc` | `timestamp_ns_utc` | 否 | 上述 session 的 open UTC |
| 10 | `availability_rule` | `string` | 否 | 固定 `first_xnys_open_after_source_capture_v1` |
| 11 | `source_record_id` | `string` | 否 | dataset/request/artifact/page/ordinal/row hash 的确定性 SHA-256 |
| 12 | `source_request_id` | `string` | 否 | canonical Bronze request ID |
| 13 | `source_provider_request_id` | `string` | 否 | response envelope request ID |
| 14 | `source_artifact_sha256` | `string` | 否 | stored gzip page SHA-256 |
| 15 | `source_page_sequence` | `int64` | 否 | 0-based manifest page sequence |
| 16 | `source_row_ordinal` | `int64` | 否 | 0-based `results` ordinal |
| 17 | `source_row_hash` | `string` | 否 | 原始 result object canonical JSON 的 SHA-256 |

只有 `description` nullable。候选 contract 不加入 `requested_snapshot_date`、surrogate type ID、
归一化 code、common-stock/ETF 粗分类、`research_eligible` 或历史有效期；这些字段会把下载 label、
研究决策或并不存在的历史语义混进 provider 字典。

### 4.3 映射、重复和 quarantine

- `code → type_code`，其余三个 provider 字段按原值映射；不 trim、不改大小写、不重写描述；
- `description` 缺失/null 写 null；存在的空串或纯空白仍保留原值并产生 warning；
- `type_code` 不匹配 reviewed format `[A-Z][A-Z0-9_]{0,31}` 时仍保留原值并 warning；
- 新 source 字段仍进入 raw row hash/lineage；已知字段照常映射，同时产生 schema-drift warning；
- 完全相同 canonical raw row 确定性只保留最早的 page/row ordinal，duplicate excess 进入 row
  funnel 和 warning；
- required 字段缺失、null、非 string 或 blank 的源行进入 quarantine；
- 同一主键对应不同 canonical raw row 时，冲突行全部 quarantine，并以 Critical failure 阻断；
- row funnel 必须满足 `input = accepted + exact_duplicate_excess + quarantined`，不能静默丢行。

两个行级 digest 的 preimage 使用 S0 `stable_digest`（sorted keys、紧凑 JSON、禁止 NaN）：

```text
source_row_hash = stable_digest(raw_result_object)

source_record_id = stable_digest({
  "dataset": "ticker_types",
  "source_request_id": source_request_id,
  "source_artifact_sha256": source_artifact_sha256,
  "source_page_sequence": source_page_sequence,
  "source_row_ordinal": source_row_ordinal,
  "source_row_hash": source_row_hash
})
```

## 5. 候选 QA：20 项，全部 limit = 0.0

所有规则的 `metric=numerator`、`operator=eq`、`limit=0.0`。Critical/High violation 使 build
`failed`；Medium violation 保留证据并标记 `warning`，不会从分母或输出中静默删除。

| Check ID | Severity / violation | 分子定义 |
| --- | --- | --- |
| `schema_exact` | Critical / failed | 输出字段、顺序、Arrow type 或 nullability mismatch 数 |
| `source_integrity_invalid` | Critical / failed | manifest/artifact status、bytes、checksum、declared rows 不一致对象数 |
| `source_envelope_invalid` | Critical / failed | 非 OK、results 非 array、缺 provider request ID，或 count 不对账的 page 数 |
| `source_snapshot_cardinality_invalid` | Critical / failed | 每个 capture date 的权威 current-only request 数不等于 1 的日期数 |
| `row_funnel_unreconciled` | Critical / failed | row funnel 不成立时为 1 |
| `required_field_invalid_rows` | Critical / failed | `asset_class/locale/code` 缺失、null、非 string 或 blank 的源行数 |
| `primary_key_conflict_rows` | Critical / failed | 同主键映射到不同 canonical source row 的源行数 |
| `primary_key_duplicate_excess` | Critical / failed | 输出 frozen primary key duplicate excess |
| `lineage_invalid_rows` | Critical / failed | request/page/ordinal/artifact/row/record digest 无法重算的输出行数 |
| `availability_invalid_rows` | Critical / failed | 不符合冻结 calendar/capture/first-open rule 的输出行数 |
| `snapshot_scope_invalid_rows` | Critical / failed | scope marker 不是 `current_reference_snapshot` 的输出行数 |
| `asset_class_domain_invalid_rows` | High / failed | `asset_class != stocks` 的保留行数 |
| `locale_domain_invalid_rows` | High / failed | `locale != us` 的保留行数 |
| `description_missing_or_blank_rows` | Medium / warning | description 缺失/null/empty/whitespace 的保留行数 |
| `type_code_format_unreviewed_rows` | Medium / warning | type code 不匹配 reviewed format 的保留行数 |
| `exact_duplicate_excess_rows` | Medium / warning | 确定性去除的 canonical exact duplicate excess |
| `unexpected_source_field_rows` | Medium / warning | 含 reviewed 四字段之外 source field 的保留行数 |
| `new_type_code_rows_since_prior_capture` | Medium / warning | 相比紧邻前次 capture 新出现的 key 数 |
| `disappeared_type_code_rows_since_prior_capture` | Medium / warning | 相比紧邻前次 capture 消失的 key 数 |
| `description_changed_rows_since_prior_capture` | Medium / warning | 两次 capture 共有 key 中 description 原值改变的 key 数 |

`asset_class_domain_invalid_rows` 和 `locale_domain_invalid_rows` 的语义特意与 required-field
quarantine 分开：只要原值是非空 string，domain mismatch 行就原样保留在候选输出中，同时产生
High `failed` QA 并阻断 build。实现不会通过删除、改写或 quarantine 非 `stocks`/`us` 行来让
domain 检查变绿；这样 review 能看到 provider domain drift 的原始值和 lineage。

### 5.1 Temporal QA 的分母

三项 temporal QA 都只比较同一 build 内按 `capture_date` 排序后、当前 capture 与紧邻前次
capture，不跨过中间版本，也不把当前字典解释成历史有效期：

| Check | numerator | denominator |
| --- | --- | --- |
| new type code | `current_keys - prior_keys` 的 key 数 | current capture 的 accepted distinct key 数 |
| disappeared type code | `prior_keys - current_keys` 的 key 数 | prior capture 的 accepted distinct key 数 |
| description changed | 共有 key 中 canonical description 不同的 key 数 | 两次 capture 的共有 distinct key 数 |

最早 capture **排除**比较：这三项在只有一份快照时 denominator 都是 0、numerator 也是 0，
不生成“缺 baseline”warning，也不虚构一个需要审批的 prior snapshot。本次 24 行 source 因而只会
建立第一份 current snapshot；新增/消失/改描述从未来第二次捕获开始才有意义。

## 6. 与 S4 `assets.type` 的边界

S2 字典未来用于解释 S4 `assets.type`，但 S4 尚未进入 schema review。当前阶段不能为了让 coverage
变成 100% 而读取、修剪或改写 S4 数据，也不能把当前 24-code 快照历史回填到过去十年。

因此 `assets.type → ticker_type_dim.type_code` coverage **明确推迟到 S4**，并按以下方式验收：

- 对字典 `available_at_utc` 以后捕获的 assets observations 做正式 PIT decode coverage；
- 对更早 assets history 只做 diagnostic，并清楚标记 current dictionary backfill 风险；
- 分别报告 non-null `assets.type` unmatched rows 和 unmatched distinct codes；
- unmatched code 保留原始 observation 并进入 review，不改写为 `OTHER`；
- common-stock、ETF、ADR 等研究 eligibility 由独立、版本化的 S4/S7 规则定义，不能由 S2
  description 文本隐式推断。

这项推迟不是漏测，而是避免跨数据集顺序错误和 survivorship/look-ahead bias。S2 的 required QA
只证明字典自身完整、可追溯和可按捕获时点使用。

## 7. 已批准的精确决定与下一硬停点

用户对精确 contract 的批准确认了以下边界，但**没有**批准运行 preview：

1. 接受 `reference/ticker_type_dim` 的 17 字段、字段顺序和唯一 nullable `description`；
2. 接受主键 `(capture_date, asset_class, locale, type_code)` 和按 capture date append-only；
3. 接受 request label 不进入业务表，PIT 从 `manifest.completed_at` 后首个 XNYS open 开始；
4. 接受 provider code/description 原值，不在 S2 构造 coarse security type 或 eligibility；
5. 接受 13 个 Critical/High hard checks、7 个 Medium warnings，以及 temporal earliest-capture
   denominator=0 的语义；
6. 接受 `assets.type` coverage 和研究 eligibility 推迟到 S4/S7；
7. 批准精确 contract ID
   `b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d`。

上述 contract 已进入 `code_ready`，纯转换和 synthetic fixture 测试已经完成。Synthetic tests
覆盖 request date label 与 capture time 分离、首个严格晚于 capture 的 XNYS open、current-only
snapshot、相邻 capture temporal diff、首个 capture 的 0/0 temporal 指标、精确重复、主键冲突、
required-field quarantine，以及保留 domain mismatch 后产生 High failure。

当前再次停在真实 manifest-bound 24 行 preview **之前**。截至本检查点，没有 S2 preview、full
build、release 或 S2 Silver 数据产物；未经另一次明确授权，不读取真实 source 执行转换，也不
创建 staging preview，更不会推进 full build 或 publish。
