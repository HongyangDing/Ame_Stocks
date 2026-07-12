# Silver S1 `exchanges` schema review

## 1. 当前状态与授权边界

2026-07-12，S1 从 `planned` 进入 `schema_review`。2026-07-13，用户明确批准 contract
`1803d28f2b4b6088e32d27d06c7102111e4f141b6645a1059829732442f0e479`，因此 S1 已进入
**Phase 1 / `code_ready`**。

schema review 当时只允许：

- 只读检查一份有界、manifest 绑定的 `exchanges` Bronze 快照；
- 核对实际 envelope、字段、类型、空值、候选键、domain 和已有审计结果；
- 提交一个精确、可由 S0 `TableContract` 解析的 schema 候选；
- 识别进入 `code_ready` 前必须由用户决定的语义和框架缺口。

schema review 没有编写业务转换，也没有生成 preview/full build、登记 SourceInventory 或写入
数据盘。schema 获批后的 code-ready 实现仍只使用 synthetic fixture；真实 Bronze preview 继续
保持未执行。

已批准并作为 Python package resource 冻结的合同：
[`exchange_dim.schema-v1.json`](../backend/ame_stocks_api/silver/schema_resources/exchange_dim.schema-v1.json)

- `contract_id`：`1803d28f2b4b6088e32d27d06c7102111e4f141b6645a1059829732442f0e479`
- domain/table：`reference/exchange_dim`
- schema version：`1`

## 2. 本次只读证据边界

数据根：`/mnt/HC_Volume_106309665/american_stocks`

### 2.1 权威输入

| 对象 | 相对数据根路径 | SHA-256 / 结果 |
| --- | --- | --- |
| Bronze manifest | `manifests/massive/exchanges/08b662df642512deb23442fcf12e397d5e30201f054cf9f355fde70168e6f9dc.json` | `bad8b1c15aac37870ad0d860df35aac70846b6e1d1b3339e4de8f19c82bfc8e0` |
| manifest 声明的唯一 page | `bronze/massive/exchanges/request_id=08b662df642512deb23442fcf12e397d5e30201f054cf9f355fde70168e6f9dc/page-00000.json.gz` | stored `6130c1f31636b322c90fb56c09506bcd06a16690bdd32910471dc8bc1f406e57` |
| page 解压内容 | 同上 | raw `63e58f4b43894c489d596fbb259746bfddcdcfeacb13ff66290146ee6536e4bd` |
| Bronze audit v9 | `manifests/audits/bronze/full-2026-07-12-v9.json` | `exchanges`: 1 manifest、1 artifact、27/27 verified rows、0 missing、0 extra |
| REST semantic audit v7 | `manifests/audits/rest_semantics/full-2026-07-12-v7.json` | `exchanges`: 27 candidate-key rows、0 key conflict、0 exact duplicate excess |

manifest 为 `complete`，只声明一个 1,055-byte gzip page；解压后 5,192 bytes。stored/raw
checksum、artifact `record_count=27`、response `count=27` 和实际 `results` 27 行全部相符。

### 2.2 非权威目录残留

只读检查还看到同目录下有一个 20,480-byte `.page-00000.json.gz.swp`；检查时它由远程 Vim
会话打开。它不在 manifest 中，checksum 也不同，因此：

- 它绝不进入 SourceInventory，也不作为备用 page；
- 本次没有打开其内容、删除、移动或修改它；
- 它不改变权威 gzip page 的 checksum 通过结论；
- preview 前仍应重新执行 manifest-bound input verification，且只消费 manifest 明确声明的 page。

## 3. Endpoint 语义和时间边界

Massive 官方的 [`GET /v3/reference/exchanges`](https://massive.com/docs/rest/stocks/market-operations/exchanges)
只接受 `asset_class` 和 `locale` 过滤；history 不适用于该 endpoint，数据按需更新。当前 provider
实现也只发送：

```text
asset_class=stocks
locale=us
```

Bronze canonical request 虽然记录 `start=end=2026-07-09`，该日期**没有发送给 provider**，只能
视为本地下载计划 label，不能成为数据的历史生效日或捕获日。实际 manifest 是：

- `created_at = 2026-07-11T15:37:41.268994Z`
- `completed_at = 2026-07-11T15:37:41.616905Z`
- 本地 canonical request ID：`08b662df642512deb23442fcf12e397d5e30201f054cf9f355fde70168e6f9dc`
- Massive response request ID：`806fdddbb346abb7d3846e3b4f65daa0`

已批准合同采用保守的 point-in-time 规则：

1. `source_capture_at_utc = manifest.completed_at`；
2. `capture_date` 是该时刻的 `America/New_York` 日期，即本快照的 `2026-07-11`；
3. `available_session` 是 XNYS market open 严格晚于捕获时刻的第一个 session；
4. 使用冻结的 `exchange-calendars 4.13.2` 时，本快照为 `2026-07-13`；
5. `available_at_utc = 2026-07-13T13:30:00Z`；
6. 当前快照不能回填到 2026-07-11 以前，不能证明任何交易所在过去已存在或属性相同。

这比把 `2026-07-09` 当作历史 effective date 更保守，也避免回测未来数据。若以后在交易日开盘
前完成捕获，同一条规则允许它从当天尚未发生的 open 起使用；盘中或盘后捕获则从下一次 open
起使用。

## 4. 实际 Bronze 字段画像

response envelope 的实际键为 `count, request_id, results, status`，`status=OK`，`results` 是数组。
27 个 result object 合计只有以下 10 个字段，没有空字符串或首尾空白。

“缺失”表示 JSON key absent；Silver 映射时才成为 null，而不是宣称 provider 返回了显式 null。

| Bronze 字段 | 实际非空类型 | 缺失行 | 非空 distinct | 观察结果 |
| --- | --- | ---: | ---: | --- |
| `id` | int × 27 | 0 | 27 | min 1，max 203；无重复 |
| `name` | string × 27 | 0 | 27 | 无空白 |
| `acronym` | string × 4 | 23 | 4 | 官方 optional |
| `mic` | string × 25 | 2 | 25 | 非空值唯一且全通过 `[A-Z0-9]{4}` |
| `operating_mic` | string × 27 | 0 | 10 | 当前完整但官方 optional，不能冻结 non-null 假设 |
| `participant_id` | string × 23 | 4 | 23 | 当前均为单字符；不把此形态升级为 provider 合同 |
| `type` | string × 27 | 0 | 4 | `exchange` 18、`TRF` 6、`SIP` 2、`ORF` 1 |
| `asset_class` | string × 27 | 0 | 1 | 全部 `stocks` |
| `locale` | string × 27 | 0 | 1 | 全部 `us` |
| `url` | string × 27 | 0 | 21 | 全部是 absolute HTTP(S)；官方 optional，所以 Silver 仍 nullable |

额外检查：

- exact duplicate excess = 0；
- duplicate `id` = 0；
- duplicate non-null `mic` = 0；
- 无非法 `mic` / `operating_mic` 格式；
- `url` 重复是合法的，例如多个 Cboe venue 或 FINRA facility 共用网站；
- 2 个 SIP 行没有 `mic`，不能用 `operating_mic` 填补；
- 多个 `operating_mic` 并不作为本响应中的 venue `mic` 出现，不能要求二者表内自连接完整。

### 4.1 文档与真实 payload 的差异

官方页面当前把 `type` 枚举列为 `SIP, TRF, exchange`，但权威 payload 还有：

```text
id=62, name="OTC Equity Security", mic="OOTC", type="ORF"
```

因此方便后续量化处理的正确做法是**保留 provider 原值**，而不是用网页枚举拒绝或改写该行。
v1 把当前观察到的四个值作为 reviewed domain；未来出现新值时保留行并产生 Medium warning，
等待显式 review 后再更新规则。

## 5. 候选输出合同

### 5.1 Grain、键和物理组织

- table：`reference/exchange_dim`
- grain：一个实际捕获日中的一个 provider exchange record
- primary key：`(capture_date, exchange_id)`
- partition：`capture_date`
- sort：`capture_date, exchange_id`
- source dataset：`exchanges`

v1 明确规定一个 `capture_date` 最多接受一个源快照。同一天出现第二份不同快照时不自动选最新，
而是作为 source-snapshot conflict 阻断 review；不同 release/build 仍由 S0 的 `build_id` 和
`release_id` 区分。这样保持每日参考维表的简单键，同时避免同日两份 current-only 真相被静默
合并。

### 5.2 字段

| Silver 字段 | Arrow 类型 | Nullable | 来源 / 规则 |
| --- | --- | ---: | --- |
| `capture_date` | `date32` | 否 | `completed_at` 转 America/New_York date |
| `exchange_id` | `int64` | 否 | `id`；只作为 provider 内部 ID |
| `name` | `string` | 否 | 原值 |
| `acronym` | `string` | 是 | 原值；absent → null |
| `mic` | `string` | 是 | 原值；未来连接 `assets.primary_exchange` |
| `operating_mic` | `string` | 是 | 原值；不填补 `mic` |
| `participant_id` | `string` | 是 | 原值，不猜测 domain |
| `exchange_type` | `string` | 否 | `type` 原值，包括 `ORF` |
| `asset_class` | `string` | 否 | 原值 |
| `locale` | `string` | 否 | 原值 |
| `url` | `string` | 是 | 原值，不规范化 host/path |
| `snapshot_scope` | `string` | 否 | 固定 `current_reference_snapshot` |
| `source_capture_at_utc` | `timestamp[ns, UTC]` | 否 | manifest `completed_at` |
| `available_session` | `date32` | 否 | 首个 open 晚于捕获时刻的 XNYS session |
| `available_at_utc` | `timestamp[ns, UTC]` | 否 | 上述 session 的 open UTC |
| `availability_rule` | `string` | 否 | 固定 `first_xnys_open_after_source_capture_v1` |
| `source_record_id` | `string` | 否 | dataset/request/artifact SHA/page/ordinal/raw hash 的确定性 SHA-256 |
| `source_request_id` | `string` | 否 | 本地 canonical Bronze request ID |
| `source_provider_request_id` | `string` | 否 | response envelope request ID |
| `source_artifact_sha256` | `string` | 否 | stored gzip page SHA-256 |
| `source_page_sequence` | `int64` | 否 | 0-based page sequence |
| `source_row_ordinal` | `int64` | 否 | 0-based result ordinal |
| `source_row_hash` | `string` | 否 | 转换前 raw object 的 canonical JSON SHA-256 |

`requested_snapshot_date=2026-07-09` 不进入业务表；它仍保留在 immutable Bronze manifest 和
SourceInventory lineage 中。这样避免一个未发送给 provider 的下载 label 被下游误用。

### 5.3 映射、重复与 quarantine

- 只重命名 `id → exchange_id`、`type → exchange_type`；其余 provider 字段不改内容；
- 不 trim、不转大小写、不规范化 URL、不用 `operating_mic` 填补 `mic`；
- optional key absent 写 null；空字符串或首尾空白原样保留并报告；
- 完全相同的 canonical raw row 只保留 `(page_sequence, row_ordinal)` 最小的一行，duplicate excess
  进入 row funnel 和 QA；
- 每个 `capture_date` 必须恰好对应一个权威 source request；同日多快照不能拼接或自动选最新；
- 同一 `(capture_date, exchange_id)` 对应不同 raw hash 时全部进入 quarantine，并以 Critical
  failure 阻止 build；
- 未登记的新 source 字段仍包含在 raw row hash/lineage 中，已映射字段照常输出，同时产生
  Medium warning，不能静默假设 schema 未变。

两个行级 digest 的精确 preimage 使用 S0 `stable_digest`（sorted keys、紧凑 JSON、禁止 NaN）：

```text
source_row_hash = stable_digest(raw_result_object)

source_record_id = stable_digest({
  "dataset": "exchanges",
  "source_request_id": source_request_id,
  "source_artifact_sha256": source_artifact_sha256,
  "source_page_sequence": source_page_sequence,
  "source_row_ordinal": source_row_ordinal,
  "source_row_hash": source_row_hash
})
```

## 6. 冻结候选 QA

所有规则都使用 S0 要求的 native float limit `0.0`；只有 numerator 等于 0 才通过。

| Check ID | Severity / violation | 分子定义 |
| --- | --- | --- |
| `schema_exact` | Critical / failed | output Arrow schema mismatch 数 |
| `source_integrity_invalid` | Critical / failed | manifest/artifact status、bytes、checksum、declared rows 不一致对象数 |
| `source_envelope_invalid` | Critical / failed | 非 OK、非 results array、缺 provider request ID，或存在但不对账的 count page 数 |
| `source_snapshot_cardinality_invalid` | Critical / failed | 每个 capture date 对应的权威 current-only source request 数不等于 1 的日期数 |
| `row_funnel_unreconciled` | Critical / failed | row funnel 不成立时为 1 |
| `required_field_invalid_rows` | Critical / failed | required 字段缺失/null/空白或 ID 非正整数的源行数 |
| `primary_key_conflict_rows` | Critical / failed | 同 capture/id 对应不同 raw row 的源行数 |
| `primary_key_duplicate_excess` | Critical / failed | 输出主键 duplicate excess |
| `lineage_invalid_rows` | Critical / failed | request/page/ordinal/artifact/row/record digest 无法重算的输出行数 |
| `availability_invalid_rows` | Critical / failed | 不符合冻结 calendar/PIT rule 的输出行数 |
| `snapshot_scope_invalid_rows` | Critical / failed | `snapshot_scope != current_reference_snapshot` 的输出行数 |
| `mic_conflict_rows` | Critical / failed | 同 capture 非空 MIC 指向多个 exchange ID 的行数 |
| `mic_format_invalid_values` | High / failed | 非空 MIC/operating MIC 不符合四位大写字母数字的值数 |
| `asset_class_domain_invalid_rows` | High / failed | 非 `stocks` 行数 |
| `locale_domain_invalid_rows` | High / failed | 非 `us` 行数 |
| `unreviewed_exchange_type_rows` | Medium / warning | 不在 `exchange, ORF, SIP, TRF` 中的保留行数 |
| `exact_duplicate_excess_rows` | Medium / warning | 被确定性去重的完全重复 excess |
| `unexpected_source_field_rows` | Medium / warning | 含 reviewed 10-field mapping 外字段的保留行数 |
| `empty_optional_string_rows` | Medium / warning | optional 字段出现空串/纯空白的源行数 |
| `url_invalid_rows` | Low / warning | 非空 URL 不是 absolute HTTP(S) 的保留行数 |

不会把“必须等于 27 行”冻结成 QA；27 只是本次 source inventory 的实际值，provider 可以按需更新。
也不会把 `mic` null 当成失败，因为官方允许 optional，且当前两个 SIP row 合法缺失。

## 7. 跨数据集 QA 的边界

`assets.primary_exchange` 应连接 `exchange_dim.mic`，不能连接 provider `exchange_id`。但 S4
`assets` 尚未处理，因此该 coverage 不能伪装成 S1 required QA。它推迟到 S4：

- 只把 exchange snapshot 用于其 `available_at_utc` 之后的规范解释；
- 对更早历史只做 diagnostic，不能把当前字典回填为历史事实；
- 分别报告 non-null `primary_exchange` 的 unmatched row rate 和 unmatched distinct MIC rate。

## 8. 已修复的 S0 fixed-case 缺口

schema review 时 S0 注册的 14 个 fixed case 都不完整覆盖 current-only reference snapshot，而
`PreviewMetadata` 又要求 `fixed_case_ids` 非空。把 `normal_session` 硬套给 S1 会虚假声称 S1 已证明
分钟稀疏性和 RTH 边界。

本次 schema approval 已授权并在 code-ready 阶段新增第 15 个元数据案例：

`current_reference_snapshot`

必须证明：

- capture date 来自 immutable manifest 完成时间，而不是未被 endpoint 消费的 request label；
- available session 的 open 严格晚于 capture instant；
- current snapshot 不向历史回填；
- 后续 capture 追加新日期分区，不覆盖旧分区；
- 每行都能回溯到精确 request/page/ordinal/raw-row hash。

这一步只扩展固定案例元数据和 synthetic fixture，不改变 S0 控制面的安全边界。注册表现在有
15 个不可变案例；S1 测试已证明上述 invariant，但尚未声称真实 Bronze preview 已通过。

## 9. Code-ready 实现与下一硬停点

用户批准精确 contract 后，已实现：

- `exchange_contract.py`：加载并在 import 时核对精确 approved contract ID；
- `exchange_source.py`：只读构造 SourceInventory，并只消费 manifest 明确绑定的 gzip page；
- `exchanges.py`：纯内存映射、PIT、lineage、去重、quarantine、row funnel 和 20 项 QA；
- `fixed_cases.py`：新增 `current_reference_snapshot`；
- `test_silver_exchanges.py`：只用 synthetic fixture 验证正常、ORF、空 MIC、时间边界、重复、
  schema drift、主键/MIC 冲突、domain、同日双快照和未声明 `.swp` 排除。

当前硬停点仍在真实 Bronze preview 之前。下一步得到明确指示后，才会针对 27 行权威输入登记
SourceInventory、生成 bounded input/output sample、QA/quarantine Parquet 和 preview manifest；
不会因此自动获得 full-run 或 publish 权限。
