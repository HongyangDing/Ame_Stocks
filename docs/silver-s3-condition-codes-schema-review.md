# Silver S3 `condition_codes` 双 workflow schema 设计与执行证据

## 1. 技术结论与授权边界

S3 不能把 Massive 的 `condition_codes` 当成一张以 `id` 为唯一键的简单字典。当前 94 条
provider 记录中，数字 `id` 只有 64 个 distinct 值，而且同一个数字会在不同
`condition_type` 中重复；`quote_condition / id=30` 还同时存在 current 与 legacy 两个定义。
因此 S3 必须保留 condition namespace 和 legacy 版本，并把 `data_types[]` 正规化为独立 bridge，
而不能按 `(asset_class, data_type, id)` 取第一条或覆盖旧版本。

本阶段采用两个独立、完整的 S0 workflow：

1. `reference/condition_code_dim`：一条 provider condition definition 一行；
2. `reference/condition_code_data_type_bridge`：一条 definition 与一个 provider data type 的关系一行。

这样做不是为了增加流程，而是因为 S0 的 contract、build、approval 和 release 都以一张目标表为
边界。两个表若共用一个 workflow 或一个 contract，任一表的 schema、QA、release lineage 都会变得
含糊，也无法让消费者分别固定两个 release ID。两个 workflow 可以共享同一份 manifest-bound
Bronze inventory 和同一 Git commit，但必须分别拥有 contract、event chain、preview、full build、
publish approval 和 release。

用户本次给出的逐字 completion authorization 是：

> 你直接把S3推进到完成吧

这条指令明确授权当前 S3 的两个必要输出从 schema 到 preview、full build 和 publish 一次推进到
结束，不要求在每个正常 gate 再暂停询问；但它不授权以下事项：

- 不授权 S4 `assets` 或后续 family；
- 不授权修改或删除 Bronze、S1/S2 release 或旧项目；
- 不授权伪造尚未生成的 workflow/build/release ID；
- 不授权为未知 Critical/High QA failure、quarantine 或 schema drift 自动创建 waiver；
- 不授权把 current-only 字典回填成历史有效期表。

每个 workflow 仍须依次通过 S0 的九段状态链，并为 schema、full run、publish 分别生成绑定精确
对象和 SHA 的 approval receipt。上述原文可以诚实地记录为这些 receipt 的用户授权来源，但只有在
无 blocking QA、无 quarantine acceptance、preview/full 核心结果一致时才允许继续；出现意外
失败必须停下，不把宽泛的“推进到完成”解释为批准坏数据。

## 2. 权威 Bronze 输入与不可变证据

远程数据根为：

```text
/mnt/HC_Volume_106309665/american_stocks
```

本次只读检查确定，`condition_codes` 权威 namespace 中只有一个 complete manifest 和它声明的
一个 gzip page；未发现第二个 snapshot、未声明 page 或旁路输入。

| 对象 | 相对数据根路径 | SHA-256 / 大小 |
| --- | --- | --- |
| Bronze manifest | `manifests/massive/condition_codes/3054f84fb36c30dceadd16d0533efd7be8ddc13b4cbb64ccf93ac9c2ee5d4bf3.json` | `f4bfc27b609605551a25ccadb77e68ec6f224903259db59a0f72311b46582a40` / 1,178 bytes |
| manifest 声明的唯一 page | `bronze/massive/condition_codes/request_id=3054f84fb36c30dceadd16d0533efd7be8ddc13b4cbb64ccf93ac9c2ee5d4bf3/page-00000.json.gz` | stored `85861aecc2d6fc369578323b11362b4c179d7ff012b9d93d09a244e2463b778a` / 2,100 bytes |
| page 解压内容 | 同上 | raw `b40d5f7b2ca9dbedbd4f9dbb04b8282d93f2a6988a672c3bfe6e66b9db05ffa4` / 22,107 bytes |

文件系统只读画像如下；这些值用于发布后证明 Bronze 没有被 S3 改写，不作为业务字段：

| 对象 | inode | mode / nlink | mtime UTC |
| --- | ---: | --- | --- |
| manifest | 5,391,030 | `0644` / 1 | `2026-07-11 18:54:40.264488558` |
| page | 5,391,033 | `0644` / 1 | `2026-07-11 18:54:40.247488500` |

manifest 和 response 对账结果：

- canonical request ID：
  `3054f84fb36c30dceadd16d0533efd7be8ddc13b4cbb64ccf93ac9c2ee5d4bf3`；
- manifest `status=complete`，只声明 sequence 0，`record_count=94`，`is_last=true`；
- response envelope 的键为 `count, request_id, results, status`；
- response `status=OK`、`count=94`、实际 `results` 也是 94 行；
- provider response request ID：`51c445cae3c1ba479e043ea459c42de9`；
- manifest stored/raw bytes、stored/raw SHA 与现场重算全部相符。

### 2.1 Bronze 与 REST semantic audit

冻结的 Bronze v9 报告：

```text
manifests/audits/bronze/full-2026-07-12-v9.json
SHA-256 = a23fdd2aa4c613274dfe0dcca611e8ed1bd62153146f787ecd415c345c1a15d6
```

其中 `condition_codes` 为 1 expected object、1 complete manifest、1 artifact、94 declared rows、
94 verified rows；missing、extra、unavailable、failed 均为 0，compressed/raw bytes 分别为
2,100/22,107。报告级 semantic gate 的失败来自其他已登记内容异常，不能解释为这份 page 损坏；
该报告的 authoritative-plan 和 physical-integrity gate 均通过。

冻结的 REST semantic v7 报告：

```text
manifests/audits/rest_semantics/full-2026-07-12-v7.json
SHA-256 = 95366ec4abcdc9903b0c1aea972e2cf9f14da008f931bdfc3111523addfae301
```

其中 `condition_codes` 为 1 expected/complete manifest、1 page、94 source rows、123 条展开后的
candidate-key rows、121 distinct candidate keys、2 conflicting keys、0 exact duplicate excess。
两个 conflict 是同一个已识别的 current/legacy 共存问题，不是 corruption：

```text
["stocks", "bbo", 30]
["stocks", "nbbo", 30]
```

S3 的版本保留设计必须把这项审计 difference 消解为两个合法版本，不能通过删掉 legacy 行让检查
变绿。

## 3. 时间证据：request label 不是历史生效日

canonical request 中记录了：

```text
start = 2026-07-09
end = 2026-07-09
adjusted = false
parameters = {}
```

但该 endpoint 是 latest-only 当前字典；`2026-07-09` 只是下载计划 label，不是 provider
condition definition 的历史有效日期。v1 采用与 S1/S2 一致的保守 PIT 规则：

- manifest `created_at = 2026-07-11T18:54:39.901306Z`；
- `source_capture_at_utc = completed_at = 2026-07-11T18:54:40.265369Z`；
- 该时刻的 `America/New_York` 日期为 `capture_date=2026-07-11`；
- 2026-07-11/12 是周末；冻结运行环境的 `exchange-calendars 4.13.2` 给出的下一个 XNYS
  session 是 `2026-07-13`；
- 其 open 为 `available_at_utc=2026-07-13T13:30:00Z`；
- `availability_rule=first_xnys_open_after_source_capture_v1`；
- `snapshot_scope=current_reference_snapshot`。

所以本次 94 行只可用于 `available_at_utc` 之后的解释。它们不能说明 2016–2026 年任意交易日当时
已有同样的 condition mapping；未来再次抓取时应追加新的 `capture_date` 分区，不覆盖本次快照。

## 4. 原始字段、domain 与嵌套结构

### 4.1 字段完整性

| Bronze 字段 | 现场类型 | 缺失 | null / blank | 观察结果 |
| --- | --- | ---: | ---: | --- |
| `id` | integer × 94 | 0 | 0 | 1–94 的稀疏编号；64 distinct，不是全局键 |
| `name` | string × 94 | 0 | 0 | provider 原始名称 |
| `type` | string × 94 | 0 | 0 | condition namespace |
| `asset_class` | string × 94 | 0 | 0 | 全部 `stocks` |
| `data_types` | array × 94 | 0 | 0 | 全部非空，元素均为 string |
| `exchange` | integer × 2 | 92 | 0 | 只出现 1 和 10；缺失表示无 exchange 限定 |
| `legacy` | boolean × 8 | 86 | 0 | 只显式出现 `true`；缺失不等同于 provider 显式 `false` |
| `sip_mapping` | object × 94 | 0 | 0 | 全部非空 |
| `update_rules` | object × 41 | 53 | 0 | 缺失而非显式 null |

本次出现的八类 `type`：

| `type` | 行数 |
| --- | ---: |
| `sale_condition` | 40 |
| `quote_condition` | 33 |
| `financial_status_indicator` | 10 |
| `short_sale_restriction_indicator` | 4 |
| `settlement_condition` | 2 |
| `sip_generated_flag` | 2 |
| `market_condition` | 2 |
| `trade_thru_exempt` | 1 |

这些值是 reviewed domain，不是永远固定的删除 allowlist。未来出现新 namespace 时保留原值并产生
Medium review warning，不能先改写为 `other`。

### 4.2 `data_types[]` 必须展开但保留原数组

| 原数组 | source rows | 展开后关系行 |
| --- | ---: | ---: |
| `["trade"]` | 55 | 55 |
| `["bbo", "nbbo"]` | 29 | 58 |
| `["bbo"]` | 9 | 9 |
| `["nbbo"]` | 1 | 1 |
| **合计** | **94** | **123** |

展开后的 domain 为 `trade=55`、`bbo=38`、`nbbo=30`。本次没有空数组、重复元素、非 string 元素
或其他 data type。Dim 保留 canonical `data_types_json`，bridge 再保存每个元素及其原始
`source_data_type_ordinal`；这样既方便 SQL join，也不会丢失 provider 的数组顺序和原始表达。

### 4.3 legacy/current 冲突必须显式保留

八条显式 `legacy=true` 记录是：

- `sale_condition`：ID 6、27、31、33、35、55；
- `quote_condition`：ID 26、30。

`id` 单独作为键时有 29 组重复；加入 `condition_type` 后仍有一个真实双版本组：

| asset class | type | id | legacy | name | SIP mapping |
| --- | --- | ---: | --- | --- | --- |
| stocks | quote_condition | 30 | absent → current | In View Of Common | `{"CTA":"V","UTP":"V"}` |
| stocks | quote_condition | 30 | true | Equipment Changeover | `{"CTA":"X","UTP":"X"}` |

将缺失的 `legacy` 归一为 `is_legacy=false` 后，
`(capture_date, asset_class, condition_type, condition_id, is_legacy)` 在 94 行中唯一。另设
`legacy_source_present` 保存字段是否真的存在，避免把“provider 没给”伪装成“provider 明确给了
false”。Bridge 的主键必须继续包含 `is_legacy`；否则上述两个版本会在 `bbo` 和 `nbbo` 各冲突
一次，正是 REST semantic audit 已报告的两项 difference。

### 4.4 SIP mapping 与 update rules

`sip_mapping` 的现场结构稳定但仍需保存 canonical JSON：

- 94/94 都是非空 object；
- 只出现 `CTA`、`UTP`、`FINRA_TDDS` 三种 key，出现次数为 71、59、9；
- 四种 keyset 为 `CTA`、`UTP`、`CTA+UTP`、`CTA+UTP+FINRA_TDDS`；
- 139 个 values 全部为 string，当前观察均为单字符大写字母或数字；
- SIP key 不是 exchange ID，不能连接 `exchange_dim`。

`update_rules` 只在 41 行出现：40 条 `sale_condition` 和 1 条 `trade_thru_exempt`。每个 object 都
精确包含：

```text
consolidated:
  updates_high_low: boolean
  updates_open_close: boolean
  updates_volume: boolean
market_center:
  updates_high_low: boolean
  updates_open_close: boolean
  updates_volume: boolean
```

共 246 个 leaf，全部为 native boolean。其余 53 行是字段缺失，不应把六项规则填成 `false`；Dim
同时保存 nullable `update_rules_json` 和六个 nullable typed boolean，且必须逐行核对 flattened 值
与 JSON 一致。

### 4.5 exchange 外键只针对显式 `exchange`

只有两条 sale condition 带 `exchange`：

| condition | exchange ID | 已发布 S1 `exchange_dim` |
| --- | ---: | --- |
| Rule 155 Trade (AMEX), ID 23 | 1 | NYSE American, LLC / AMEX / XASE |
| Rule 127 (NYSE Only), ID 24 | 10 | New York Stock Exchange / XNYS |

两者都能匹配 S1 published release
`feab0e1f32a5685d1115a6e4e87aab8ff50c18b99c6336a8790ecba44464d838` 的
`exchange_id`。S1 source 在同一周六更早捕获，S1 与 S3 都从 2026-07-13 13:30 UTC 开始可用，
因此这 2/2 个 FK 在 S3 可用时点没有未来数据。其余 92 行的 `exchange_id=null` 是合法的“无显式
限定”，不是 unresolved FK。

Dim workflow 必须通过 release-only reader 绑定该精确 S1 release，而不是直接读取 S1 staging 或
按目录猜 latest build。未来 condition snapshot 若早于可用的 exchange snapshot，则不能用后来
的 exchange row 反向补齐。

## 5. Workflow A：`reference/condition_code_dim`

冻结的 packaged contract resource：
[`condition_code_dim.schema-v1.json`](../backend/ame_stocks_api/silver/schema_resources/condition_code_dim.schema-v1.json)

- frozen contract ID：
  `de48f79738b2ed8d65c04a49c9f889ace84b69a4df7771051f67d30acd153192`；
- grain：一个捕获日、asset class、condition namespace、condition ID 和 legacy 状态的一条 provider
  definition；
- primary key：
  `(capture_date, asset_class, condition_type, condition_id, is_legacy)`；
- partition：`capture_date`；
- sort：与 primary key 相同；
- sources：manifest-bound `condition_codes` Bronze 加已发布的 S1 `exchange_dim`。

### 5.1 Dim 的 29 个字段

| # | 字段 | Arrow type | Nullable | 来源 / 规则 |
| ---: | --- | --- | ---: | --- |
| 1 | `capture_date` | `date32` | 否 | manifest completion 的纽约日期 |
| 2 | `asset_class` | `string` | 否 | provider 原值 |
| 3 | `condition_type` | `string` | 否 | `type` 原值，是 ID namespace |
| 4 | `condition_id` | `int64` | 否 | `id` 原值，不跨 namespace 解释 |
| 5 | `is_legacy` | `boolean` | 否 | 仅字段缺失时归一为 false |
| 6 | `legacy_source_present` | `boolean` | 否 | raw object 是否含 `legacy` |
| 7 | `name` | `string` | 否 | provider 原值 |
| 8 | `exchange_id` | `int64` | 是 | 原始 `exchange`；缺失为 null，并做 PIT FK |
| 9 | `data_types_json` | `json_string` | 否 | 保序 canonical JSON array |
| 10 | `sip_mapping_json` | `json_string` | 否 | canonical JSON object |
| 11 | `update_rules_json` | `json_string` | 是 | 字段缺失为 null |
| 12 | `consolidated_updates_high_low` | `boolean` | 是 | typed flatten；无 rules 为 null |
| 13 | `consolidated_updates_open_close` | `boolean` | 是 | 同上 |
| 14 | `consolidated_updates_volume` | `boolean` | 是 | 同上 |
| 15 | `market_center_updates_high_low` | `boolean` | 是 | 同上 |
| 16 | `market_center_updates_open_close` | `boolean` | 是 | 同上 |
| 17 | `market_center_updates_volume` | `boolean` | 是 | 同上 |
| 18 | `snapshot_scope` | `string` | 否 | `current_reference_snapshot` |
| 19 | `source_capture_at_utc` | `timestamp_ns_utc` | 否 | manifest `completed_at` |
| 20 | `available_session` | `date32` | 否 | capture 后首个 XNYS session |
| 21 | `available_at_utc` | `timestamp_ns_utc` | 否 | 该 session open |
| 22 | `availability_rule` | `string` | 否 | 固定 PIT rule |
| 23 | `source_record_id` | `string` | 否 | request/page/ordinal/row digest 的确定性 lineage ID |
| 24 | `source_request_id` | `string` | 否 | canonical Bronze request ID |
| 25 | `source_provider_request_id` | `string` | 否 | response request ID |
| 26 | `source_artifact_sha256` | `string` | 否 | stored gzip SHA |
| 27 | `source_page_sequence` | `int64` | 否 | manifest page sequence |
| 28 | `source_row_ordinal` | `int64` | 否 | result ordinal |
| 29 | `source_row_hash` | `string` | 否 | raw object canonical SHA |

### 5.2 Dim 的 27 项 QA

所有规则都是 `metric=numerator, operator=eq, limit=0.0`。共同的控制、identity、PIT 与 domain
检查为：

| Check ID | Severity / status | 证明内容 |
| --- | --- | --- |
| `schema_exact` | Critical / failed | 29 字段顺序、类型、nullability 精确 |
| `source_integrity_invalid` | Critical / failed | manifest/page bytes、SHA、rows 完整 |
| `source_envelope_invalid` | Critical / failed | OK/count/request ID/results 对账 |
| `source_snapshot_cardinality_invalid` | Critical / failed | 每个 capture date 恰好一份权威 current snapshot |
| `row_funnel_unreconciled` | Critical / failed | input/accepted/duplicate/quarantine 对账 |
| `required_field_invalid_rows` | Critical / failed | identity/name 等不做宽松 coercion |
| `legacy_field_invalid_rows` | Critical / failed | legacy 存在时必须是 native boolean |
| `data_types_invalid_rows` | Critical / failed | 非空、唯一、非 blank string array |
| `primary_key_conflict_rows` | Critical / failed | 含 legacy 的自然键不映射到不同 raw rows |
| `primary_key_duplicate_excess` | Critical / failed | 输出主键无重复 excess |
| `lineage_invalid_rows` | Critical / failed | 每行 digest 可重算 |
| `availability_invalid_rows` | Critical / failed | 严格遵循冻结 calendar/PIT rule |
| `snapshot_scope_invalid_rows` | Critical / failed | scope marker 精确 |
| `asset_class_domain_invalid_rows` | High / failed | reviewed source 全部为 stocks |
| `condition_type_unreviewed_rows` | Medium / warning | 新 namespace 保留但要求 review |
| `data_type_unreviewed_rows` | Medium / warning | 新 data type 保留但要求 review |
| `unexpected_source_field_rows` | Medium / warning | raw schema drift 可见 |
| `exact_duplicate_excess_rows` | Medium / warning | exact duplicate 去除数量可见 |
| `current_legacy_versions_unpreserved_rows` | Critical / failed | 同一 namespace/ID 的 current 与 legacy 版本均被保留 |

Dim 特有检查为：

| Check ID | Severity / status | 证明内容 |
| --- | --- | --- |
| `sip_mapping_invalid_rows` | Critical / failed | required object、string key/value 可解析 |
| `update_rules_invalid_rows` | Critical / failed | 存在时 nested object/boolean shape 精确 |
| `exchange_id_invalid_rows` | Critical / failed | exchange 存在时为合法 native positive integer |
| `exchange_fk_unresolved_rows` | High / failed | 非空 exchange 按 PIT 连接已发布 S1 release |
| `canonical_json_invalid_rows` | Critical / failed | 三个 JSON string 可重建 approved raw payload |
| `update_rule_flatten_mismatch_rows` | Critical / failed | 六个 typed boolean 与 JSON 完全一致 |
| `sip_mapping_key_unreviewed_rows` | Medium / warning | 新 SIP root key 保留并 review |
| `unexpected_update_rule_field_rows` | Medium / warning | 新 nested rule 不被静默丢弃 |

当前 source 的设计预期是 94 input → 94 accepted → 94 Dim output、0 exact duplicate、0 quarantine；
这是待 runtime 验证的预期值，不在运行前冒充已完成结果，也不把“必须永远为 94”冻结为 QA。

## 6. Workflow B：`reference/condition_code_data_type_bridge`

冻结的 packaged contract resource：
[`condition_code_data_type_bridge.schema-v1.json`](../backend/ame_stocks_api/silver/schema_resources/condition_code_data_type_bridge.schema-v1.json)

- frozen contract ID：
  `a088a7ab0c562a9fbb90fb0a242be598b7d983d004af27973dd22666d16960dd`；
- grain：一个 version-preserved condition definition 与一个 provider `data_type` 的 membership；
- primary key：
  `(capture_date, asset_class, condition_type, condition_id, is_legacy, data_type)`；
- partition：`capture_date`；
- sort：与 primary key 相同；
- source：同一份 manifest-bound `condition_codes` Bronze；不从 Dim 的 JSON 反解析数组。

### 6.1 Bridge 的 20 个字段

| # | 字段 | Arrow type | Nullable | 来源 / 规则 |
| ---: | --- | --- | ---: | --- |
| 1–5 | `capture_date, asset_class, condition_type, condition_id, is_legacy` | 与 Dim 相同 | 否 | Dim FK 的 version-preserved natural key |
| 6 | `data_type` | `string` | 否 | 从 provider 数组逐项原样展开 |
| 7 | `legacy_source_present` | `boolean` | 否 | 与 Dim 相同 |
| 8 | `source_data_type_ordinal` | `int64` | 否 | 原数组中的 0-based ordinal |
| 9 | `snapshot_scope` | `string` | 否 | `current_reference_snapshot` |
| 10 | `source_capture_at_utc` | `timestamp_ns_utc` | 否 | manifest completion |
| 11 | `available_session` | `date32` | 否 | capture 后首个 XNYS session |
| 12 | `available_at_utc` | `timestamp_ns_utc` | 否 | session open |
| 13 | `availability_rule` | `string` | 否 | 与 Dim 相同 |
| 14–20 | `source_record_id, source_request_id, source_provider_request_id, source_artifact_sha256, source_page_sequence, source_row_ordinal, source_row_hash` | lineage types | 否 | 与 Dim 同一 raw row 的完整 provenance |

Bridge 复用前述 19 项共同 QA，再增加：

| Check ID | Severity / status | 证明内容 |
| --- | --- | --- |
| `expansion_unreconciled` | Critical / failed | output rows 精确等于 accepted source 的数组长度之和 |
| `source_data_type_ordinal_invalid_rows` | Critical / failed | ordinal 非负、连续且能还原 source-order array |
| `parent_dim_missing_rows` | Critical / failed | 每个 bridge parent key 都存在于同次转换的 Dim |
| `dim_without_bridge_rows` | Critical / failed | 每个 Dim parent 至少存在一个 Bridge membership；精确展开另由 expansion 与 ordinal QA 联合证明 |

因此 Bridge 共 23 项 QA。当前 source 的设计预期是 94 input → 94 accepted → 123 bridge output；
`123 != 94` 是受控的一对多展开，不是 row-funnel 错误。`expansion_unreconciled` 必须证明
`55 + 29×2 + 9 + 1 = 123`，同时主键中的 `is_legacy` 必须保留 id 30 的两个版本。

## 7. 两个 workflow 的依赖、发布顺序与一致性

两个 workflow 必须绑定：

- 同一个 `condition_codes` manifest/page 及其 exact hashes；
- 同一个最终 Git commit、transform/calendar version 和公共逻辑参数；table、contract 与 workflow identity 各自独立；
- 分别冻结的 contract ID；
- 分别独立的 preview/full output prefix、build manifest 和 release manifest。

Source inventory 只把 94 条 Bronze condition rows 计入 row funnel；同时把精确 S1 release manifest
作为第二个 checksummed upstream manifest。这样 lookup dimension 不会把 27 条 exchange rows 错算成
condition input，却会在每次 source verification 时重验 S1 published trust chain 和 release bytes。

执行顺序为：

1. 两个 schema contract 分别登记并进入 `code_ready`；
2. 两个 bounded preview 分别运行并验证相同 source identity；
3. Dim preview 证明 94 条 version-preserved definitions 和 2/2 exchange FK；
4. Bridge preview 证明 123 条 array expansion 和 ordinal round-trip；
5. 分别创建 full-run approval 和 full build；
6. 先发布 Dim；
7. Bridge publish 前对已发布 Dim 做跨 workflow FK gate：每个 bridge natural-key prefix 必须在本次
   Dim release 中恰好存在，不能引用 S1/S2、staging 或另一个 capture；
8. 再发布 Bridge，并分别用 release-only reader 重验两条完整 trust chain；
9. 对最终 `published` event 做精确幂等 replay，确认 event 数、IDs、files、SHA、mtime、inode 不变。

Bridge transform 本身仍直接从 Bronze 展开，以避免把 Dim 的 JSON presentation 当成第二份事实；
但发布协调层必须阻止“Dim 失败而 Bridge 单独发布”。对当前 source，跨表理论关系应是 123/123
bridge rows 都找到 94-row Dim 中的唯一 parent；最终数字必须以 runtime evidence 为准。

两个 release 都只能暴露各自 full build 的正式 DATA Parquet；preview 不得被网页、Gold 或回测读取，
也不创建可漂移的 `latest`/`current` symlink。

## 8. Quarantine、版本与 temporal 规则

- required identity、legacy type、数组 shape、SIP/update rule shape 无法安全解析的 source row 进入
  quarantine；不能通过转字符串或默认 false 把坏行变绿；
- exact canonical duplicate 只确定性保留最早 page/ordinal，并在 funnel/QA 中报告 excess；
- 同一冻结主键对应不同 canonical raw row 时，冲突版本全部 quarantine 且 Critical failure；
- current/legacy id 30 不是 quarantine：`is_legacy` 是主键的一部分，两个版本都应保留；
- 非 reviewed domain 的可解析原值应保留并产生 Medium warning，而不是删除；
- 本次只有一个 capture，不能伪造“新增/消失/变更”的历史 baseline；未来第二次 capture 应比较
  相邻 snapshot 的新增、消失、name/data_types/SIP/update rules/exchange/legacy 变化，并根据实际
  provider drift 决定是否升级 contract，而不是覆盖旧 partition。

本次 completion authorization 不包含任何 quarantine acceptance。若真实 preview/full 与当前
94→94 Dim、94→123 Bridge 的完整 source 画像不一致，必须保留证据并停止，而不能假定 profile
过时后仍直接发布。

## 9. 研究用途与明确限制

这两个表可用于：

- 解释 Massive 文档中 condition ID 所属 namespace、名称和 legacy 状态；
- 识别一个 condition 适用于 trade、BBO、NBBO 中哪些 feed；
- 查看 provider 声明的 SIP code mapping；
- 查看 sale/trade-through 条件对 consolidated 与 market-center OHLC/volume 的声明更新规则；
- 为未来逐笔或定向 API 样本提供版本化、PIT-safe 的 decode dictionary。

它们不能证明：

- 某个历史交易日当时使用了同一字典；当前 snapshot 没有历史有效区间；
- 某一根已下载分钟/日 bar 包含或排除了哪些具体 condition；现有研究 scope 没有全量 trades/quotes；
- 仅凭 `update_rules` 就能从 bars 反推出每笔 eligible trade 或完全复刻 provider aggregate；
- SIP mapping key 等同交易所；CTA/UTP/FINRA_TDDS 是 feed mapping namespace；
- condition definitions 可直接作为研究 universe eligibility 或 Barra 因子。

因此 S3 的价值是提供可追溯的 reference semantics 和未来 QA 基础，不是声称已经解决 provider
aggregate 差异。真正的逐笔验证只有在以后明确增加有界 trade/quote 样本并单独审批时才能进行。

<!-- RUNTIME_EVIDENCE_TODO -->

## 10. Runtime evidence 待最终运行后填写

本节必须由实际 registry、build manifests、release manifests 和远程文件现场检查回填。以下值在
运行前均未知，禁止根据 contract ID 推测或预先填写。

| Evidence | `condition_code_dim` | `condition_code_data_type_bridge` |
| --- | --- | --- |
| workflow ID / final event SHA / sequence | 待运行后填写 | 待运行后填写 |
| schema approval ID | 待运行后填写 | 待运行后填写 |
| preview build ID / manifest SHA | 待运行后填写 | 待运行后填写 |
| preview row funnel / QA / quarantine | 待运行后填写 | 待运行后填写 |
| full-run approval ID | 待运行后填写 | 待运行后填写 |
| full build ID / manifest SHA | 待运行后填写 | 待运行后填写 |
| full row funnel / QA / DATA SHA | 待运行后填写 | 待运行后填写 |
| publish approval ID | 待运行后填写 | 待运行后填写 |
| release ID / release manifest SHA | 待运行后填写 | 待运行后填写 |
| released DATA path / bytes / rows / SHA | 待运行后填写 | 待运行后填写 |
| exact published replay result | 待运行后填写 | 待运行后填写 |

最终验收还须记录：

- 两条 workflow trust chain 和 release-only reader 均通过；
- Dim/Bridge preview 与 full 的 schema、row funnel、QA core、quarantine 和 DATA SHA 是否一致；
- 2/2 exchange FK 与 123/123 bridge-to-dim FK 的实际结果；
- 所有正式 output 是否为 regular file、`0444`、`nlink=1`，manifest 是否声明完整文件集合；
- 发布和精确 replay 前后 Bronze manifest/page 的 SHA、bytes、mtime、inode 是否不变；
- S1/S2 release 是否保持原 published 状态，S4 是否仍未创建 workflow/build/release；
- 本地、GitHub `origin/main`、远程 `/opt/american_stocks` HEAD 是否完全一致且工作树干净。

在这些 runtime evidence 填入前，本文件只证明 S3 的 source profile、schema 决策和授权边界，
不声称两个 release 已经发布。
