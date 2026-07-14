# S7 Identity Resolution：combined-source profile 与 schema proposal

## 1. 当前检查点与硬边界

S7 已开始，但本检查点只完成两件事：

1. 只读核验 S4、S5、S6 的固定发布版本并做联合身份画像；
2. 提交 `asset_master`、`ticker_alias`、`issuer_master`、`universe_daily` 四份候选 schema。

本检查点没有写 S7 transform、fixture、preview、FullRunPlan 或 PublishPlan；没有创建远程 S7
staging/Silver 产物；没有修改 Bronze、已发布 Silver 或现有 release。四表 contract 获得显式批准前，
不得进入 code-ready。

联合 profile 的机器可读证据在
[`silver/source-profiles/identity-resolution-s7-2026-07-14.json`](silver/source-profiles/identity-resolution-s7-2026-07-14.json)，
文件 SHA-256 为
`02678e174d70d2801152a4fed67c2e6579f32ed0a2d3922cfc63651df4851545`，其中 deterministic fact
digest 为 `f788483993e6c4536eb15acece4a90ddd4e8e86005763bfe8ad43d84ac7ec3af`。

三份只读 profiler 和一份全量 integrity receipt 脚本已纳入版本管理并通过 lint，SHA-256 分别为
`fd9b78a4…`、`74eec67e…`、`48bc84ca…`、`90063edb…`。同一远程 fixed releases 上的 stdout
SHA-256 分别为 `921b4b5a…`、`5f031abe…`、`c0523a8d…`、`4a629fa3…`。integrity run 逐一重验
7,542 个 artifact、六份 release manifest 和批准的 exact S4 atomic marker；另从 S6 release/build
参数，经 overview/lifecycle 两份 source inventory 和 canonical coverage receipt，重建并固定 lifecycle plan 为
30,739 行 / 12,910,337 bytes / SHA-256 `ce8e6c45…`，S6 pending quarantine 为 169 行 /
20,063 bytes / SHA-256 `b12b8bae…`。完整值、初次执行 remote HEAD、runtime/RSS 与零 filesystem
output receipt 均在机器 profile 中。

## 2. 固定输入：只按 release ID，不找“最新文件”

S7 输入固定为六个已发布业务表、共 7,542 个 DATA artifacts、138,825,855 行、
15,944,020,220 bytes：

| 输入表 | Release ID | Full build | 行数 | bytes |
| --- | --- | --- | ---: | ---: |
| `asset_observation_daily` | `26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c` | `9e3b5df531c01d1bcdd73cbd9cdf747bd30cdff459481b262e1ed7a23f40acc4` | 69,381,182 | 8,248,987,847 |
| `asset_observation_version` | `b422fd05df859b33587b8ece80d078247dd972d01d272710ef49c3529b0e54be` | `59708791dc897214d3151dfd7da6b15534800afabf0c36dd36c566bd8d01ef9a` | 9,706 | 14,376,829 |
| `universe_source_daily` | `c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614` | `21921c72c4be79665d41077664f8f027a1beb9ac0600ff4c6610d4f40638b185` | 69,376,329 | 7,661,290,322 |
| `ticker_event_request_status` | `afc63db6850fb50295daa8e6e499c52fe1c16b8290b7932b08aea67531ff98eb` | `7ff845634148274b61c2f515cb66cb9e94f8bb8a5e1abe47316343eaa9f22ca1` | 15,173 | 2,362,190 |
| `ticker_change_event` | `18a7eb3dd6805b94151f5b6ce0167c19dbeb328f45bec7c2f806dac42b8a6350` | `7753688e3d4f19658ca5657b2dc5ccb9bf4c4b229b3c58dc68b255d5999735d2` | 12,895 | 3,805,444 |
| `ticker_overview_safe` | `8715f90d0e01f990e9738b9266edfeb2830a76d59a00ae4fb7490d9f077092a5` | `f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0` | 30,570 | 13,197,588 |

固定 source inventory 指纹：

- artifact refs digest：
  `0ee09491cfb7b3e627950d15f2cd12211aca44433e3b869f5556086065448a5a`
  （`s7_release_output_groups_v1`）；
- release bundle digest：
  `79c8a1237569737368c6ebe56754cd7b932ce12b70cf817d90a93841b5185c1b`
  （`s7_six_release_receipts_v1`）；
- S4 atomic release-set：
  `f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d`，marker SHA-256
  `937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13`。

六个 release 的机器可读 binding ID 为
`49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64`。两个 digest 都由
版本化脚本从固定顺序的六组 exact release outputs/receipts 重算，测试会从 profile 的 receipt payload
独立重算，不再只比较两个已存副本。四个 member build
必须共享这一个 binding、完全相同的 inputs/source digest，并从同一个内存 resolution graph 生成；
`asset_master`、`ticker_alias`、`issuer_master`、`universe_daily` 是 sibling outputs，不得互相作为
staging 或 unpublished Silver source。逻辑依赖图为：

```text
S4 protected three-release set + S5 status/event + S6 overview
                              │
                              ▼
                 canonical six-release bundle
                              │
                              ▼
                 ephemeral resolution graph
                    ├── asset_master
                    ├── ticker_alias
                    ├── issuer_master
                    └── universe_daily
                              │
                              ▼
             coordinated QA + four-table atomic release-set
```

S4 的保护边界不能全局放开。后续代码阶段复用现有 `PublishedAssetEvidenceReader` 对完整 S4
release-set 的验证，并新增窄范围 protected-S4 source-inventory admission，使 S7 coordinator 可消费
这三张表，同时保持 generic `PUBLISHED_SILVER` reader 的拒绝不变。S6 的标准 published reader
不暴露 quarantine；169 条 pending issue 也必须通过单独的、release-authenticated evidence 路径读取，
不能绕过发布信任链。

## 3. 联合画像结论

### 3.1 S4 全历史 daily universe

`universe_source_daily` 的 2,513 个分区、69,376,329 行已做一次全量只读标量扫描：

| 指标 | 结果 |
| --- | ---: |
| 日期 | 2016-07-11 至 2026-07-09 |
| active / inactive | 25,630,067 / 43,746,262 |
| active 行含 Composite FIGI | 19,963,825（77.89%） |
| active 行无任何 security-level FIGI | 5,666,242（22.11%） |
| active exact ticker | 25,381 |
| 全历史 exact ticker | 36,573 |
| active Composite / Share-class FIGI | 18,072 / 15,018 |
| 每日 active 行 median / min / max | 10,847 / 8,213 / 12,977 |

和日频股票因子最相关的 source type 中：

- `CS`：12,545,704 个 active row，10,420,309 个有 Composite FIGI，覆盖率 83.06%；
- `ADRC`：978,574 个 active row，912,079 个有 Composite FIGI，覆盖率 93.20%；
- `ETF`：6,796,768 个 active row，6,365,345 个有 Composite FIGI，覆盖率 93.65%。

因此 S7 preview 不能只报告“解析成功多少资产”；必须按 session row、ticker、type 分别报告 strong、
candidate、unresolved 和 quarantine。当前 77.89% 只是直接 Composite-FIGI evidence coverage，
不是最终 S7 resolution rate。

### 3.2 为什么需要 `issuer → share class → tradable asset` 三层

S6 里 17,337 个 Composite FIGI 的 Share-class 关系为：16,597 个唯一映射、737 个没有
Share-class FIGI、仅 3 个映射到多个 Share-class FIGI。三个例外均可按日期分离：

| Composite FIGI | ticker | 日期边界 |
| --- | --- | --- |
| `BBG000KMY6N2` | `SOR` | 2024-12-31 / 2025-01-02 |
| `BBG01XL8FHT0` | `XZO` | 2025-11-05 / 2025-11-06 |
| `BBG021DMXXT2` | `ANABV` | 2026-04-06 / 2026-04-07 |

反方向 `Share-class FIGI → 多个 Composite FIGI` 是正常层级：一个全球 share class 可以有多个
国家/Composite 层 tradable securities，不能当成冲突。基于此，S7 v1 冻结为：

```text
issuer_id (CIK layer)
    └── share_class_id (Share-class FIGI layer)
            └── asset_id (U.S. Composite FIGI / tradable-security layer)
```

ID 使用仓库现有的单参数 `stable_digest(object)`，不是伪多参数调用或随机 surrogate。算法冻结为：
exact-key JSON object 经 `json.dumps(..., allow_nan=False, ensure_ascii=True,
separators=(",", ":"), sort_keys=True)` 编码为 UTF-8、无换行，再取 lowercase SHA-256。字段不可增减；
日期是严格 `YYYY-MM-DD`；nullable alias hierarchy key 必须写 JSON `null`，不能省略、写空串或
字符串 `"null"`。四类 exact payload 为：

```json
{"anchor_type":"composite_figi","anchor_value":"<exact FIGI>","namespace":"ame_stocks.identity.asset","rule_version":"ame_stocks_asset_id_from_composite_figi_v1"}
{"anchor_type":"share_class_figi","anchor_value":"<exact FIGI>","namespace":"ame_stocks.identity.share_class","rule_version":"ame_stocks_share_class_id_from_share_class_figi_v1"}
{"anchor_type":"cik_normalized","anchor_value":"<10-digit CIK>","namespace":"ame_stocks.identity.issuer","rule_version":"ame_stocks_issuer_id_from_normalized_cik_v1"}
{"asset_id":"<asset sha256>","issuer_id":"<issuer sha256 or null>","namespace":"ame_stocks.identity.ticker_alias","rule_version":"ame_stocks_ticker_alias_id_from_observed_interval_v1","share_class_id":"<share sha256 or null>","ticker":"<exact ticker>","valid_from_session":"YYYY-MM-DD"}
```

FIGI 必须原样满足 `^BBG[0-9A-Z]{9}$`，不得 trim/casefold/Unicode normalize；CIK raw 只接受未
trim 的 1–10 位 ASCII 数字字符串，拒绝全零，再 `zfill(10)`；ticker 保持 S4 exact case-sensitive
值。AAPL 固定向量分别为 asset `423f8da3d1b7dcae53aa997d845cd269fe8ed3ab188dc3e7e982d18c8650ce08`、
share class `858ec64e0790912a3298b0c2ac62023f3d807bfcde8f7e143d179c3db8915012`、issuer
`cd178adefcd4e3b564cafee98411e18c87bd843d91eeb502a4ac2604dfee7940`，以及 2024-01-02
alias `ff8708591441fc3a86ed609d1e025b78f392b9a9f415f268573f2f44224f34f1`。

当前 fixed six-release bundle 没有独立、可用日明确的 supersession plan，因此 v1 所有 asset 都是
`active_identity`、所有 issuer 都是 `active_reference`，两个 `superseded_by_*_id` 保留字段必须为
null。若未来 provider 修正 anchor，旧 ID 不会静默改写；必须先单独批准 supersession source、availability
规则、新 contract version 与新 release binding，不能用当前字段自行推断迁移。

### 3.3 CIK 只能标识 issuer，不能标识 asset

S6 中 959 个 CIK 对应多个 Share-class FIGI，最大 454 个；2,491 个 CIK 对应多个 Composite
FIGI，最大 464 个。七日 S4 固定抽样若按 CIK 直接 join，会从 163,537 行膨胀到 3,451,447 对，
约 21.105 倍。

因此：

- CIK 只生成 `issuer_id`；
- 相同 CIK、name、ticker root 不能合并资产；
- CIK 与 FIGI 不一致是 issuer relationship review，不会覆盖 Composite-FIGI asset anchor；
- S6 里 679 条“请求是 FIGI、最终只由 CIK 支持”的记录继续保持非 backtest eligible。

### 3.4 S5 / S6 的使用边界

S5 的 12,895 条 accepted event 中，11,989 条可得到唯一 Share-class 候选，其中 2 条全局关系原本
歧义、但可由 `event_date` 唯一落入上表的日期区间。另有 355 条 Composite 存在但无 Share-class、
551 条 Composite 未出现在 S6。

S5 的 3,702 个 HTTP 404 是 2026 当前 endpoint 结果，不代表历史不存在：其中 3,613 个 Composite
仍在 S6 历史证据中出现。因此 404 只能作为 request-status coverage，不能删除历史资产。

S5 event 也不是永久身份的独立 anchor：只有 exact Composite 已由 accepted S4 selected membership
或 accepted S6 Overview 独立锚定时，event 才能附着并贡献 count/corroboration/availability。未附着的
S5 event 保留在上游 event 表并进入 build-level High review metric，不创建 asset、issuer relationship、
alias 或 universe resolution，也不进入 asset-row funnel。

S6 `first_active_date/last_active_date` 是 observation envelope，不证明中间连续有效。S5 没有 prior
ticker，`event_date` 也不是 announcement/availability。Ticker interval 必须以 S4 每日 membership
为 spine，S5 只佐证 boundary，S6 只提供身份候选。

## 4. S7 v1 resolution contract

### 4.1 证据优先级

1. `source_composite_figi_exact`：S4 row 自带合法 Composite FIGI，生成 deterministic `asset_id`；
2. `share_class_date_unique_composite_candidate`：只有 Share-class FIGI 时，在日期上只命中一个
   non-conflicting Composite；保留 candidate，v1 默认不直接 backtest eligible；
3. `share_class_only`：只生成/保留 parent `share_class_id`，不生成正式 `asset_id`；
4. `cik_issuer_only`：只生成 `issuer_id`；
5. `ticker_only`：只保留 exact ticker evidence；
6. `conflict`：安全层级或日期仍有多个候选，active membership 仍留在 DATA，标为
   `conflicted_unresolved` 且不可回测。

合法 Composite FIGI 即使 Share-class/CIK 关系冲突也保留确定的 `asset_id`，但使用
`resolved_conflicted`、不生成 alias、`backtest_identity_eligible=false`。所有 candidate、unresolved、
conflicted row 都必须留在 DATA funnel，不得伪装成 resolved strong。标准 source quarantine 只用于
不可信 supporting evidence；S5 的 193 条和 S6 的 169 条 upstream quarantine 在 build level 对账，
不自动生成、删除或污染某个 daily membership row。名字、CIK、ticker root、ticker casefold、S6
envelope 都不是 promotion rule。

### 4.2 Ticker interval

`ticker_alias` 使用 observed XNYS session 区间：

```text
[valid_from_session, valid_to_session_exclusive)
```

- interval 只由 S4 active membership 的连续 XNYS sessions 生成；
- exact ticker、asset、Share-class 或 issuer 改变时拆段；resolution status 只是 versioned attribute，
  单独变化不拆段也不改变 `ticker_alias_id`；
- inactive/gap 后同一 ticker/asset 再出现时是新 interval，避免掩盖 ticker reuse；
- 相邻且所有 identity fields 相同的 interval 必须 coalesce；
- 非末端 `valid_to_session_exclusive` 必须等于 `valid_through_session` 后的下一条固定 S4 source
  session，即使 ticker 在该 session 缺席；coverage 末端才 right-censored/null；
- 任一 `(session_date, exact ticker)` 最多一个 eligible asset；
- S5 event 可标记 `ticker_event_corroborated`，但不能单独创建 start/end 或 predecessor ticker。
- interval-level S5 association 只接受 distinct `source_record_id`：
  `response_composite_figi == interval.composite_figi`、exact case-sensitive
  `effective_ticker == interval.ticker`、`event_date_quality == ordinary_calendar_date`，且 calendar
  `event_date` 落在 `[valid_from_session, valid_through_session]` 内并只命中一个 emitted interval；
  sentinel/cluster quality、pair mismatch、区间外、零命中或多命中全部不计入 interval count；
- `source_row_count == interval_session_count`，边界 source record 必须精确对应首尾 session；
- eligible `universe_daily` 与 alias 展开必须双向一一覆盖，不能存在多余 alias session。

已知需要 preview 明示 disposition 的 6 个不同 Share-class lifecycle-envelope overlap ticker 为
`PKD, BCEL, RAD, SRCL, SWIR, VOXX`。后五个集中在 2019-09-24 单日，可能是 provider/snapshot
系统性 artifact；这是待回看 Bronze 的推断，不是已批准结论。`PKD` 是多月 overlap，必须单独 review。

### 4.3 `universe_daily` 的 active-only 选择

S4 `universe_source_daily` 继续永久保存 69,376,329 个 active + inactive source rows。S7 下游
`reference/universe_daily` 则只保留 25,630,067 个 active membership rows，但不会丢弃 unresolved：

| status | method | asset_id | share_class_id | issuer_id | alias | eligible |
| --- | --- | --- | --- | --- | --- | --- |
| `resolved_strong` | `source_composite_figi_exact` | required | optional exact | optional exact | required | true |
| `resolved_conflicted` | `source_composite_figi_exact_with_relationship_conflict` | required | only if independently non-conflicting | only if independently non-conflicting | null | false |
| `candidate` | `share_class_date_unique_composite_candidate` | null | required | optional exact | null | false |
| `unresolved` | `share_class_only` | null | required | optional exact | null | false |
| `unresolved` | `cik_issuer_only` | null | null | required | null | false |
| `unresolved` | `ticker_only` | null | null | null | null | false |
| `conflicted_unresolved` | `conflict` | null | only if independently non-conflicting | only if independently non-conflicting | null | false |

- 每个 active `(session_date, ticker)` 必须恰好出现一次；
- resolved row 附 `asset_id/share_class_id/issuer_id/ticker_alias_id`；
- candidate/unresolved/conflicted row 仍留在 DATA；direct-Composite 的 `resolved_conflicted` 保留
  `asset_id`，Share-class/CIK-only 方法分别保留可独立验证的 `share_class_id`/`issuer_id`，但它们都不
  生成 alias，且全部 `backtest_identity_eligible=false`；
- 回测显式过滤 `active_on_date=true AND backtest_identity_eligible=true`。

`backtest_identity_eligible` 只表示可做 retrospective structural join，不表示这份 2026 下载/重建的
身份或描述字段在历史当时可作为信号。`universe_daily` 会分别保留 S4 membership source availability
与实际参与解析的 identity-evidence availability，并固定 `current_reference_factor_eligible=false`。

这和最初“每日保留 active/inactive 标记”的要求表面不同，但不丢信息：inactive 证据已经在 S4
审计表中；final universe 用逐日 active membership 才是更直接的无幸存者偏差回测输入，并避免复制
43,746,262 条不参与当日股票池的 inactive rows。如果希望 S7 也物理复制 inactive rows，应在批准 schema
前明确修改，本方案默认采用 active-only。

### 4.4 其余三表的 exact state matrix

`asset_master`：

- `unique_share_class`：parent ID/FIGI 非空且 distinct share count = 1；
- `temporal_multiple_share_classes`：parent 为空、count ≥ 2、conflict count = 0；
- `missing_share_class`：parent 为空、count = 0；
- `conflicted_share_class`：parent 为空、count ≥ 2、conflict count > 0；
- `resolved_strong_s4` 要求 strong count > 0；`reference_only_s6` 要求 strong = 0 且
  candidate > 0；v1 不允许 `superseded` / `superseded_identity`，replacement 恒为 null；
- eligibility 当且仅当 `active_identity` + resolved strong + conflict count = 0 + 非 conflicted share state。

`ticker_alias`：

- `source_composite_figi_exact ↔ resolved_strong`；
- `source_composite_figi_exact_temporal_hierarchy ↔ resolved_temporal`；
- 两类都必须有 asset/composite、可空但 jointly-valid 的 share parent、可空 valid issuer，并且所有
  emitted alias 都 `backtest_identity_eligible=true`；
- `ticker_event_corroborated == (ticker_event_count > 0)`；exclusive end 为 null 当且仅当 right-censored。
- `ticker_event_count` 只按上述 exact unique association 计 distinct S5 `source_record_id`；多 interval
  命中是 Critical，不按最近日期、source ordinal 或输入顺序打破 tie；未关联事件单独 High 对账。

`issuer_master`：

- v1 只允许 `active_reference`，replacement 恒为空；没有 supersession source，不能输出
  `superseded_reference`；
- name/SIC 的 `unique` 分别要求 scalar 非空且 variant count = 1；`multiple` 要求 scalar 为空且
  count ≥ 2；`missing` 要求 scalar 为空且 count = 0；
- `backtest_classification_eligible` 永远为 false。

所有 observed/evidence/variant/event counts 均从同一个 six-release resolution graph 精确重算，不能把
已有 aggregate 当输入；asset/issuer count 只计已通过 exact security identity 附着到 S4/S6 anchor 的
accepted `ticker_change_event` occurrences，未附着 S5 event 单独对账，
`ticker_event_request_status`（含 404）全部排除。相应 mismatch 是 Critical。

### 4.5 Availability 的 exact rule

`identity_evidence_available_session` / `reference_available_session` 一律从原始 accepted source evidence
计算，不能从 sibling output 的 availability 复制。先构造“实际参与该 row 的 ID、hierarchy、method、
status、eligibility、count、corroboration 或 conflict disposition”的 direct evidence set，再做 transitive
evidence closure：只要 referenced asset/share/issuer/alias 的状态或冲突依赖其他日期、ticker 或 entity 的
source row，那些 accepted source rows 也全部进入 closure。最终取整个 closure 的
`max(source_available_session)`：

- 可用集合只含 S4 selected membership、accepted S5 event、accepted S6 Overview；
- 对 alias row，accepted S5 event 还必须满足 exact Composite/ticker、ordinary date quality 与 unique
  interval containment；未关联/歧义事件不进入该 interval 的 availability closure；
- S5 request-status（含 404）永远排除；
- S4 不只包含当前 daily/interval 行；asset-level、parent-level 和 competing conflict 裁决所使用的其他
  session/entity S4 selected rows也必须进入 closure；
- sibling output row 本身不是 source，不能作为 closure 终点掩盖其上游证据；
- `ticker_only` daily row 没有额外身份 evidence，严格回退为自身
  `membership_source_available_session`；
- conflict row 对所有参与裁决的 competing accepted evidence 取 max；
- asset、issuer 与 alias 不允许 empty evidence set。

membership availability 仍逐行原样继承 S4，与 identity/reference availability 不合并。四表都有 Critical
recomputation gate，避免不同实现产生不同可用日。

## 5. 四份候选 schema

四表将来必须作为同一个 atomic release-set 发布，任何一表失败都不能单独对 Gold/backtest 可见。

| Table | columns / QA | Contract ID | Arrow schema digest | Candidate file SHA-256 |
| --- | ---: | --- | --- | --- |
| `identity/asset_master` | 26 / 26 | `d7a6ef66f72c1048b6556b57910af3afbea4926661f3c6062708937fbc2b4ba6` | `83415d165ed166cea75fc9103c0ce062bac893bb725cea478759bdf049f138ff` | `ef8b6a9160a20a9f9d7313e978c7588ee26db97fe994139399511d532cf2cce4` |
| `identity/ticker_alias` | 29 / 33 | `e645573e813c18a82fcea80f3bfef07c547c738fc00cc01419a1f1824a27a47b` | `e5b021c0e1ae2e956b815b41a1f8ecd8ad9762daecbd6b5d9088654770143793` | `5da4cedd48bc83d39225e2facaf1c2f05ef5655f3ec2480e5d42067f8daba77f` |
| `identity/issuer_master` | 24 / 27 | `33c146bab2a9aed61a44d8c20e9b301fd1aa116deb5185214df92a4ee69f632d` | `0b308f96f3277385e40faf1f42a3aa420df1dd7d9dd85f7a932036fec8527f6e` | `4f637d92bf89fb685577be013809649c82967400228648c54272d785ab3d5e6a` |
| `reference/universe_daily` | 32 / 33 | `915a389ccfa9d8442cd2b7b2a14f782adf68f2bb5c8635ddf90224102750e319` | `c6133821e404b3a35ed7b460035796ffee6a1d69c456c8f380076fa6e9cd2329` | `12cf84371b99e23c10b4578760bf8ee6b64a139505bfacf0e6ac521325ca8a84` |

候选文件：

- [`asset_master.schema-v1.candidate.json`](silver/contracts/identity/asset_master.schema-v1.candidate.json)
- [`ticker_alias.schema-v1.candidate.json`](silver/contracts/identity/ticker_alias.schema-v1.candidate.json)
- [`issuer_master.schema-v1.candidate.json`](silver/contracts/identity/issuer_master.schema-v1.candidate.json)
- [`universe_daily.schema-v1.candidate.json`](silver/contracts/reference/universe_daily.schema-v1.candidate.json)

其中：

- `asset_master` 一行一个 Composite-FIGI asset；Share-class 是可空 parent，不把 1:N 当冲突；
- `ticker_alias` 一行一个 maximal contiguous interval；弱 candidate 不进入该表；
- `issuer_master` 一行一个规范化十位 CIK；provider `reference_name` 不冒充 legal issuer name，
  name/SIC 只有 consensus 才填 scalar，并固定为 retrospective；
- `universe_daily` 保留所有 active row，包括未解析 row；type code 原样保留，不用 current-only S2
  dictionary 回填历史。
- 四表都只声明相同六个 published upstream；cross-table FK/coverage 由 coordinated group build
  计算，sibling output 不进入任何 member 的 source inventory。

## 6. QA、quarantine 与预期 warning

Critical gate 至少包括：

- 六个 release 和 S4 release-set 的 trust chain、artifact checksum/bytes/rows/schema 全部重验；
- 四表 schema、PK、FK、deterministic ID、row funnel 与 atomic release-set；
- exact state-domain/state-matrix、所有 count 非负、固定 ID vectors 与六-release binding；v1 supersession
  row 与 replacement ID 必须为零/null；
- Composite → asset、Share-class → share class、CIK → issuer 的 ID 重算；
- `universe_daily` active-left exact preservation，不得把 conflict/unresolved membership 丢进 quarantine；
- ticker interval 的 XNYS 连续性、half-open endpoint、无内部 gap、无同日 ticker 多 asset；
- S5 不能单独定义 interval，S6 envelope 不能替代 daily evidence；
- current-reference metadata 不得标为 historical PIT factor。

当前已知 High review 项：

1. S6 169 条 `identity_evidence_unresolved` evidence quarantine：145 Share-class、21 CIK、3
   Composite；全部 `pending`，不能整体 waiver，也不能自动映射成 daily-row 状态；
2. active row 直接 Composite-FIGI coverage 只有 77.89%；
3. `CS` 直接 Composite-FIGI coverage 83.06%，剩余部分必须按 strong/candidate/unresolved 单列；
4. 三个 Composite → temporal Share-class transition；
5. 六个不同 Share-class lifecycle-envelope overlap ticker，尤其 `PKD`。
6. S5 event attach gate 必须报告 exact numerator；任何无法附着到 accepted S4/S6 Composite anchor
   的非零结果都要给 bounded examples 并触发 High review，不能据此新建永久身份。
7. S5 ticker event 的 alias association reason counts（非 ordinary date、pair mismatch、区间外、零/多
   命中）；只有 exact unique match 可影响 interval corroboration/count/availability。

这些 warning 的实际 numerator、issue ID 和 bounded examples 只能由批准后的 fixture/preview 生成；当前
schema approval 不预先接受任何 future warning 或 quarantine。

## 7. 下一审批点

当前检查点为：**S7 combined-source profile complete；four schema candidates awaiting explicit
approval；transform code not started**。

若批准，应逐字批准上表四个 Contract ID 与 Candidate file SHA-256，并同时确认
`universe_daily active-only` 设计。批准后的下一步仍只会实现 S7 source reader、resolution engine、
四表 contract resource 和固定小样本 fixture，然后停在 code-ready；不会直接跑十年 preview/full/publish。
