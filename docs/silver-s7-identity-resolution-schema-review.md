# S7 Identity Resolution：cross-market contamination 修订提案

## 1. 当前结论与硬停点

本次修订解决一个此前 schema 无法安全表达的问题：Massive 的 `locale=us` 股票记录可能混入同一
Share Class 的非美国市场 Composite FIGI；这不是公司身份变化，也不是 inactive/delisted。

当前仍严格停在 **schema proposal / candidate contracts / no execution**：

- 既有 bounded detector preview 保持原字节、ID 和 lineage，不重跑、不重写；
- 19 个 detector case 仍只是 finding，不是最终 adjudication；
- 本次没有生成这 19 个 case 的 adjudication plan；
- 没有生成或执行 S7 FullRunPlan、PublishPlan；
- 没有运行任何远端 S7 数据任务，也没有写远端数据盘；
- 第 11 节六份 contract 都是新的 **candidate，等待重新批准**，此前五份 approval 不自动覆盖本次修订。

保留的 preview 事实：

| 项目 | 值 |
| --- | --- |
| Preview ID | `306543f5fc1d30f868482392aaafdc781daf9f36f30d3f12504024c10f865c70` |
| Completion ID | `7a1e2386e18428aecf50a9ce322eaaf6b3035307b4a704939584288f131c6b9d` |
| 状态 | `awaiting_review` |
| Case / suspected rows | 19 / 89 |
| 输入范围 | 2022-02-01 至 2022-03-08，25 sessions，25 tickers |
| 物理输入 | 50 artifacts，1,471,768 rows，168,141,801 bytes |

## 2. 已核验的 9 个 cross-market group

OpenFIGI 官方 API 的 exact Composite 查询和 Share Class 反向查询均显式使用
`includeUnlistedEquities=true`。每组的 US/foreign Composite 都共享同一个 Share Class；US target 的
Composite market code 为 `US`，污染值的 code 为 `GR`、`EO` 或 `EP`。

| Ticker | Canonical US Composite | Observed foreign Composite | Code | Share Class | foreign scope | 当前 foreign / inverse rows |
| --- | --- | --- | --- | --- | --- | ---: |
| AZPN | `BBG000DFMXT3` | `BBG000KRLLH9` | GR | `BBG001S87NT0` | 2022-02-09…2022-03-02 | 15 / 2 |
| CR | `BBG000BG7423` | `BBG00CTGPFW0` | EO | `BBG001S5Q3X4` | 2022-02-09…2022-03-02 | 15 / 2 |
| FLOW | `BBG007FL7ZD2` | `BBG00K03RX51` | EO | `BBG007FL7ZF0` | 2022-02-09…2022-03-02 | 15 / 2 |
| SBGI | `BBG000F2XXP2` | `BBG000C3K505` | GR | `BBG001S7W602` | 2022-02-08 | 1 / 0 |
| SIRI | `BBG000BT0093` | `BBG000BGPKZ1` | GR | `BBG001S70ZY6` | 2022-02-08 | 1 / 0 |
| TA | `BBG000F71CC6` | `BBG000CVD896` | GR | `BBG001SHR063` | 2022-02-08 | 1 / 0 |
| TBLT | `BBG00LDFP150` | `BBG00YGNW2D3` | EO | `BBG00LDFP1X9` | 2022-02-09…2022-03-02 | 15 / 2 |
| TNXP | `BBG000LG8XM5` | `BBG00R4FG9L2` | EP | `BBG001T49NZ9` | 2022-02-09…2022-03-02 | 15 / 2 |
| WW | `BBG000DY6735` | `BBG000D08924` | GR | `BBG001SFWZR1` | 2022-02-08 | 1 / 0 |

所以当前 89 行应解释为：

- 79 行是 provider `locale=us` 中的 non-US Composite observation；
- 10 行是正确的 US Composite observation，只是被 foreign→US→foreign 的 inverse bounce 再次检出；
- 9 个 group 最终最多形成 9 个 cross-market series，不是 19 个 case decisions；
- 19 个 case 全部保留，只在将来作为 `contaminated_middle_episode` 或
  `inverse_middle_is_canonical_us` lineage 关联。

注意：79 是本次 bounded preview 的 foreign middle rows，不是未来 full-sequence scan 的全历史承诺。
更长的 foreign run 或完全不 bounce 的污染可能产生额外 finding；其范围只能由后续另行批准的完整扫描决定。

## 3. 官方证据与限制

OpenFIGI 的定义支持分层判断：

- Composite FIGI 聚合同一国家或市场内的 trading-venue FIGIs；
- Share Class FIGI 在全球范围连接同一 share class 的多个 Composite FIGIs。

本次冻结的 candidate evidence manifest：

- 文件：[`identity-cross-market-external-evidence-manifest.candidate.json`](silver/evidence/s7-cross-market/identity-cross-market-external-evidence-manifest.candidate.json)
- Manifest ID：`2ae779168e3e56887a5b0ae557bb928b6006c1b96392fe1606c201e1649ff848`
- Candidate SHA-256：`9544537ac7e6817c1b8f946c9ae2d5afb65399b1b553c3fe233a298614b375ab`
- Availability：2026-07-17；不得回填到 2022。

它逐文件保存 URL、relative path、raw bytes、byte count、SHA-256、capture time 和 media type，并冻结：

- OpenFIGI API 文档 HTML、Allocation Rules PDF；
- 三组原始 mapping request / response JSON 及去除 cookie/credential 的 allowlisted response headers；
- 4 份 SEC full submission、对应主文/Exhibit 与 EDGAR acceptance time；
- SPX FLOW issuer 页面及 allowlisted response headers；
- 每一个 mapping claim 的 request job index、response path、目标 Composite/Share Class/market code 与验证算法。

两个实现例外已固定测试：

1. TNXP foreign `BBG00R4FG9L2` 没有 `figi == compositeFIGI` self-row；唯一 venue row
   `BBG00R4FG9M1` 仍明确给出该 Composite 和 Share Class。因此验证器接受任一 exact relation row。
2. CR 的 direct response 有非目标 venue row 的 `shareClassFIGI=null`；验证只要求目标 relation 存在，
   并拒绝相同 Composite 下冲突的 non-null Share Class，不能要求整个 response 每行都 non-null。

OpenFIGI 是 current snapshot，不是 2022 point-in-time archive。它证明 hierarchy，但 provider contamination
结论还必须同时绑定 pinned Massive `market=stocks, locale=us`、ticker、primary exchange、source release 和
raw source rows。当前 OpenFIGI name/ticker 可能已漂移，不能作为历史身份真值。

公开行动日期也与 2022-02-08…2022-03-03 的跳变不符：AZPN 于 2022-05-16 consummated；CR 在
2022-02-28 仅签署且仍有生效条件（CIK `0000025445`）；FLOW 于 2022-04-05 closing；TBLT reverse split
于 2022-04-25 生效；TNXP reverse split 于 2022-05-17 生效。原始页面和 submission bytes 都在上述 manifest。

## 4. 旧 schema 的表示缺陷

旧 `identity_adjudication` 是一 case 一 series，并要求
`confirmed_provider_contamination.canonical_composite_figi == left_outer_composite_figi`。这对普通
US→foreign→US bounce 可能碰巧成立，但在 foreign→US→foreign inverse case 中，outer 正是污染值；若继续
使用旧 matrix，会把正确 US middle 错误映射回 foreign outer。

第二个缺陷是 bounce detector 只识别 `A→B→A`，无法发现一个 non-US Composite 在 Massive US locale 中
长期保持不变的情况。

修订原则：

1. provider observed identity 与 canonical research identity 永久分列；
2. canonical target 可以来自独立、不可变外部 evidence，不再由 outer A 决定；
3. correction 必须 exact scope match，绝不建立全局 `foreign FIGI → US FIGI` 字典；
4. 未批准的 known cross-market finding 保留 membership，但无 alias、不可回测；
5. 正确 US observation 无需 override，保持 direct eligible；
6. identity quality 不改变 active/inactive/delist，不发 forced-liquidation signal。

## 5. 架构选择：新增独立 cross-market registry

本提案保留原 `identity_adjudication` 表和 contract 不变，新增第六张 upstream registry：

`identity_cross_market_adjudication`

不把 cross-market group 硬塞进旧表的理由：

- 旧表、external evidence 和 series 都强绑定单个 bounce case；
- 长期不 bounce 的污染没有合法 `identity_case_id`；
- 保持旧表不变能完整保留既有 preview/case/evidence bytes、IDs 和 replay 行为；
- 两类 registry 的职责清晰：旧表处理真正 episode transition；新表处理 provider-locale market mismatch。

新的控制链为：

```text
six exact S4/S5/S6 releases
        ├── preserved bounce preview / cases
        └── full-sequence market-consistency detector
                    │
                    ▼
identity_market_consistency_candidate_manifest
        + immutable cross-market external evidence
                    │
                    ▼
separately approved cross-market plan / row receipt
                    │
                    ▼
identity_cross_market_adjudication release
                    │
                    ▼
exact cross-market scope → inverse-US direct rule → ordinary bounce rule
                    │
                    ▼
four coordinated derived tables
```

本次只定义 contract、fixture 和 evidence；没有创建图中的 candidate manifest、plan、approval 或 release。

## 6. Cross-market override 的 exact scope

一条 effective override 至少绑定：

- `provider_id=massive`、`provider_market=stocks`、`provider_locale=us`；
- exact case-sensitive ticker；
- exact Share Class FIGI；
- exact observed foreign Composite FIGI 与 independently evidenced canonical US Composite FIGI；
- inclusive `valid_from_session / valid_through_session`；
- sorted exact S4 `source_record_ids`、count 和 set digest；
- exact S4 release-set 和 six-release binding；
- full-sequence candidate manifest ID/SHA/availability；
- external evidence manifest ID/SHA/claim digest/availability；
- related case IDs 和每个 case 的 role（允许零个，用于 non-bounce finding）；
- append-only version/predecessor；
- literal approval request、row-specific receipt、actor、time 和 availability；
- `adjudication_available_session=max(candidate,evidence,approval)`。

版本模型分成两层：稳定 `cross_market_subject_id` 只由 provider、market、locale、ticker、Share Class 和
observed foreign Composite 生成；`cross_market_series_id` 始终从 subject 生成。canonical target、日期区间、
source release/source-record set、candidate 和 disposition 属于 version-specific `cross_market_scope_id`。
因此 successor 可以在同一 append-only series 中修正 target/scope，或以
`cross_market_adjudicated_unresolved`（canonical=null、override=false）撤回先前映射；predecessor 永久保留，
cutoff 只选择唯一 available terminal head。不同 stable subjects 的 effective scopes 才做 overlap hard gate。

匹配必须同时满足全部 scope 维度。相同 foreign Composite 在真实 foreign locale、不同 ticker、不同 Share
Class、日期区间外、不同 source release 或不在 exact source-record set 中时均不命中。

## 7. 四张派生表的变更

### `asset_master`

- 新增 cross-market override evidence row count 和 distinct adjudication count；
- canonical target 可由 approved immutable external identifier assertion 独立 anchor；
- 因此 purely external anchor 的 first/last direct observation 可为 null；
- unapproved foreign finding 不能创建 asset；
- foreign hierarchy evidence 不能污染 canonical Share Class/issuer。

### `ticker_alias`

- 新增 observed/canonical market code、cross-market scope/decision/availability、case role；
- foreign observation 只有 exact approved scope 才生成 canonical US alias；
- inverse US middle 保持 `observed=canonical`、`canonical_override=false`、direct alias；
- 真实 foreign-locale row 永不受 US scope 影响。

### `issuer_master`

- cross-market-contaminated CIK/name/SIC 只保留 observed lineage；
- excluded cross-market evidence 独立计数；
- 不把 foreign observation 的 hierarchy 自动迁移到 canonical US issuer。

### `universe_daily`

- 保留每个 active membership row 和原始 observed FIGI；
- 新增 market classification、scope/decision/availability 和 case role；
- resolution priority 固定为：
  1. exact approved cross-market foreign match；
  2. externally classified correct inverse US direct observation；
  3. ordinary bounce/case adjudication；
- pending cross-market finding 保留 membership，但 canonical/alias 为 null、ineligible；
- `identity_quality_liquidation_signal=false`，不得把 identity uncertainty 当作 delist。

四表共同新增 exact market-consistency candidate 和 cross-market registry release binding；仍需 coordinated
sibling build 才能发布。

## 8. Resolution matrix

| Observation | Cross-market decision | canonical | eligible | 说明 |
| --- | --- | --- | --- | --- |
| US→foreign→US 中的 foreign row，已批准 | confirmed contamination | independent US target | ordinary gates 通过时 true | raw foreign FIGI 保留 |
| US locale 的 known foreign row，未批准 | pending cross-market review | null | false | 不静默猜测 |
| 已批准映射后追加 withdrawal | cross-market adjudicated unresolved | null | false | append-only 撤回，不改旧 row |
| foreign→US→foreign 的 US middle | observed consistent / inverse-US direct | observed US | ordinary gates 通过时 true | 不是 genuine transition，不 override |
| long-lived foreign、US locale、无 bounce | finding；批准前 pending | null | false | full-sequence detector 能发现 |
| 同一 foreign Composite、真实 foreign locale | ordinary direct | observed foreign | ordinary gates 决定 | US scope 不泄漏 |
| 任一关系 conflict | resolved conflicted 或 unresolved | 已确认 target 或 null | false | FIGI 裁决不绕过其他 gate |

## 9. 必须冻结的 QA

用户要求的三个新 gate：

- `us_locale_non_us_composite_figi_rows`：High warning；全序列统计，必须有 reason counts、reference coverage
  和 bounded examples，不限于 bounce；
- `unapproved_cross_market_composite_eligible_rows = 0`：Critical；
- `inverse_bounce_misclassified_as_genuine_transition_rows = 0`：Critical。

同时加入：

- `cross_market_override_scope_mismatch_rows = 0`；
- `cross_market_scope_overlap_rows = 0`；
- `cross_market_override_outside_us_locale_rows = 0`；
- `correct_us_observation_overridden_rows = 0`；
- `cross_market_target_evidence_invalid_rows = 0`；
- `cross_market_registry_binding_invalid_rows = 0`；
- `identity_quality_membership_mutation_rows = 0`；
- `identity_quality_forced_liquidation_signal_rows = 0`；
- `figi_market_classification_uncovered_rows`：High warning。

最后一项很重要：本次 9 组 evidence 不能伪装成全市场 identifier reference。未来若要声称全历史扫描
coverage，必须先固定有版本、可审计覆盖率的 Composite market-classification reference；unknown 不能当 clean。

## 10. 固定测试

代码 fixture 已覆盖：

1. US→foreign→US：只 override foreign row，raw observed 永久不改；
2. foreign→US→foreign inverse：US middle direct eligible、override=false、genuine count=0；
3. locale=us、MIC=XNAS/XNYS 的长期 foreign Composite：无 bounce 仍被发现，未批准 fail closed；
4. 同一 foreign Composite 在真实 foreign locale：合法 direct identity，不被全局覆盖；
5. exact scope、release、source-record、evidence 和 approval availability；
6. fixed scope/series/decision ID vectors；
7. 25 份外部 raw artifacts 的 bytes/SHA replay；
8. 18 个 exact Composite relations 和 9 个 Share Class reverse projections；
9. TNXP no-self-row 与 CR nullable non-target relation 两个 API 例外；
10. membership 不变、无 forced liquidation。
11. 同一 stable subject 的 append-only withdrawal：series 不变、version-specific scope 可变、cutoff 前后选择正确。

fixture 仍不是 production ingress，也没有读取或改写远端 S7 数据。

## 11. 新的 candidate contracts 与 hashes

以下六份组成 2026-07-17 exact-approved package；`identity_adjudication` 本身 byte-for-byte 不变，但整体
S7 contract set 已扩展：

| Table | columns / QA | Contract ID | Arrow schema digest | Candidate SHA-256 |
| --- | ---: | --- | --- | --- |
| `identity/identity_adjudication` | 51 / 19 | `6423cc01b952498cc78d55e93a349d7afe408bd30003e4f7be59f211102f2d5e` | `e5082a8611bedb6913f79da506f1f5cc19c94507b9e27d04edfb88566033575f` | `eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231` |
| `identity/identity_cross_market_adjudication` | 60 / 24 | `ae91c7b1bfc27bde82e5f5a39afdc5a3c2c9929d075486cb081836b6798e14e8` | `96fe9108cd246919a9a00855d04d9f4057c439b6043d4d67178beb1c32d7a0fe` | `a7308e22c07e8243a8587bfc7eab7ae45b2f232fe9bba310d084916d722f56d0` |
| `identity/asset_master` | 46 / 37 | `959c5f7bf464eed59fd32a7008349f60ebcfd3cf9e892c9c3d7f00080eae2149` | `5ef86bbe8e3e0219e795ed9f8c5c9eca35ebc7b16ff21a903901765b3e7d53d3` | `bfb31004df41c4556e71beb379bb36e07063f36298d329c887be48c005b02fa5` |
| `identity/ticker_alias` | 54 / 49 | `39dbf6ef89ed4c2d466fa0be2e47d2840a90f1a97f6a47670af05df3e15513ce` | `2f857bc07319426e48494901571a570b1abf622c16c9e429ab8185c08af2d743` | `8bf758af5c358c79477ff40177aab5f3b7c8d26f7f0882e261f7d844a66a1f95` |
| `identity/issuer_master` | 35 / 35 | `2faa8d4d2e10e4a065b10b9ae851e53ac517db7e69af4fd59d5f6edc677aa408` | `dac9dbe43450cf094c8170d8e88db1742fb035052df9d1b78b7ced02cc4282d2` | `adee0a5457ac32356a0ec9b9a28c692fcebdacc4ba9cccedd1237e8c66b722b7` |
| `reference/universe_daily` | 59 / 55 | `38cd59c4e4b04de8444ba99ed93e6fd8c7a78aec24f01205d7df7494bcfd33d3` | `80902539df5dc822dc43a88cf7325b16f4fdc2c4c6786c78ea93434116e6e25a` | `c0923508dafa0d56de4be6b8ff43187a581627dd1d64e964cf5f506f5ce8ea0b` |

Candidate files：

- [`identity_adjudication.schema-v1.candidate.json`](silver/contracts/identity/identity_adjudication.schema-v1.candidate.json)
- [`identity_cross_market_adjudication.schema-v1.candidate.json`](silver/contracts/identity/identity_cross_market_adjudication.schema-v1.candidate.json)
- [`asset_master.schema-v1.candidate.json`](silver/contracts/identity/asset_master.schema-v1.candidate.json)
- [`ticker_alias.schema-v1.candidate.json`](silver/contracts/identity/ticker_alias.schema-v1.candidate.json)
- [`issuer_master.schema-v1.candidate.json`](silver/contracts/identity/issuer_master.schema-v1.candidate.json)
- [`universe_daily.schema-v1.candidate.json`](silver/contracts/reference/universe_daily.schema-v1.candidate.json)

## 12. 审批结果与下一门

第 11 节六份 exact Contract ID / Candidate SHA-256 与 evidence manifest ID/SHA 已于 2026-07-17 按原文
批准。approval 只接纳 schema/evidence，不授权 scan、adjudication、registry、四表 materialization、Full
或 Publish；evidence manifest 的原始 `candidate_not_approved` bytes 不改写，runtime 由独立 aggregate
receipt 表达 package approval。本次 9 个 group 仍只是 schema/evidence representation，
`future_adjudication_status` 继续为 `not_planned_not_approved`。

设计复核确认，当前 18 个 Composite relationship 只能作为 seed，不能冒充全市场 classification
reference。因此不能直接提出 `identity_market_consistency_candidate_manifest` scan。下一门只生成完整
S4 Composite denominator 的 Gate A Plan 与 approval Request，并停在 literal gate；Gate A execution、
OpenFIGI reference acquisition、market-consistency scan、adjudication、Full 和 Publish 均未获授权。三门
依赖及 unknown fail-closed 规则见
[`silver-s7-market-reference-prerequisite-plan.md`](silver-s7-market-reference-prerequisite-plan.md)。
