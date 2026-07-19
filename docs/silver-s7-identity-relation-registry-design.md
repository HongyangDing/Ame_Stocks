# S7 三组 identity relation registries：schema 与控制设计

## 1. 结论与边界

Exact-group full-history review 将三个问题分成三种互不替代的事实：

| Ticker | Full-history fact | Registry action |
| --- | --- | --- |
| SOR | 旧 Composite `BBG000KMY6N2` 从 2024-12-31 一直被观察到 2026-07-09；Share Class 在 2025-01-02 从 `BBG001S5W848` 变为 `BBG01RK6N5G9` | 一条真实 `asset_transition`；2025-01-02 至当前 S4 截止日的旧 Composite 行另走 exact-scope `provider_composite_override`，目标为 `BBG01RK6N4M5` |
| XZO | Composite 始终为 `BBG01XL8FHT0`；临时 Share Class `BBG01XL8FJS7` 只在 2025-11-04 至 2025-11-05 出现，之后为 `BBG01227MF17` | exact-scope `share_class_adjudication`；不删除 11 月 4 日 membership，不产生 transition |
| ANABV | 独立 Composite `BBG021DMXXT2`；普通 ANAB Share Class `BBG0026ZDHT8` 只在 2026-04-06 出现，之后为 ANABV 的 `BBG021GNPBR6`；4 月 20 日 inactive | 仅修正 4 月 6 日 Share Class。ANABV 仍是独立 temporary asset；不并入 ANAB，不产生 transition，inactive 不传播到 ANAB |

上述 observed rows、FIGI、active 状态及 source lineage 永久保留。三个 registry 都不产生最终
`final_tradability_eligible`，也不把 identity quality 解释为 inactive、delisted、零收益或强制平仓。

## 2. 职责互斥

Composite correction registries 只有：

1. `identity_adjudication`：deterministic bounce middle episode；
2. `identity_cross_market_adjudication`：US locale 中的非美国 Composite 污染；
3. `provider_composite_override`：真实 transition 后，同一市场中的 provider stale Composite。

`share_class_adjudication` 只允许
`observed_share_class_figi → canonical_share_class_figi`；它必须在 canonical Composite 唯一确定后应用，
不能创建或改变 `asset_id`、Composite、CIK 或 issuer。

`asset_transition` 只表达 predecessor/successor relation。它不执行 Composite/Share Class override，
不自动拼接收益，也不改变 membership 或 tradability。

同一 cutoff 下，一个 source row 同时命中两个或以上 Composite correction registries 时，不使用 priority、
最新、最长或多数规则。原始 collision 进入 High review；该行必须同时满足：

```text
backtest_identity_eligible = false
identity_resolved = false
alias_allowed = false
```

因此 QA 分开为 raw High count，以及 eligible/resolved/alias 三个 Critical zero gates。

## 3. 三张 contract

### `provider_composite_override`

Grain 是一个 stable subject 的一版 approved exact-source-row decision。Stable subject 绑定 provider、market、
locale、ticker、observed Composite 和 `asset_transition_series_id`；canonical target、日期、S4 release、完整
source-record set、candidate 和 exact transition decision 都属于 version payload。

Confirmed row 必须：

- observed/canonical market code 都是 `US`；跨市场 observation 必须由 cross-market registry 处理；
- canonical Composite 与 observed 不同；
- 绑定一个 approved genuine `asset_transition_id`；
- 只匹配 exact provider/ticker/Composite/session/S4 release/source-record scope；
- availability 为 candidate、transition、external evidence 和 approval availability 的最大值。

SOR v1 scope 只能从 2025-01-02 开始，不能包含仍合法的 2024-12-31 predecessor row；当前
`valid_through_session=2026-07-09` 是 S4 release-censored boundary，不是开放式永久规则。未来 S4 release
扩展必须追加 successor decision，不能修改 v1。

### `share_class_adjudication`

Stable subject 绑定 provider、market、locale、ticker、observed Composite 和 observed Share Class。Decision
另绑定 required unique canonical Composite、canonical Share Class、exact interval/release/source-record set、
evidence 与 approval。

- XZO：`2025-11-04..2025-11-05`，`BBG01XL8FJS7 → BBG01227MF17`；
- ANABV：仅 `2026-04-06`，`BBG0026ZDHT8 → BBG021GNPBR6`。

如果 Composite 未唯一确定或多条 Share Class decision 命中，同一 membership row 仍保留，但 hierarchy
保持 conflicted，不能由该 registry 生成 alias 或 eligibility。

### `asset_transition`

Stable subject 绑定 provider、market、locale、ticker、transition type 与 legal effective date。Decision version
绑定 predecessor/successor Composite 与 deterministic asset IDs、边界 session、exact S4 boundary rows、外部
evidence 和 approval。

SOR 表示为：

```text
predecessor = BBG000KMY6N2
predecessor_last_session = 2024-12-31
legal_effective_date = 2025-01-01
successor = BBG01RK6N4M5
successor_first_session = 2025-01-02
relationship_effect = lineage_only_no_override_no_return_stitching
```

XZO 没有 predecessor/successor 变化；ANABV 是 entitlement 不同的临时证券，也不生成到普通 ANAB 的
transition。未来如需累计收益连续，必须经过独立 entitlement/corporate-action accounting，而不是消费本表后
直接拼接价格。

## 4. 两条时间轴

历史事实时间包括 legal effective date、predecessor/successor session 和 exact source interval。研究可用时间
包括 source candidate、外部资料实际抓取、row approval 和 registry publication。2026 年抓取的 OpenFIGI
结果可以支持 retrospective identity correction，但不得伪装成 2024/2025/2026 当时已知的因子输入。

每个 decision availability 由冻结 XNYS calendar 重算；每个 registry release availability 不得早于其中任何
decision。Cutoff resolver 只选择 available、完整且 append-only version chain 中最高的一版；withdrawal 通过
追加 `*_adjudicated_unresolved` successor 表示，旧 decision 永久保留。

## 5. 固定 contracts

| Table | columns / QA | Contract ID | Arrow schema digest | Candidate/resource SHA-256 |
| --- | ---: | --- | --- | --- |
| `provider_composite_override` | 52 / 21 | `a090c4ed150b2f59c38b4f01791f70ce655d44e9c3576bd0a13ac7fd9ba32bc5` | `a79e9774d9915fc223b2ff2cea2f7c665892abcd175d70abd8bea04cfdc0bd4c` | `1e87d4c5d61a973eddd1e2b39e2d6c56f5405a1aedd451597067eaef192506eb` |
| `share_class_adjudication` | 51 / 24 | `5918ade4aaca64372cbb9de70297dce042ef39da4fd3186b174c4c687edd2919` | `9a2580dbc02fa76658e4a9f7ae4f01efb823e756c52f92b29627abb16c6b1589` | `004abaea381e3897d383b3d4e90d9a13336f153f7cd892c2a4bc34101026eabd` |
| `asset_transition` | 51 / 20 | `8831443729fe360c3b4265595a2bd74c8a8b9031cb6f6ca30ee0ac4e1beef7ac` | `668f9c1d747f5de6dcd62c517a524590d5c45a571f92ce3bad65e8aea9ca5a4e` | `7694dc99a5d92ed99e7c6e22dd2625ea0e9029b4a8abda707006ef1892ec3024` |

Candidate 与 packaged resource 必须 byte-for-byte 相同。当前 package 只定义 schema、pure models、fixture
semantics 和 release envelope；它没有生成任何 production decision、approval receipt 或 registry release。

## 6. 后续控制链

1. 固化 exact-group candidate/completion 与新的 SEC、issuer、OpenFIGI raw evidence manifest；
2. 审批三份 schema contract 与 evidence manifest；
3. 先生成并逐行审批 SOR `asset_transition`，发布 transition release；
4. 生成并逐行审批 SOR `provider_composite_override`，显式绑定上一步 transition release；
5. 独立生成并审批 XZO、ANABV 两条 `share_class_adjudication`；
6. 分别发布三个 append-only registry release；
7. 用一个 content-addressed coordinated release bundle 绑定现有 episode/cross-market registries、三个新 release、
   exact cutoff 与 calendar；
8. 此后才允许四表 candidate materialization，并在 collision、availability、lineage、membership 与 no-forced-exit
   gates 全部通过后进入 Full/Publish。

任何 schema 文件存在都不构成 decision、release、四表 materialization、Full 或 Publish 授权。
