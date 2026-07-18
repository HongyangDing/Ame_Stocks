# S7 三案例 directional raw preview：review-only schema proposal

## 1. 当前状态与边界

本文件冻结 SOR、XZO、ANABV 三个已知 Share Class 冲突案例的下一道只读审查结构。
当前只完成：

- review-only slot candidate contract；
- packaged schema resource；
- 固定 11-pair scope constants；
- registry 职责互斥语义；
- 本地、非执行型 ScopeSet / PreparationPlan / Request 控制面；
- contract、Plan、Request 测试。

当前没有实现或授权 runner、approval receipt、远程 preview、external evidence capture、exact-group
全历史补读、adjudication plan、registry release、四表 materialization、Full 或 Publish。刚完成的
`composite_figi_inventory` 继续有效，不重跑。

## 2. 精确逻辑范围

本 preview 是 11 个明确的 `(ticker, session_date)` pair，不是 3 tickers × 11 sessions 的 33 行笛卡尔积：

| Ticker | Inventory anchor（只作选案上下文） | 精确 sessions |
| --- | --- | --- |
| SOR | `BBG000KMY6N2` | `2024-12-31`, `2025-01-02`, `2025-01-03` |
| XZO | `BBG01XL8FHT0` | `2025-11-04`, `2025-11-05`, `2025-11-06`, `2025-11-07` |
| ANABV | `BBG021DMXXT2` | `2026-04-06`, `2026-04-07`, `2026-04-17`, `2026-04-20` |

11 个日期互不重复，因此物理输入预期为 22 个 daily S4 artifacts：

- `asset_observation_daily`：11 个；
- `universe_source_daily`：11 个。

逻辑过滤只允许 exact ticker + session。`inventory_anchor_composite_figi` 不能参与过滤，否则会漏掉同一
ticker 在边界另一侧的新 Composite 或 Share Class。

固定 scope digest：

`c232e8b7c910d8bb0fe6c82e101c075f5ea1d0ce5845acd8dede4ec2b1ffd6ea`

## 3. 为什么不用既有 bounce preview

既有 detector preview 使用连续 `start_session/end_session`，并将 allowlisted tickers 应用于范围中的每个
session。它还以 active selected-parent pair 为 bounce 证据。当前审查则必须同时表达：

- 非连续 session matrix；
- present active、present inactive 和 source membership absent；
- 每个 ticker/session 的所有 matching `asset_observation_daily` versions；
- `universe_source_daily.selected_source_record_id` 的唯一 parent；
- 不经过 Composite、Share Class、active 或 canonical 过滤的完整 source lineage。

因此本提案使用独立的 review-only contract，不能把三案硬塞进 bounce detector/case schema。

## 4. Slot contract

候选合同：

- Table：`identity_directional_raw_preview_slot`
- Grain：固定 scope 中每个 `review_case_id, session_date` 一行；source membership absent 也保留一行
- Primary key：`review_case_id, session_date`
- Sort：`ticker, session_date`
- Columns：42
- QA：34

固定标识：

| 项目 | 值 |
| --- | --- |
| Contract ID | `b475ee2c9745791aae83908c0b6b6380724a34db132b194315ccae1a72ca1366` |
| Arrow schema digest | `fc9a81955b3fe0c79545902c496cc4320df1b7d91f57c5a91e7498657a6cb1af` |
| Candidate/resource SHA-256 | `e9c54a61ed5f65b522ba8362268a44966a6620908182e9059bc519c43086d3f6` |
| QA semantics digest | `73aa1e615f5094cb1923e35083cb58536c5f43a5c1ebf1c524d513beaa32ff44` |

文件：

- `docs/silver/contracts/identity/identity_directional_raw_preview_slot.schema-v1.candidate.json`
- `backend/ame_stocks_api/silver/schema_resources/identity_directional_raw_preview_slot.schema-v1.json`

两份 JSON 必须 byte-for-byte 相同。合同记录：

1. exact provider/ticker/session scope；
2. universe membership 是否存在及 selected identity fields；
3. 全部 matching Asset versions 的 attestation IDs；
4. selected Asset parent 的唯一性和 projection 对账；
5. case evidence manifest ID/path/SHA；
6. direction-only、registry-not-evaluated 和全部 false capability markers。

它没有 `asset_id`、canonical Composite/Share Class/CIK、issuer、disposition、override interval、transition
decision 或 registry decision 字段。

## 5. Source evidence 与 parent reconciliation

每个 retained S4 row 必须使用现有 `ProviderRowAttestation` schema version 2。其 full-row snapshot 和
物理 locator 共同绑定：

- exact six-release/S4 release；
- Table contract ID 与 Arrow schema digest；
- Parquet artifact path/SHA；
- row group 与 row index；
- primary key、source record/request IDs；
- full-row digest；
- source capture 与 availability；
- frozen XNYS calendar。

`universe_source_daily` 每个 exact ticker/session 允许 0 或 1 行。0 行输出
`membership_status=absent_source_membership`，不能被合成为 inactive。存在 universe row 时：

- `selected_asset_parent_match_count` 必须等于 1；
- selected parent 必须通过现有 S4 observation-parent projection；
- 所有 nonselected Asset versions 仍进入 case evidence；
- Asset 与 universe 的 source availability 时间线保持各自语义，不强行相等。

`active_on_date` 在这里仍只是 provider membership 事实，不是最终交易资格，也不能据此触发强制平仓。
后续 `final_tradability_eligible` 还必须独立结合 security-type、价格/流动性和 entitlement/corporate-action
policy；本 preview 不产生该字段。

## 6. Direction 与 interval 限制

每行固定：

```text
interval_inference_state = direction_only_not_exact_scope
```

这 11 个 sampled sessions 可以确认观测方向，但不能推断未采样日期中的 exact
`effective_from/effective_to`。特别是 SOR 的 provider Composite 滞留终点和 ANABV 的连续范围，仍可能需要
后续单独审批的 exact-group 历史补读。

## 7. Registry 职责互斥

未来 Composite correction registries：

1. `identity_adjudication`：bounce episode；
2. `identity_cross_market_adjudication`：跨市场 Composite 污染；
3. `provider_composite_override`：真实 transition 后、同市场 exact-scope stale Composite。

非 Composite correction：

- `share_class_adjudication` 只修正 Share Class，不能改变或产生 `asset_id`；
- `asset_transition` 只表达 predecessor/successor relation，不执行 override。

同一 cutoff 下同一 source row 若命中多个 Composite correction registry，未来派生层必须保留 membership、
不生成 canonical/alias，并令 identity 不可回测；不能按 priority、最新、最长或多数规则自动选择。
Share Class 修正只能在 canonical Composite 唯一后应用。

未来实际加载 registry 时，collision QA 必须拆分，避免把“保留 unresolved 行”和“整次构建必须零
collision”混成同一规则：

- `multi_registry_composite_override_collision_rows`：High/review，报告原始命中数、reason counts 和
  bounded examples；允许 candidate 保留这些 unresolved 行；
- `multi_registry_composite_override_collision_eligible_rows = 0`：Critical；
- `multi_registry_composite_override_collision_resolved_rows = 0`：Critical；
- `multi_registry_composite_override_collision_alias_rows = 0`：Critical。

因此 collision candidate 可以保留 observed lineage 和 membership，但不能保持 identity eligibility、被自动
resolved 或生成 alias；未经显式 collision review acceptance 也不能进入 Full/Publish。

本 preview 不加载上述 registries，因此固定：

```text
registry_evaluation_state = not_evaluated
```

它不得把 raw collision count 或 collision-eligible/resolved/alias 指标伪报为 0。Registry exclusivity
semantics digest 由 `identity_directional_raw_preview_contract.py` 的固定语义对象内容寻址；任何上述 QA
变化都会改变 digest。当前 digest：

`d2edbfe9420da8ceca4fe40b6b5a12df381fece7198763dba94658242ceb9d5d`

## 8. QA surface

26 个 Critical gate 覆盖：

- fixed 11-pair scope、inventory/S4/calendar/artifact/source-count binding；
- exact-pair no leakage、所有 matching source rows 无遗漏；
- universe 唯一性、selected parent 唯一性与 projection；
- ProviderRowAttestation v2、物理 replay、无 orphan/duplicate；
- observed source row 不变；
- direction-only、registry not evaluated；
- 无 canonical identity、adjudication、transition decision；
- row capabilities 全 false；
- PK、sort、artifact readback 与 resource caps。

8 个 High/review metric 覆盖：

- missing membership；
- Asset-only source rows；
- nonselected Asset versions；
- same-session identity variants；
- directional Composite/Share Class edges；
- sampled gaps；
- inventory anchor 在 sampled slot 中未出现。

这些 review metric 可以非零；它们不会被静默纠正。

## 9. Capability boundary

本 schema package 的所有 capability 均为 false：

```text
preview_execution
exact_group_history_read
external_evidence_capture
adjudication_plan_generation
registry_materialization
canonical_identity_materialization
full_run
publication
```

本 checkpoint 的 PreparationPlan/Request 也不可执行：它们只请求批准实现并冻结未来 executable package，
不授权读取 22 份日分区。当前 Request 没有 approval recorder、runner、run CLI、exact physical artifact refs
或 completion manifest SHA；其 literal 即使获批，也只允许准备这些代码和新的 execution controls。

真正执行 preview 前仍须再生成一份绑定 runner/approval/run CLI、exact 22 artifact refs、completion lineage、
clean Git commit/tree、runtime/test file sets 的 execution Plan/Request，并等待新的逐字 execution approval。
未来 Preview completion 必须停在 `awaiting_review`。

## 10. 后续输出设计（尚未实现）

未来 executable package 获得单独 execution approval 后，独立 runner 才可生成：

```text
directional-raw-preview-case-evidence/<review_case_id>/<manifest_id>/manifest.json
directional-raw-preview-candidates/<candidate_id>/manifest.json
directional-raw-preview-candidates/<candidate_id>/data/review-slots.parquet
directional-raw-preview-candidates/<candidate_id>/review/directional-sequences.json
directional-raw-preview-candidates/<candidate_id>/qa/qa.json
directional-raw-preview-candidates/<candidate_id>/examples/review-anomalies.json
directional-raw-preview-completions/<plan_id>/<approval_id>/manifest.json
```

这只是 output contract 设计，不是文件创建或执行授权。SEC、issuer、OpenFIGI 原始 bytes 和两条
availability 时间轴在 raw preview review 后进入独立 external-evidence gate，不能回填成历史事件时点。
