# S7 identity registry 决策与发布控制

## 1. 目的与当前边界

本设计实现五份 registry 的统一、不可变、fail-closed 工作流：

1. `identity_adjudication`；
2. `identity_cross_market_adjudication`；
3. `provider_composite_override`；
4. `share_class_adjudication`；
5. `asset_transition`。

执行链固定为：

`candidate → decision plan → literal approval request → approval receipt → release`

实现位于：

- `backend/ame_stocks_api/silver/identity_registry_workflow.py`；
- `backend/ame_stocks_api/cli/silver_identity_registry_workflow.py`；
- `tests/test_silver_identity_registry_workflow.py`。

本文描述控制能力和固定 case 约束；它本身不证明某次远端 candidate、release、四表
materialization、Full 或 Publish 已经执行。

## 2. 当前授权与 production 边界

当前代码固定的五份 registry contract pin 为：

| Registry | Contract ID | Resource SHA-256 |
|---|---|---|
| `identity_adjudication` | `6423cc01b952498cc78d55e93a349d7afe408bd30003e4f7be59f211102f2d5e` | `eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231` |
| `identity_cross_market_adjudication` | `ae91c7b1bfc27bde82e5f5a39afdc5a3c2c9929d075486cb081836b6798e14e8` | `a7308e22c07e8243a8587bfc7eab7ae45b2f232fe9bba310d084916d722f56d0` |
| `provider_composite_override` | `a090c4ed150b2f59c38b4f01791f70ce655d44e9c3576bd0a13ac7fd9ba32bc5` | `1e87d4c5d61a973eddd1e2b39e2d6c56f5405a1aedd451597067eaef192506eb` |
| `share_class_adjudication` | `5918ade4aaca64372cbb9de70297dce042ef39da4fd3186b174c4c687edd2919` | `004abaea381e3897d383b3d4e90d9a13336f153f7cd892c2a4bc34101026eabd` |
| `asset_transition` | `8831443729fe360c3b4265595a2bd74c8a8b9031cb6f6ca30ee0ac4e1beef7ac` | `7694dc99a5d92ed99e7c6e22dd2625ea0e9029b4a8abda707006ef1892ec3024` |

用户已经给出 standing authorization，并以最新的“批准”再次确认。Production 路径可以按固定顺序
自主生成和发布单个 exact registry，但每一步仍必须记录并重放：

- 用户 standing literal 和 reaffirmation literal 的固定 bytes；
- contract、source candidate/completion、external evidence 三类 exact target-bound prerequisite
  authorization receipts；
- 当前 clean Git runtime、固定 XNYS calendar、canonical production root 和全部输入 refs；
- candidate/plan/request 的完整不可变控制链及零 Critical、零 factual contradiction 的 standing QA。

只要出现 Critical QA、事实矛盾、资源越界、scope/receipt mismatch 或 runtime drift，就必须停止；不能
把 standing authorization 扩展到 source scan、scope mutation、network、materialization、Full 或
Publish plan。Evidence manifest 中的 `candidate_not_approved` 表示 evidence 本身不执行 decision；真正
的准入由上述 exact prerequisite receipt 和后续 standing release receipt 完成。

## 3. Candidate 冻结内容

每个 `RegistryDecisionCandidate` 必须包含：

- 一个明确 registry 和 case key；
- 预期最终 decision ID、版本和 supersedes ID；
- contract 中除审批后字段以外的完整 row claims；
- 每个 source row 的完整 scope snapshot：
  `provider/market/locale/ticker/session/observed Composite/observed Share Class/MIC/`
  `source_record_id/S4 release`；
- source record count、ID-set digest 和 full-scope digest。

只给日期区间、ticker 或行数不足以生成 candidate。每一行必须来自一个精确绑定且可重放的上游
candidate/completion；workflow 不扫描目录，也不发现 `latest`。

Candidate 还逐字节绑定：

- source candidate manifest；
- external evidence manifest；
- schema contract approval、source candidate approval 和 external evidence approval 三类不可变
  approval artifacts；
- contract ID、Arrow schema digest、contract resource SHA；
- XNYS calendar ID/SHA；
- availability。

Canonical production candidate 还必须包含一份 `production_ingress_attestation`。该 attestation 绑定
production root、当前 clean runtime、contract、source/evidence/authorization refs、calendar 以及可选
的 exact `asset_transition` release；loader 每次重建固定 decisions，并拒绝没有 provenance 的历史或
fixture candidate。External evidence 不能只验证 manifest：其中每个 raw JSON/HTML/PDF/header artifact
都按 path、regular-file/no-symlink、bytes 和 SHA-256 重放。

Production external evidence 先经过独立的 content-addressed import：代码只允许两条固定 repository
manifest 路径，从当前 clean runtime 的 exact Git commit 读取 manifest 和全部 raw blobs，再不可变复制到
canonical data root。Import receipt 绑定 Git commit/tree、完整 runtime file set、manifest、raw
path/bytes/SHA、导入时间和 operational availability。调用方不能提供 repository path、任意 evidence
ref 或 `latest`；`prepare-fixed-request` 只重放该固定 import。

`candidate_available_session` 必须等于 candidate 创建时间后的第一个 XNYS open 和所有上游
availability session 的最大值。

## 4. Plan、literal 与审批

Decision plan 只引用已写入并重读验证的 candidate，不复制或重新解释 decision。Plan 状态固定为
`awaiting_review`，并明确 `release_authorized=false`。

Approval request 输出一个无歧义 JSON literal，action 固定为：

```text
approve_exact_s7_registry_candidate_and_release_once
```

Literal 包含 registry、candidate ID/path/SHA/bytes、plan ID/path/SHA/bytes、完整 decision ID set、
source-scope-set digest、contract 三重 pin 和 calendar pin。Receipt 只有在调用方提供的 JSON bytes 与
该 literal 完全相等时才能生成；审批 availability 必须是审批时间后的第一个 XNYS open。

Fixture/internal 路径仍只接受某次 request 的 exact literal approval。Production standing 路径不接收
调用方自造 literal 文件、时间或 availability；代码内部使用固定 standing/reaffirmation bytes，运行时
采样时间，并为当前 exact request 生成 target-bound standing receipt。一般性“继续”仍不能替换这些
不可变 receipts。

## 5. Release 物理结构与双重重放

每份 release 的结构为：

```text
manifests/silver/identity/registry-releases/
└── registry=<registry>/release_id=<release_id>/
    ├── manifest.json
    ├── data/decisions.parquet
    └── decisions/decision_id=<decision_id>.json
```

`manifest.json` 最后写入，是唯一可见性边界。它冻结：

- registry name 和 release ID；
- contract pin；
- candidate、plan、request、receipt 的 exact artifact refs；
- Parquet receipt；
- 每个 decision JSON 的 receipt、row digest、availability 和 source-scope digests；
- release availability。

Loader 不只检查 manifest SHA。它会依次：

1. 重读 candidate → plan → request → receipt 完整控制链；
2. 重读 calendar 和所有 candidate source/evidence artifacts；
3. 校验 Parquet Arrow schema、必填 null、sort 和 PK；
4. 读取每个 decision JSON，并要求其中的完整 contract row 与 Parquet row 完全相等；
5. 要求 decision ID set 与 candidate、plan、request、receipt、manifest 和 Parquet 全部相同；
6. 重算每个 exact source-row scope、ID-set digest 和全 scope digest；
7. 重算 decision chain、availability 和 release availability；
8. 拒绝 release 目录中的缺失或额外文件。
9. 对 canonical production release 重放 ingress provenance、所有 raw evidence bytes 和当前 runtime。

这满足流式 materializer 的“完整 decision row replay”，而不是只验证一个 manifest hash。

## 6. 下游 materializer 接口

`load_registry_release_set` 只接受按固定顺序提供的五个 exact release pins。加载结果提供：

```python
release_set.require_decision_scope(
    registry_name=...,
    release_id=...,
    decision_id=...,
    source_row=exact_s4_source_row,
    cutoff_session=...,
)
```

该调用同时确认：

- derived row 指向正确 registry release；
- decision ID 确实存在于该 release 的 Parquet 和 JSON replay；
- decision 和 release 在 cutoff 已可用；
- materializer 当前读取的完整 S4 source row 与 release 中的 exact scope snapshot 相同。

`require_unique_composite_match` 同时查询三个 Composite correction registries。命中数大于一时直接
报错；没有 priority、majority、最近值或自动纠正。生产 streaming runner 可以捕获这一明确的
collision 结果，保留 membership 并输出 canonical/alias null、identity ineligible；不得选一个
decision 继续。

Release-set loader还验证 `provider_composite_override.asset_transition_id` 必须真实存在于同一组
`asset_transition` release，ticker、successor Composite、series 和 availability 必须完全一致。

## 7. 五类职责互斥

- `identity_adjudication`：只处理真正的 bounce middle episode；当前 19 个 inverse/foreign cases
  作为 cross-market lineage，不再生成第二份 Composite correction。
- `identity_cross_market_adjudication`：只对 exact `massive/stocks/us + ticker + Share Class + foreign
  Composite + source row` 生效；不能污染真实 foreign locale。
- `provider_composite_override`：只处理已批准 genuine transition 后的同市场 US→US stale
  observation；必须绑定独立 `asset_transition`。
- `share_class_adjudication`：只有 canonical Composite 已唯一确定后才修 Share Class；不得改 asset、
  issuer、membership 或 tradability。
- `asset_transition`：只添加 predecessor/successor lineage；不得 override、拼收益、改 membership、
  设 tradability 或触发强平。

所有 registry row 都要求 `identity_quality_liquidation_signal=false` 和
`outcome_or_backtest_evidence_used=false`。

## 8. 固定 case 约束

代码内冻结以下 production validation specs，但不会发明缺失 source IDs：

- SOR genuine transition：`BBG000KMY6N2 → BBG01RK6N4M5`，boundary
  `2024-12-31 / 2025-01-01 / 2025-01-02`；2025-01-02 provider source row仍观察到旧
  Composite，successor target 来自独立外部证据，不能改写 observed lineage；
- SOR provider stale override：`2025-01-02..2026-07-09`，US→US；
- XZO Share Class：`BBG01XL8FJS7 → BBG01227MF17`，`2025-11-04..05`；
- ANABV Share Class：`BBG0026ZDHT8 → BBG021GNPBR6`，仅 `2026-04-06`，不得并入 ANAB；
- 9 个 cross-market groups：AZPN、CR、FLOW、SBGI、SIRI、TA、TBLT、TNXP、WW，共 79 个
  foreign source rows；另外 10 个 inverse US rows保持 direct identity，只作为 lineage，不进入 override
  source scope，也不标为 genuine transition。

固定测试要求 9 组的 ticker、foreign/US Composite、Share Class、market code、日期和 79 行总数
全部一致。

## 9. CLI

Fixture/internal CLI 从仓库环境运行：

```bash
PYTHONPATH=backend .venv/bin/python -m \
  ame_stocks_api.cli.silver_identity_registry_workflow <command>
```

可用命令：

- `show-contract-pins`；
- `store-candidate`；
- `store-plan`；
- `store-request`；
- `show-request-literal`；
- `record-approval`；
- `publish-release`；
- `verify-release`；
- `verify-release-set`。

该 low-level CLI 只用于测试和内部 replay，所有含 `--data-root` 的命令都会拒绝 canonical production
root；它不能用于回填 production candidate 或 backdate release。

Production 只使用固定 ingress CLI：

```bash
PYTHONPATH=backend .venv/bin/python -m \
  ame_stocks_api.cli.silver_identity_registry_production <command>
```

- `record-fixed-prerequisite-authorization`：代码内部提供固定 standing/reaffirmation bytes，只接收
  registry、authorization role、exact target refs 和 actor；
- `import-fixed-evidence-package`：只接收 `exact-group` 或 `cross-market` 枚举值，从当前 exact Git
  commit 导入代码固定的 evidence package 并输出 manifest/import-receipt refs；
- `prepare-fixed-request`：只接收 exact source/auth refs，内部选择并重放固定 evidence import，构造
  decisions、时间、availability 和 IDs；不接收 evidence path/ref；
- `publish-fixed-standing-release`：只接收 exact request ref 和 actor，发布时间来自 runtime clock。

每个 registry 的三类 prerequisite target 必须按下表构造：

| Registry | `schema_contract_approval` | `source_candidate_approval` | `external_evidence_approval` |
|---|---|---|---|
| `asset_transition` | 本 registry contract ID/resource SHA | exact-group candidate ID/SHA + completion ID/SHA | exact-group evidence manifest ID/SHA |
| `provider_composite_override` | 本 registry contract ID/resource SHA | 同一 exact-group candidate/completion pair | 同一 exact-group evidence manifest ID/SHA |
| `share_class_adjudication` | 本 registry contract ID/resource SHA | 同一 exact-group candidate/completion pair | 同一 exact-group evidence manifest ID/SHA |
| `identity_adjudication` | 本 registry contract ID/resource SHA | Gate C candidate ID/SHA + completion ID/SHA | cross-market evidence manifest ID/SHA |
| `identity_cross_market_adjudication` | 本 registry contract ID/resource SHA | 同一 Gate C candidate/completion pair | 同一 cross-market evidence manifest ID/SHA |

因此实际顺序是：先运行一次对应 kind 的 evidence import，使用其输出 manifest ID/SHA 生成
`external_evidence_approval`，再运行 `prepare-fixed-request`。Prepare 会重新读取同一 Git-pinned package；
若 runtime commit/tree、导入 receipt 或任一 raw byte 已变化则 fail closed。

`provider_composite_override` 还必须在 ingress attestation 中绑定已经完整 replay 的 exact
`asset_transition` release pin；它不是第四类 prerequisite role，也不能由 provider row 自报 ID 替代。

## 10. Production 自动推进与停止条件

Production 顺序固定为 `asset_transition → provider_composite_override →
share_class_adjudication → identity_adjudication → identity_cross_market_adjudication`。每个 registry 只在
上表三类 exact prerequisite receipts、完整 source scope、raw evidence replay、clean runtime 和 calendar
全部通过后生成 candidate/request；随后在 standing QA clean 时发布。

缺任一 exact input，或发生 ID 重算失败、provider transition dependency 不一致、Composite registry
scope collision、Critical/factual/resource/scope 问题时，workflow 必须停止并保留可审计状态，不能按
日期、ticker、当前 OpenFIGI snapshot、priority、majority 或最近值补猜。
