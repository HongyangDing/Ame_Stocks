# S7 四张派生表 v4 合同增量

## 状态与不可变历史

2026-07-17 已批准的四份 v3 candidate 继续保留在原路径，供 Gate A、历史 schema
approval 和 inventory control chain 逐字节重放。它们不得被新设计原地覆盖。

本文件记录引入 `provider_composite_override`、`share_class_adjudication` 和
`asset_transition` 后的四份 v4 candidate。运行时 schema resource 使用 v4 合同；历史
candidate 仅作为旧审计链证据，不再作为 Full 的当前合同。

## v4 candidate

| Table | Columns / Critical+High QA | Contract ID | Arrow schema digest | Candidate SHA-256 |
| --- | ---: | --- | --- | --- |
| `asset_master` | 56 / 42 | `4d85c7cc73ee4b61ca548aec4b64aa6cb05e779d3c3beb1a8f601023d96f8df1` | `d20c31bba79e5c2507f8f74ac61d3f0a9caa89a93f44e4f1b0d635ceb0384493` | `cc14c11dca1f449a3c8fcdbe4f0e419a26dc15312b75116db767706699f0b849` |
| `ticker_alias` | 67 / 54 | `796423964d875daa3aa25fc2d14b06dcebd436bb91d42629866f0995dbc2931e` | `2f7fd74487df3d4255e46c90ec0189f109deb1d3618642f1e3716948cbae06fc` | `89c1f05a545ab18100dafc7b3b27210aff38ecceeddb0e1d048419a30b8f83de` |
| `issuer_master` | 41 / 39 | `0e46c0e939989205b4dcd48f11e3443ec5c3e72b366dcd5684c417bb134d6b70` | `a53e9c66db027dc5a6e2883fe4c0596897776a831ea1ade798975feecbd18cd1` | `17108231fa5ab46fd98095b52a06fda88a1ababf9e9e3b3dae8ce66bdd7f8c50` |
| `universe_daily` | 72 / 63 | `bf1ab110844f1d7a572db2d4e14e725b83ca0a99b566c61b1af89aa24d514fbf` | `905694a195817cdabe2117568460eb0cbebd3aedbb8c17efb4200396a2dbfca7` | `c83327b1e38defa8f56bc1ea87f011bb360692da327323cae41cc8eeee2d54be` |

Candidate paths:

- `docs/silver/contracts/identity/asset_master.schema-v1.registry-v4.candidate.json`
- `docs/silver/contracts/identity/ticker_alias.schema-v1.registry-v4.candidate.json`
- `docs/silver/contracts/identity/issuer_master.schema-v1.registry-v4.candidate.json`
- `docs/silver/contracts/reference/universe_daily.schema-v1.registry-v4.candidate.json`

## 关键语义

- 三类 Composite correction registry 同时命中时，不选优先级：membership 保留，canonical
  identity 与 alias 均为空，`backtest_identity_eligible=false`。
- `share_class_adjudication` 只在唯一 canonical Composite 确定后应用，不能创建或改变
  `asset_id`，也不能修改 CIK。
- `asset_transition` 只记录 predecessor/successor 关系；不得执行 override、合并资产或拼接收益。
- identity quality 不能推导 inactive、delisted 或强制平仓。
- 四张表必须绑定同一 S4 release set、Gate B、Gate C 和五份 registry release，并作为一个
  visibility-atomic release set 发布。

生产合同 approval 必须由 runtime 重新绑定当前 clean Git commit/tree、上述 exact IDs/hashes、
XNYS calendar 和用户的持续 S7 授权；旧 v3 approval 不可冒充 v4 approval。

Gate B 的 production request、Gate C、registry releases、四表 Full 与最终 Publish 必须在同一冻结
Git commit 上完成。Gate B 官方 verifier 会把当前 checkout 与 capture 的 runtime binding 逐字段比较；
因此这条链运行期间不得部署新的 repository commit。
