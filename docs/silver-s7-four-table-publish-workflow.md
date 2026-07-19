# S7 四表 Publish：独立审批与原子可见 release set

## 1. 边界

S7 streaming Full 只能停在 `awaiting_review`。Publish 是独立控制链，不复用 Full 的
“without publish” 权限，也不修改、复制、重命名或 `chmod` Full candidate。生产入口只接受固定
`/mnt/HC_Volume_106309665/american_stocks`，fixture 入口反向禁止该路径。

发布单元固定且有序：

1. `asset_master`；
2. `ticker_alias`；
3. `issuer_master`；
4. `universe_daily`。

不存在 `latest`、目录扫描发现、调用方时钟、availability 覆盖、adapter、source rows、receipt
JSON 或输出路径参数。

## 2. 控制顺序

```text
Full completion + candidate + QA + source binding + contracts
  -> PublishPlan
  -> fixed-slot standing approval
  -> durable group intent
  -> hidden immutable member release x 4
  -> final immutable release-set marker
  -> exact-ID reader
```

`PublishPlan` 冻结 Full plan/approval/completion、candidate manifest、QA、四份 v4 contract、source
binding、runtime commit/tree/file set、四表全部 DATA receipts 与 row counts。计划创建、审批和真正
发布时均按精确 ID 重放；发布执行在 durable intent 之后完整重放 Full candidate/QA 和官方
Gate B/Gate C/五份 registry/source-binding trust chain。

intent 只采样一次 runtime clock，并用冻结 XNYS calendar 计算共同 availability：

```text
max(
  first bound XNYS open strictly after runtime publish time,
  standing approval available session,
  source binding cutoff session
)
```

intent 同时冻结四份未来 member manifest 的 ID/path/bytes/SHA。重试必须采用原 intent 的时间与
availability，不能重新采样。四份 member 只引用 candidate 的精确 receipt，不是公共读取入口。
只有最终 marker 存在且完整验证四个 member 后，exact-ID reader 才返回 release set。

## 3. 生产 CLI

先保存 Full 命令返回的四个精确 ID，然后依次运行：

```bash
ame-silver-identity-publish prepare-plan \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --full-plan-id <FULL_PLAN_ID> \
  --full-approval-id <FULL_APPROVAL_ID> \
  --expected-completion-id <FULL_COMPLETION_ID> \
  --expected-candidate-id <FULL_CANDIDATE_ID> \
  --prepared-by <ACTOR>

ame-silver-identity-publish approve-standing \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --publish-plan-id <PUBLISH_PLAN_ID> \
  --approved-by <ACTOR>

ame-silver-identity-publish publish-release-set \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --publish-plan-id <PUBLISH_PLAN_ID> \
  --approval-id <PUBLISH_APPROVAL_ID>

ame-silver-identity-publish verify-release-set \
  --data-root /mnt/HC_Volume_106309665/american_stocks \
  --release-set-id <RELEASE_SET_ID>
```

每一步的 JSON 输出都返回下一步所需的精确 ID/receipt。不要从目录名推断 ID。

## 4. 耐久性与恢复

- 每个计划使用 inode 校验的非阻塞 `flock`；并发 writer 在任何 intent/member/marker 写入前失败。
- 所有 Publish 控制文件通过同文件系统临时文件、file `fsync`、`0444`、no-clobber hard link 和
  parent-directory `fsync` 发布。
- crash 可留下的前缀只有：intent、intent + 前 N 个 member、或完整 marker。
- marker 之前的任何前缀都不可由公共 reader 读取；精确重试只补齐缺少的固定 bytes。
- marker 写成后的 crash 重试是 inode/mtime 保持不变的幂等返回。
- foreign bytes、symlink、可写控制文件、nlink 异常、receipt/QA/source/contract tamper 均 fail closed；
  不覆盖、不清理、不选择其他 authority。

## 5. 运行前硬门

P0（任一存在即不得 Publish）：

- local/GitHub/remote 未冻结在同一 commit，或 runtime file set 有缺口；
- Full completion/candidate 不是同一 exact chain，或不是 `awaiting_review`/`complete=true`；
- Full candidate 或 Gate C `critical_failure_count != 0`；
- candidate、QA、contract approval、source binding、Gate B/Gate C、registry release receipt 变化；
- canonical production root、calendar availability、lock、磁盘/资源边界失败；
- 已存在 intent/member/marker 与预计算 bytes 不同。

P1（发布前应显式复核）：

- Full 的原始 Composite registry collision High numerator 及 review 结论；
- 四表 row counts、unresolved/eligible 比例和 candidate 大小是否符合已批准 profile；
- runtime commit/tree/file-set digest 与将执行 Gate B/Full/Publish 的冻结提交是否完全一致。

P1 不得豁免 P0，也不得把 collision 行提升为 eligible、alias 或 canonical identity。
