# Silver S0 合同、审批与发布边界

## 1. S0 的结论和硬边界

S0 只建立正式 Silver 的控制面：schema、build、QA、quarantine、审批、release 和只读发布入口。
它让后续每一类数据都必须经过同一套可审计流程，但**不处理任何真实数据**。

本阶段明确不做以下事情：

- 不读取、改写或删除远程 Bronze 文件；
- 不调用 Massive、SEC 或其他网络 API；
- 不编写或运行 S1 `exchanges` 以及任何其他 family 的业务转换；
- 不生成正式或 preview Silver 数据集，不启动 Celery/Docker/远程任务；
- 不把现有 `silver_unadjusted/` pilot 标记成正式 Silver；
- 不修改 `/mnt/HC_Volume_106309665/american_stocks` 中的运行数据；
- 不提供“latest build”旁路，也不让网页、Gold 或回测直接读取未发布 build。

S0 的测试只能使用临时目录里的小型、合成 fixture，以证明合同、不可变写入、幂等、lineage
和状态门本身有效。固定案例注册表在 S0 中也只是元数据；真实案例输入和预期输出要等对应
数据集逐项获批后再实现。

## 2. 数据面与控制面分离

未来数据盘仍以 `/mnt/HC_Volume_106309665/american_stocks` 为根。S0 定义的逻辑布局如下：

```text
<data_root>/
├── staging/silver/
│   └── <table>/build_id=<build_id>/              # preview，绝不作为正式输入
├── silver/
│   └── schema=vN/<domain>/<table>/build_id=<build_id>/
└── manifests/silver/
    ├── contracts/<domain>/<table>/schema-vN/contract-<contract_id>.json
    ├── source-inventories/<source_dataset>/inventory-<inventory_id>.json
    ├── workflows/<workflow_id>/events/<sequence>-<event_sha256>.json
    ├── approvals/<approval_id>.json
    ├── builds/<table>/build_id=<build_id>/manifest.json
    └── releases/release_id=<release_id>.json
```

preview 和 full build 使用不同的物理前缀。相同 logical intent 产生相同 `build_id`；同一个路径
只能发布一次，同内容重试是幂等成功，不同内容冲突必须失败。正式消费者只能由明确的
`release_id` 进入，release 再固定到一个 full build 和它的精确输出列表。

## 3. 核心合同

### 3.1 `TableContract`

每张表在写数据前先冻结以下内容：

- `domain`、`table` 和正整数 `schema_version`；
- 表说明和一行代表什么的 `grain`；
- 有顺序的列名、Arrow 类型、nullable 和字段说明；
- 非空主键、分区列、排序列；
- 输入数据集 family；
- 发布前必须出现的结构化 `QARule`：check ID、固定 severity、metric、operator、limit、
  violation 对应 `warning/failed` 以及说明。

合同只允许明确支持的 Arrow 类型，包括 string、boolean、int64、float64、date32、UTC 纳秒
时间戳和稳定 JSON string。主键列不能 nullable，合同字段不能重复，分区/排序/主键不能引用
未知列。`contract_id` 是规范化合同内容的 SHA-256；相同
`(domain, table, schema_version)` 不能静默替换为不同合同。

QA status 不能由转换代码随意自报。registry 会用 rule 对 manifest 的 numerator/rate 重新求值，
并拒绝 severity、threshold 或 status 与合同政策不一致的结果。这里重新求值的是已报告 metric；
除 schema、checksum、row count、文件集合等控制面检查外，主键、PIT 等业务 numerator 仍由
Git 固定的转换/QA 代码计算，必须在对应 S1–S34 的 fixture、preview 和独立 Data Health 审计中
证明，S0 不声称能从任意业务表自动推导所有错误分子。

### 3.2 `SourceInventory` 与 `ArtifactRef`

业务 build 不能把任意文件自行标成 source。每批输入先登记一个不可变 `SourceInventory`：

- 明确 `source_dataset` 与 `source_layer`；layer 只能是 `bronze`、`published_silver` 或测试专用
  `synthetic_fixture`；
- 保存构建 inventory 的完整 Git commit；
- 保存每个上游 manifest 的路径和 SHA；非 release 上游必须是
  `complete/passed/passed_with_warnings` 终态；
- 把 gzip JSON page、gzip CSV Flat File、JSON 或 Parquet 规范成统一的 path、SHA、bytes、
  row count、media type，Parquet 另含 table/schema digest；
- upstream manifest 必须实际绑定文件 checksum 和 row count；inventory 注册时再次核对文件；
- `staging/`、`silver_unadjusted/`、Gold、tmp、backup 或其他任意目录不能伪装成正式输入；
  `silver/` 输入必须能追溯到已发布 release 的完整 workflow 信任链。

每个输入和输出都由相对 `data_root` 的规范路径、SHA-256、字节数、媒体类型和角色标识。
Parquet 还必须有行数、目标表和 schema digest。允许的角色是 `source`、`data`、`qa`、
`quarantine` 和 `sample`。source ref 必须绑定上述 inventory 的路径和 SHA，且 dataset/layer/整条
artifact 记录完全一致。正式 data、QA、quarantine 都只能是强类型 Parquet；raw source 另明确
支持 gzip JSON 与 gzip CSV，不能错标成普通 JSON。manifest 中的引用不是提示；注册或读取时
要重新核对实际文件。

### 3.3 `BuildIntent` 与 `BuildManifest`

`BuildIntent` 记录 workflow、合同、preview/full 类型、重试关系、transform version、完整 Git
commit、交易日历版本、输入文件和安全的逻辑参数。`source_digest` 按输入路径排序后由路径和
完整 `ArtifactRef`（包括 dataset/layer、lineage、row count、media/schema）确定；`build_id`
只由逻辑输入确定，时间戳不参与，因此同输入重跑可以判断是否真正一致。

完成后的 `BuildManifest` 在 intent 之外记录：

- 所有 data、QA、quarantine 和有界 sample 输出；
- 输入、接受、精确重复 excess、quarantine、未映射、版本保留和分表输出的 row funnel；
- 每项 QA 的 severity、status、分子、分母、rate、阈值和有界样例路径；
- quarantine issue 数与唯一 source row 数；
- UTC `started_at`、`completed_at`；
- preview 专属的固定案例 IDs、样例行数、资源实测和 full-run 外推。

preview 还必须保存可阅读的 input/output sample artifact，以及 `case_id → QA result IDs`；只登记
case 名称而没有样例和断言证据会失败。更重要的是 preview manifest 内嵌**完整 full-run source
inventory preimage**，不是只有一个未来可随意解释的 digest。full build 必须与已批准 preview 的
Git commit、transform/calendar version、逻辑参数和完整 source inventory 一致，并显式写入
`approved_preview_build_id`。

row funnel 必须满足：

```text
input_rows = accepted_source_rows
           + exact_duplicate_excess
           + quarantined_source_rows
```

data artifact 的汇总行数还必须与 `output_rows_by_table` 精确相等。一次源行可以触发多个
quarantine issue，因此 issue 数和唯一坏源行数分开记录；唯一坏源行数必须与 row funnel
中的 quarantined source rows 对齐。

### 3.4 QA 与 quarantine

QA severity 为 `critical/high/medium/low`，status 为 `passed/warning/failed`。合同列出的 required
check 不能缺席，未知 check 也不能临时加入。每个 build 必须产生冻结 schema 的 QA Parquet；
registry 会逐行核对它与 manifest 内嵌 QA、build ID、rule evaluation 完全一致。checksum、
manifest、schema、主键、未来数据、双重复权等阻断性问题不得用删除样例或修改分母的方式变绿；
Critical/High failure 阻止发布。允许 warning 或 Medium/Low failure 发布时，approval 必须逐项
列出确切 **QA result digest**，不能只写重复的 check ID 或空泛全局 waiver。

Quarantine 是 append-only 的正式证据，后续数据集至少要表达：

```text
source_record_id, table_name, issue_code, severity,
detected_build_id, source_pointer, field_name,
observed_value, expected_rule, review_status
```

Quarantine Parquet 使用冻结 schema；registry 会重新解析每行，核对 build/table、issue digest、
severity、唯一 source row 和 row funnel。build 产生的 issue 一律从 `pending` 开始。Critical issue
不可接受或豁免；High issue 必须由 full-run/publish approval 精确列出 issue digest 才能越过门；
Medium/Low 保留为可见证据。S0 只冻结机制并用合成 fixture 验证，不产生真实 quarantine 行。

### 3.5 `ApprovalReceipt`

审批阶段只有 `schema`、`full_run` 和 `publish`。receipt 保存 decision、workflow、被审批对象 ID、
对象 manifest 的精确 SHA-256、审批时所见的上一 workflow event SHA、approver 标签、UTC 时间、
说明、逐项 QA waiver 和逐项 High quarantine acceptance。receipt 自身由内容产生
`approval_id`，发布后不可覆盖。

三次批准绑定的对象不同：

- schema approval 绑定精确 contract 文件；
- full-run approval 绑定精确 preview build manifest；
- publish approval 绑定精确 full build manifest，并在发布时再次核对 QA 和输出文件。

因此批准旧 preview 不能授权一个参数或代码已经变化的新 full run，批准旧 full build 也不能
发布后来被替换的文件。

### 3.6 `ReleaseManifest`

release 绑定 workflow、表合同、full `build_id`、build manifest hash、publish approval 及 hash、
UTC 发布时间和完整输出引用。`release_id` 是上述逻辑内容的 digest。网页、Gold 和回测必须
显式给出 `release_id`；reader 重新验证 release、approval、build 和文件 checksum 链后才返回
输出。没有按 mtime、目录排序或“最近一次成功”自动选择的接口。

## 4. 状态机与强制审批门

唯一合法的成功路径是：

```text
planned
  → schema_review
  → code_ready
  → preview_ready
  → awaiting_review
  → approved_full_run
  → full_ready
  → awaiting_publish
  → published
```

各状态含义如下：

| 状态 | 已存在的证据 | 仍禁止的动作 |
| --- | --- | --- |
| `planned` | workflow 已建立 | 写 preview、跑 full、发布 |
| `schema_review` | 待审合同已登记 | 写数据 |
| `code_ready` | schema approval 已绑定合同 | 直接跑 full |
| `preview_ready` | preview build 已登记并核验 | 跑 full、发布 |
| `awaiting_review` | preview 已提交人工 review | 未审批直接跑 full |
| `approved_full_run` | full-run approval 已绑定 preview | 发布、处理其他未批 family |
| `full_ready` | full build 已登记并核验 | 供公开消费者读取 |
| `awaiting_publish` | full build 已提交最终 review | 未审批直接发布 |
| `published` | publish approval 和 release 已冻结 | 改写该 release 或其输出 |

`failed` 和 `rejected` 保留为终态证据。恢复工作必须创建显式重试/新 workflow，不能覆盖失败
文件。每次变更要携带调用方所见的 `expected_event_sha256`；不匹配说明状态已被其他操作推进，
旧调用必须失败，不能自动合并。

workflow event 采用单调 sequence 和 `previous_event_sha256` 哈希链。读取时核对序号连续、文件
名 hash 与内容一致、上一事件 hash 一致、UTC 时间单调和每一步状态合法。每种状态还有严格的
evidence key schema；读取会重新打开 schema approval、full-run approval、build、publish
approval 和 release，逐个核对对象 ID、SHA、actor、note、时间及上一 event SHA。删掉或损坏
中间 receipt 后，即使最终 release 文件仍在，公开 reader 也会 fail closed。

## 5. 不可变写入和验证顺序

S0 的持久化规则是 fail closed：

1. 所有 manifest 先做严格字段、枚举、UTC、native integer、有限浮点和 JSON 深度检查；
2. JSON 使用固定键序和紧凑编码，禁止 NaN/Infinity，以便 digest 跨重跑稳定；
3. 路径必须是位于 `data_root` 内的规范相对路径，拒绝绝对路径、`..` 和 symlink escape；
4. 写临时文件并完成 flush/fsync，设为只读，再以不可覆盖 hard-link publication 发布；
5. 要求正式输出为 `0444`、regular file、单 link，使用 `O_NOFOLLOW` 的同一 fd 完成
   stat/hash/Parquet metadata 检查，并确认读取前后 inode/size/mtime/ctime 未变化；
6. 重新计算 bytes、SHA、Parquet 行数和精确 Arrow schema；
7. fsync 父目录后才登记 manifest/event；
8. 目标已存在时只接受字节完全一致的幂等结果，不执行 replace。

build 注册还要拒绝其输出前缀以外的文件、manifest 未声明的额外文件和缺失的声明文件。
reader 每次消费 release 都重做信任链和输出验证，而不是因为文件曾经通过检查就永久信任。
公开读取为避免对十年 Bronze 反复做几十 GB hash，只重验全套控制面对象和 release 中的正式
DATA bytes；完整 source/inventory/QA/quarantine 深审在 build 登记、审批、publish 以及周期性
Data Health audit 执行。

同一 intent 的 build ID 不含运行时间。若 manifest 已不可变落盘、event 尚未追加时进程崩溃，
恢复必须加载并复用该精确 orphan manifest；不能用新时间覆盖。显式 `attempt > 1` 还必须绑定
存在的 `retry_of_build_id`，并保持同一 workflow/kind/contract/source/code/calendar/参数，attempt
严格加一，否则不构成可审计 retry。

## 6. 固定案例注册表

[`fixed_cases.py`](../backend/ame_stocks_api/silver/fixed_cases.py) 用 frozen dataclass、tuple 和只读
mapping 注册 14 个必须覆盖的场景：

| Case ID | 场景 | 后续主要 family |
| --- | --- | --- |
| `normal_session` | 正常交易日 | 行情/日历 |
| `half_day` | 美股半日市 | 行情/日历 |
| `forward_split_2_for_1` | 2:1 拆股 | 公司行动/复权 |
| `reverse_split` | 反向拆股 | 公司行动/复权 |
| `regular_dividend` | 普通现金分红 | 公司行动/收益 |
| `special_dividend` | 特殊分红 | 公司行动/收益 |
| `halt_or_missing_minutes` | 停牌或缺分钟 | 分钟线/coverage |
| `ticker_change` | ticker 变更 | 身份 |
| `ticker_reuse` | ticker 被不同证券复用 | 身份 |
| `delisting` | 退市证券 | 身份/universe |
| `case_sensitive_tickers` | 大小写相近 ticker | 身份 |
| `provider_timestamp_2019_08_12` | 2019-08-12 的 29 条 provider 异常 timestamp | Flat Day |
| `date_only_filing` | 只有日期的 filing | SEC/PIT |
| `form_13f_header_only` | 没有持仓明细的 13-F header | Form 13-F |

注册表中的 invariant 是 review 要求，不是转换实现。某个 preview 只能声明自己实际构造并断言
过的 case IDs；不能因为 case 已注册就声称该数据集已经通过案例测试。

## 7. 威胁模型

S0 主要防范同一可信运维环境中的意外错误和低复杂度篡改：

- 意外覆盖、部分写入、重复运行产生不一致内容；
- 路径穿越或 symlink 把输出写到数据根之外；
- manifest 与实际 bytes、checksum、行数或 schema 不一致；
- 并发/过期调用导致状态跳跃或旧审批被复用；
- Critical/High QA 失败、warning 未逐项批准时仍被发布；
- Critical quarantine 或未逐项接受的 High quarantine 被发布；
- 未发布 build 被网页、Gold 或回测当成正式数据；
- API key、authorization、cookie、password、token、signed URL 等敏感内容进入参数、审批说明
  或 manifest。

S0 **不**声称能抵御拥有服务器 root 权限、可同时修改代码和全部证据的攻击者，也不把 approver
字符串当作加密身份。当前审批模型面向可信的本地/SSH operator，是审计绑定而不是数字签名。
若以后通过网页执行审批，必须另行增加管理员认证、安全会话、CSRF 防护、授权检查以及必要时
外部签名/不可变日志。S0 也不证明 provider 数据语义正确；每个 S1–S34 转换仍需自己的字段
mapping、固定案例、PIT 规则和 QA 审批。

## 8. S0 验收与下一硬停点

S0 的通过标准仅包括：

- 合同和 manifest 能严格 round-trip，未知字段或不安全值被拒绝；
- 合成 fixture 能证明 source inventory/layer、schema、checksum、row funnel、QA/sample/
  quarantine 对账、原子不可变写、深层不可变 ID 和确定性 build ID；
- 非法状态跳跃、过期 event hash、未批准 full run、阻断性 QA 和未批准 publish 都失败；
- 只有 `published` release 能由公开 reader 解析，且任一信任链或输出被改动都会失败；
- 14 个固定案例元数据完整、唯一、不可变；
- 全部测试只使用临时目录，没有触碰 Bronze 或远程数据盘。

完成这些条件只表示控制框架可用，不表示任何 Silver family 已处理。下一步仍需用户单独批准
**S1 `exchanges` schema review**；未经该批准，不读取其 Bronze 输入，也不编写或运行该转换。
