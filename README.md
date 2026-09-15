# kbsync

`kbsync` 是 KingbaseES → KingbaseES 的 Go 命令行同步工具，使用随项目附带的官方 `kingbase.com/gokb` 驱动（V009R003C018）。

## 能力

- 单次全量同步：结构同步后，按外键依赖统一清理目标表，再通过 COPY 写入各表。
- 单次增量同步：有主键表按“游标列 + 单列/复合主键”稳定分页并 upsert；无主键表按游标值整组替换。断点文件原子写入，可由 cron/调度平台反复调用。
- 结构同步：创建 schema/表，补齐并调整列类型、默认值和 NULL 约束，创建普通/唯一索引，校验主键。
- 序列同步：创建表列关联序列，同步步长、上下限、缓存、循环属性、当前值和 `OWNED BY` 关系。
- 支持同名表以及 `source schema.table → target schema.table` 映射。
- 捕获 Ctrl+C/SIGTERM，并将取消传递给查询和事务。

## 构建

要求 Go 1.23 或更新版本：

```bash
cp kbsync.example.yaml kbsync.yaml
make test
make build
./bin/kbsync --help
```

驱动源码位于 `third_party/kingbase.com/gokb`，来源为官方 `KingbaseES_V009R003C018B0003_GOLANG` 分发包；项目补充了模块依赖声明及 Linux/macOS/Windows 构建适配。根 `go.mod` 通过 `replace` 使用它，不依赖私有模块仓库。

也可以从 GitHub Releases 下载 Linux、Windows 或 macOS 的 amd64/arm64 归档，并使用同一 Release 中的 `checksums.txt` 校验 SHA-256。推送 `v*` 标签时由 GoReleaser 自动构建发布：

```bash
sha256sum -c checksums.txt --ignore-missing
```

## 配置

参见 [`kbsync.example.yaml`](kbsync.example.yaml)。DSN 也可由环境变量 `KBSYNC_SOURCE_DSN`、`KBSYNC_TARGET_DSN` 或命令行参数覆盖；不要提交包含密码的 `kbsync.yaml`。

增量同步中：

- `cursor` 必须非空且在每次 INSERT/UPDATE 时单调增大，典型值为 `updated_at` 或递增版本号。
- `key_columns` 支持单列或复合键，例如 `[tenant_id, order_id]`；省略时自动使用源表主键。目标端须有对应主键/唯一约束。
- 如果源表没有主键且未配置 `key_columns`，工具会批量读取若干个游标值，在一个目标事务中删除这些游标值的旧数据并 COPY 对应源记录。该方式可安全重放，也不会因同一游标值的记录数超过 `batch_size` 而漏行；每批游标值数量由 `no_key_cursor_batch` 控制。
- 最好为 `(cursor, key_columns...)` 建联合索引。
- 游标为 NULL 的行不会进入增量同步；首次上线应先执行全量同步。
- 无主键模式仍要求数据发生业务变化时游标值随之增大；已经越过断点的旧游标组不会被再次扫描。需要发现任意历史差异时，应使用后续的分块哈希 `reconcile` 模式。

## 使用

```bash
# 仅同步/核对结构与关联序列
./bin/kbsync -c kbsync.yaml schema

# 单次全量同步
./bin/kbsync -c kbsync.yaml full

# 单次增量同步；执行成功后更新 state_file
./bin/kbsync -c kbsync.yaml incremental
```

命令日志输出到 stderr，方便将 stdout 留给脚本使用。

### 性能与可观测性

- 性能改动遵循“基线测试 → 单项修改 → 同环境复测 → 报告结果”的流程；复现命令、硬件和结果见 [`PERFORMANCE.md`](PERFORMANCE.md)。
- 有主键增量使用事务内临时暂存表，通过 COPY 装载后执行一次 `INSERT ... SELECT ... ON CONFLICT`，避免逐行往返和超大参数 SQL；每次读取行数由 `batch_size` 控制。
- 全量同步默认同时处理 2 张互不依赖的表；外键依赖层级仍严格按父表在前、子表在后执行。缺失的二级索引会在 COPY 完成后创建。
- `table_parallelism` 控制同一依赖层内并发同步的表数；存在外键时按拓扑层执行，父表完成后才开始子表。
- `--log-format=json` 输出 JSON 日志；批次日志包含模式、表名、批次号、行数、读取/写入/提交/总耗时、吞吐率和断点。DSN、密码和行内容不会记录。
- 交互式终端默认显示整体百分比、总行数、速度、ETA，以及完成/运行/排队表数；每个运行中表还有独立进度条，完成后自动移除。`--progress=always` 可强制显示，`--progress=never` 可关闭；JSON 日志和重定向输出默认关闭动态进度条。
- 百分比和 ETA 使用同步开始时的精确待处理行数，因此启用进度条会为每张表额外执行一次 `COUNT(*)`；追求最低源库扫描开销时可使用 `--progress=never`。
- 配置 `metrics_file` 后写出 Prometheus textfile 指标，包括每表行数、批次数、错误数、耗时和吞吐率，可由 node_exporter textfile collector 采集。

```bash
./bin/kbsync --log-format=json --log-level=info incremental
```

### 外键依赖

- 配置范围内的外键会同步到目标库，并按源表依赖关系自动规划执行顺序；schema/table 映射会同步应用到被引用表。
- 全量 `truncate` 使用一条多表 `TRUNCATE`，避免单独清理父表时被外键阻止；`delete` 按子表到父表清理，写入按父表到子表进行。
- 同一拓扑层内的无依赖表可以并发；存在依赖的层严格串行推进。
- 引用未配置表的外键默认跳过并记录原因，避免错误指向另一个环境的对象。
- 检测到跨表循环外键时会停止并报告涉及表；需要先将约束设计为可延迟，或拆分同步任务。

## 测试

快速单元测试不依赖数据库：

```bash
make test
```

真实集成测试通过 Testcontainers 同时启动源、目标两个 Kingbase 实例，验证目标建表、COPY 全量同步、索引、结构增量、单主键及复合主键 upsert、无主键游标组替换、断点幂等和序列当前值：

```bash
make test-integration
```

默认镜像为 `lihongjie0209/kingbase:latest`，可按实际授权镜像覆盖：

```bash
KBSYNC_TEST_IMAGE=registry.example.com/kingbase:v8r6 \
KBSYNC_TEST_PORT=54321 \
KBSYNC_TEST_USER=system \
KBSYNC_TEST_PASSWORD=manager \
KBSYNC_TEST_DATABASE=test \
make test-integration
```

测试需要可用的 Docker daemon。集成测试带有 `integration` build tag，不会混入日常 `go test ./...`。

## 快速差异修复方案

对于没有可靠更新时间列、但又不适合反复全量覆盖的大表，可以增加独立的 `reconcile` 模式：按主键范围分块，在源端和目标端计算相同规范下的行指纹；相同块直接跳过，不同块再逐主键归并比较，只回源读取并 upsert 缺失或变化的完整行。目标端独有主键默认仅报告，开启显式删除选项后才删除。

该方式不依赖业务游标，适合定期兜底校验。其主要成本是扫描主键索引和参与计算的列；LOB/超大文本列应使用分批流式哈希，并限制批次内存。它应作为独立命令实现，不应复用 `incremental` 的游标断点文件。

## 安全与一致性边界

- 默认 `full_strategy: truncate`。为处理外键，所有配置目标表会先由一条多表 TRUNCATE 统一清理，然后各表分别在事务中 COPY；某张表的 COPY 失败会回滚该表写入，但此前的统一清理和已经完成的其他表不会回滚。要求全库原子切换时，应使用影子 schema/表并在外部编排切换。
- 结构同步不会删除目标端多出的列、索引、表或序列。列类型、默认值、NULL 约束会向源端定义对齐；主键或同名索引定义冲突时停止并要求人工处理。
- 增量模式同步新增和更新，不传播物理删除。删除同步需要源端变更日志/触发器或 Kingbase 逻辑复制语义，不能只依赖更新时间列可靠推断。
- 当前自动同步的是配置表及其列、主键、普通/唯一索引、配置范围内外键、关联序列。视图、触发器、函数、权限、表空间和分区拓扑不自动修改，以避免跨环境误操作。
- 断点只会在目标事务提交后推进，并通过临时文件原子替换。默认每 10 个批次以及每张表退出时落盘；若进程被强制终止，下次会重放尚未持久化的已提交批次，upsert/游标组替换保证这种重放幂等。可将 `checkpoint_every_batches` 调为 1，换取每批持久化。
