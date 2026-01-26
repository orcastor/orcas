# VFS 必验证 Test Case 清单（回归/发布门禁）

> 目标：列出 **vfs 必须覆盖并持续回归** 的高风险路径与典型用户行为（文件读写、Log-based、Sparse、Journal/WAL/Retention、原子替换等）。
>
> 说明：
> - **[已有测试]**：仓库里已经有对应 `*_test.go` 覆盖（给出函数名）。
> - **[建议新增]**：目前未发现现成测试或覆盖不充分，建议补齐为自动化测试。
> - 本清单偏“门禁”，优先保证：**数据正确性（内容/大小/版本）**、**崩溃恢复**、**并发一致性**、**边界条件**。

---

## 1. 文件基础读写（RandomAccessor / Node API）

- **顺序写 + 顺序读（小文件）**：[已有测试]（建议在 `random_accessor_test.go` 中补一个最小用例：Write/Flush/Read 全量校验）
- **随机写（覆盖/重叠/重复写）**：
  - **同一 offset 多次写（last-write-wins）**：[已有测试] `TestLogBasedReadWritePattern`
  - **重叠区间写（partial overlap）**：[建议新增]（断言：覆盖区间正确、未覆盖区间保持不变）
  - **写入跨 chunk 边界**（例如 10MB chunk）：[已有测试]（`random_accessor_test.go` 已有大量 chunk 场景；建议加一个“刚好跨边界”的最小 case）
- **读边界**：
  - **读到 EOF 的行为**（短读/EOF 返回）：[建议新增]（断言：不 panic；返回长度符合预期；EOF 处理一致）
  - **读 size=0**：[建议新增]
  - **读 offset<0 / write offset<0**：[建议新增]（断言：返回明确错误）
- **文件大小一致性**：
  - **扩容写（写到文件尾之后）**：[已有测试] `TestLogBasedReadWritePattern`
  - **缩容（truncate 变小）后读写不越界/不读取旧 chunk**：[已有测试]（`random_accessor_test.go` 有“File shrinking”回归）
  - **大文件 flush 后 size 计算正确**：[已有测试]（`journal_test.go` 中有 “LARGE FILE SIZE CORRUPTION BUG” 回归段）
- **Node API 覆盖（Open/Read/Write/Fsync/Release）**：
  - **与 RandomAccessor 等价的读写序列**：[已有测试] `TestLogBasedReadWritePatternWithNode`
  - **多次 Open/Release 资源回收**：[建议新增]（配合 `memory_leak_test.go` 的思路）

---

## 2. `.tmp` 原子替换（Atomic Replace / Office/WPS 模式）

- **Office/WPS 典型保存流**（`.tmp` 写入 → ForceFlush(模拟 rename 去掉 `.tmp`) → 新文件读回校验）：
  - [已有测试] `TestTmpFileOfficeSaveScenario`
- **`.tmp` 常规 Flush 行为**：
  - **force=false 不应 flush `.tmp`**（只允许 rename/force 时提交）：[已有测试]（`random_accessor.go` 逻辑对应；建议在测试里明确断言）
- **`.tmp` rename 后 RandomAccessor 语义**：
  - **旧 RA 继续写应报错 “renamed from .tmp, must recreate”**：[已有测试]（`random_accessor_test.go` 有相关断言）
  - **rename 后新 RA 写/读正常**：[已有测试] `TestTmpFileOfficeSaveScenario`
- **`.tmp` 与 Sparse 交互的历史 bug**：
  - **`.tmp` 阶段设置了 sparseSize，rename 后必须清理 sparseSize，避免错误走 SPARSE writer**：[已有测试] `TestTmpFileWriteAfterRenameWithSparseSizeBug`

---

## 3. Journal（日志式写入）正确性与边界

- **Flush 返回 versionID 一致性**：
  - **dirty → 非 0；non-dirty → 0**：[已有测试]（`journal_test.go` 中 “versionID consistency” 测试段）
- **小文件/大文件策略分界（<10MB vs >=10MB）**：[建议新增]
  - 断言：策略切换时（阈值附近）读写正确、size 正确、不会产生异常版本/错误 dataID。
- **Entry 合并/排序/覆盖规则**：[建议新增]
  - 断言：多个 entry 合并后读回等价于按时间顺序应用写入；重叠区 last-write-wins。
- **Journal 内存限制与自动 flush**：[建议新增]
  - 断言：达到 `MaxMemoryPerJournal/MaxTotalMemory` 触发路径不会丢数据、不产生重复版本、不死锁。

---

## 4. WAL（崩溃恢复）与重启恢复

- **WAL FULL 模式：写入后崩溃恢复**：[建议新增]
  - 测试形态：写入若干 journal entry → 只落 WAL/快照（不完成正常 flush）→ 模拟重启（新建 `JournalManager` 调 `RecoverFromWAL`）→ 校验数据可读且大小正确。
- **WAL 快照/Checkpoint 回放正确性**：[建议新增]
  - 断言：snapshot 存在时能加速恢复且结果与完整回放一致。
- **WAL 文件清理**：
  - `JournalManager.Remove()` 删除 `.jwal/.snap` 等文件：[已有实现]（建议补测试，避免磁盘泄漏）

---

## 5. Version Retention（版本保留/合并/清理）

- **时间窗口合并（TimeWindowSeconds）**：[建议新增]
  - 断言：窗口内只保留最后一个完整版本；窗口外保留策略生效。
- **最大版本数（MaxVersions）/最小完整版本（MinFullVersions）**：[建议新增]
  - 断言：超限会删最老版本；但不会删到低于 MinFullVersions。
- **每个 base 的 journal 数上限（MaxJournalsPerBase）触发合并**：[建议新增]
  - 断言：合并后读回等价；版本链正确；不会丢历史版本必要元信息。

---

## 6. Log-based 回放（用真实日志还原用户行为）

- **按日志序列执行的 Read/Write/Flush（含加密）**：[已有测试] `TestLogBasedReadWritePattern`
- **同一日志序列走 Node API（Open/Read/Write/Fsync/Release）**：[已有测试] `TestLogBasedReadWritePatternWithNode`
- **建议新增：log2/log3 等回归集**：[建议新增]
  - 目标：把线上/用户的“真实序列”持续沉淀为自动回归（尤其是 `.tmp`、sparse、跨 chunk、rename、并发）。

---

## 7. Sparse（稀疏文件/预分配/洞）相关

> 关键点：**稀疏文件的“逻辑大小”与“实际写入数据大小（OrigSize）”必须区分**；未写区域的读行为要稳定（通常全 0 或短读，按实现约定）。

- **标记稀疏文件 + 预分配大小一致性**：[已有测试]（`random_accessor_test.go` 中 `test sparse file size consistency` 段）
- **稀疏文件随机写冗余（Journal 只存实际数据）**：[已有测试] `TestRandomWriteRedundancy`
- **本地顺序写优化（LocalSequentialChunkCount 范围内走 ChunkedFileWriter）**：[已有测试] `TestSparseFileLocalSequentialWrite`
- **超出本地顺序范围写（应走 Journal）**：[已有测试] `TestSparseFileBeyondLocalRangeUsesJournal`
- **ChunkedFileWriter cleared 后还能继续写**：[已有测试] `TestSparseFileWriteAfterChunkedWriterCleared`
- **大文件稀疏上传模拟（200MB+，禁止误用 TMP writer）**：[已有测试] `TestSparseFileLargeUploadSimulation`
- **稀疏文件“被完全写满”后应清除 sparse 标记（或下次访问不再当 sparse）**：[已有测试] `TestSparseFileFullyWrittenVerification`
- **洞区读取语义稳定**：[部分已有]（`random_accessor_test.go` 有“zeros acceptable”日志）
  - [建议新增]：明确断言“洞区读返回全 0（或短读）”的约定；避免因实现调整导致上层应用（如 BT/Office）行为异常。

---

## 8. Atomic Replace 与 Journal/Sparse 的交叉场景

- **atomic replace pending deletion 存在时 flushJournal 能检测并合并版本**：[已有测试] `TestJournalFlushWithAtomicReplace`
- **建议新增：atomic replace 与 retention 同时触发**：[建议新增]
  - 断言：版本合并/删除不会互相干扰；不会误删新文件版本链。

---

## 9. 并发/一致性/资源回收（稳定性门禁）

- **读写并发 + 数据一致性校验（多 offset、多 chunk）**：[已有测试]（`random_accessor_test.go` 中并发读写与校验段）
- **多个 RandomAccessor/Handle 并发访问同一 fileID**：[建议新增]
  - 断言：不会产生 size 回退、dataID 错乱、死锁；读到的内容满足“可见性”约定（至少 flush 后必可见）。
- **Close/Flush 幂等性**：[建议新增]
  - 重复 Close/Flush 不 panic；重复 flush 不产生额外版本（dirty 判定正确）。
- **内存泄漏/对象池回收**：[已有测试] `memory_leak_test.go`（建议纳入 CI/门禁）

---

## 10. 平台差异（Windows / Dokany 等）

- **Windows 路径/语义差异回归**：[已有测试] `fs_win_test.go`
- **建议新增：Windows 下 RandomAccessor 行为一致性冒烟**：[建议新增]
  - 覆盖：读写/rename/truncate/flush 的最小闭环，确保跨平台不会出现明显语义偏差。

---

## 11. 建议的“门禁最小集合”（用于 CI 快速回归）

如果要做一个“必须全绿”的快速集（不跑极慢的性能/超大文件），建议优先包含：

- `TestLogBasedReadWritePattern`
- `TestLogBasedReadWritePatternWithNode`
- `TestTmpFileOfficeSaveScenario`
- `TestTmpFileWriteAfterRenameWithSparseSizeBug`
- `TestJournalFlushWithAtomicReplace`
- `TestSparseFileLocalSequentialWrite`
- `TestSparseFileBeyondLocalRangeUsesJournal`
- `TestSparseFileLargeUploadSimulation`
- `TestSparseFileFullyWrittenVerification`

---

## 12. 维护建议

- **每次线上出现新日志/新模式**：优先把“操作序列”抽取成 Log-based 测试加入本清单（`## 6`）。
- **清单与测试函数名同步**：新增/重命名测试时，同步更新本文件，保证“清单即门禁”的可追踪性。

