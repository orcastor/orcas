package vfs

import (
	"fmt"
	"sync"
	"time"

	"github.com/orcastor/orcas/core"
)

// SaveOperation 保存操作类型
type SaveOperation int

const (
	OpCreate SaveOperation = iota
	OpWrite
	OpDelete
	OpRename
	OpClose
)

func (op SaveOperation) String() string {
	switch op {
	case OpCreate:
		return "CREATE"
	case OpWrite:
		return "WRITE"
	case OpDelete:
		return "DELETE"
	case OpRename:
		return "RENAME"
	case OpClose:
		return "CLOSE"
	default:
		return "UNKNOWN"
	}
}

// FileOperation 文件操作记录
type FileOperation struct {
	Op        SaveOperation
	FileID    int64
	FileName  string
	ParentID  int64
	DataID    int64
	Size      int64
	Timestamp int64
	// For rename operations
	NewName   string
	NewParent int64
}

// SavePattern 保存模式
type SavePattern struct {
	Name        string
	Description string
	Matcher     func([]*FileOperation) *SavePatternMatch
}

// SavePatternMatch 模式匹配结果
type SavePatternMatch struct {
	Pattern      *SavePattern
	TempFile     *FileOperation
	OriginalFile *FileOperation
	DeleteOp     *FileOperation
	RenameOp     *FileOperation
	Confidence   float64 // 0.0 - 1.0
	ShouldMerge  bool
	Reason       string
}

// SavePatternDetector 保存模式检测器
type SavePatternDetector struct {
	fs *OrcasFS
	mu sync.RWMutex

	// 操作历史记录（按目录分组）
	// key: dirID, value: 操作列表
	operations map[int64][]*FileOperation

	// 操作历史保留时间
	retentionPeriod time.Duration

	// 已注册的模式
	patterns []*SavePattern

	// 匹配结果缓存
	matches map[int64]*SavePatternMatch // key: fileID

	// 待处理的删除操作（用于拦截）
	pendingDeletes map[int64]*SavePatternMatch // key: fileID

	// 清理定时器
	cleanupTicker *time.Ticker
	stopChan      chan struct{}
}

// NewSavePatternDetector 创建保存模式检测器
func NewSavePatternDetector(fs *OrcasFS) *SavePatternDetector {
	spd := &SavePatternDetector{
		fs:              fs,
		operations:      make(map[int64][]*FileOperation),
		retentionPeriod: 5 * time.Minute, // 保留 5 分钟的操作历史
		patterns:        make([]*SavePattern, 0),
		matches:         make(map[int64]*SavePatternMatch),
		pendingDeletes:  make(map[int64]*SavePatternMatch),
		stopChan:        make(chan struct{}),
	}

	// 注册内置模式
	spd.registerBuiltinPatterns()

	// 启动清理任务
	spd.cleanupTicker = time.NewTicker(1 * time.Minute)
	go spd.cleanupLoop()

	return spd
}

// Stop 停止检测器
func (spd *SavePatternDetector) Stop() {
	close(spd.stopChan)
	if spd.cleanupTicker != nil {
		spd.cleanupTicker.Stop()
	}
}

// RecordOperation 记录操作
func (spd *SavePatternDetector) RecordOperation(op *FileOperation) {
	spd.mu.Lock()
	defer spd.mu.Unlock()

	// 添加时间戳
	if op.Timestamp == 0 {
		op.Timestamp = core.Now()
	}

	// 按目录分组
	dirID := op.ParentID
	if dirID == 0 {
		return
	}

	// 添加到操作历史
	spd.operations[dirID] = append(spd.operations[dirID], op)

	DebugLog("[SavePatternDetector] Recorded operation: op=%s, file=%s (ID=%d), dir=%d",
		op.Op, op.FileName, op.FileID, dirID)

	// 尝试检测模式
	spd.detectPatterns(dirID)
}

// detectPatterns 检测保存模式
func (spd *SavePatternDetector) detectPatterns(dirID int64) {
	ops := spd.operations[dirID]
	if len(ops) < 2 {
		return
	}

	// 只检查最近的操作（最多 20 个）
	startIdx := 0
	if len(ops) > 20 {
		startIdx = len(ops) - 20
	}
	recentOps := ops[startIdx:]

	// 对每个模式进行匹配
	for _, pattern := range spd.patterns {
		match := pattern.Matcher(recentOps)
		if match != nil && match.ShouldMerge {
			DebugLog("[SavePatternDetector] Detected pattern: %s, confidence=%.2f, reason=%s",
				pattern.Name, match.Confidence, match.Reason)

			// 缓存匹配结果
			if match.TempFile != nil {
				spd.matches[match.TempFile.FileID] = match
			}

			// 触发合并
			go spd.handleMatch(match)
		}
	}
}

// handleMatch 处理匹配结果
func (spd *SavePatternDetector) handleMatch(match *SavePatternMatch) {
	// 等待一小段时间，确保所有操作都已完成
	time.Sleep(2 * time.Second)

	// 验证文件状态
	if !spd.validateMatch(match) {
		DebugLog("[SavePatternDetector] Match validation failed: %s", match.Reason)
		return
	}

	// 执行合并
	err := spd.executeMerge(match)
	if err != nil {
		DebugLog("[SavePatternDetector] ERROR: Failed to execute merge: %v", err)
	} else {
		DebugLog("[SavePatternDetector] Successfully executed merge: %s", match.Reason)
	}
}

// validateMatch 验证匹配结果
func (spd *SavePatternDetector) validateMatch(match *SavePatternMatch) bool {
	// 检查临时文件是否存在且有数据
	if match.TempFile == nil {
		return false
	}

	tmpFileObj, err := spd.fs.h.Get(spd.fs.c, spd.fs.bktID, []int64{match.TempFile.FileID})
	if err != nil || len(tmpFileObj) == 0 {
		return false
	}

	if tmpFileObj[0].DataID == 0 || tmpFileObj[0].DataID == core.EmptyDataID {
		return false
	}

	// 检查原文件是否存在
	if match.OriginalFile == nil {
		return false
	}

	originalFileObj, err := spd.fs.h.Get(spd.fs.c, spd.fs.bktID, []int64{match.OriginalFile.FileID})
	if err != nil || len(originalFileObj) == 0 {
		return false
	}

	return true
}

// executeMerge 执行合并
func (spd *SavePatternDetector) executeMerge(match *SavePatternMatch) error {
	lh, ok := spd.fs.h.(*core.LocalHandler)
	if !ok {
		return fmt.Errorf("handler is not LocalHandler")
	}

	// 获取文件对象
	tmpFileObjs, err := spd.fs.h.Get(spd.fs.c, spd.fs.bktID, []int64{match.TempFile.FileID})
	if err != nil || len(tmpFileObjs) == 0 {
		return fmt.Errorf("temp file not found")
	}
	tmpFileObj := tmpFileObjs[0]

	originalFileObjs, err := spd.fs.h.Get(spd.fs.c, spd.fs.bktID, []int64{match.OriginalFile.FileID})
	if err != nil || len(originalFileObjs) == 0 {
		return fmt.Errorf("original file not found")
	}
	originalFileObj := originalFileObjs[0]

	// 1. 创建历史版本（保存原文件的旧数据）
	versionTime := core.Now()
	oldVersion := &core.ObjectInfo{
		ID:     core.NewID(),
		PID:    originalFileObj.ID,
		Type:   core.OBJ_TYPE_VERSION,
		Name:   fmt.Sprintf("%d", versionTime),
		DataID: originalFileObj.DataID,
		Size:   originalFileObj.Size,
		MTime:  versionTime,
	}

	// 2. 更新原文件（指向新数据）
	updatedFile := &core.ObjectInfo{
		ID:     originalFileObj.ID,
		PID:    originalFileObj.PID,
		Type:   originalFileObj.Type,
		Name:   originalFileObj.Name,
		DataID: tmpFileObj.DataID,
		Size:   tmpFileObj.Size,
		MTime:  core.Now(),
	}

	// 3. 执行操作（按顺序）
	// 3.1 创建版本
	_, err = lh.Put(spd.fs.c, spd.fs.bktID, []*core.ObjectInfo{oldVersion})
	if err != nil {
		return fmt.Errorf("failed to create version: %w", err)
	}

	// 3.2 更新原文件
	_, err = lh.Put(spd.fs.c, spd.fs.bktID, []*core.ObjectInfo{updatedFile})
	if err != nil {
		return fmt.Errorf("failed to update original file: %w", err)
	}

	// 3.3 删除临时文件
	err = lh.Delete(spd.fs.c, spd.fs.bktID, tmpFileObj.ID)
	if err != nil {
		return err
	}

	// 4. 更新缓存
	fileObjCache.Put(originalFileObj.ID, updatedFile)
	fileObjCache.Del(tmpFileObj.ID)

	// 5. 失效目录列表缓存
	if originalFileObj.PID > 0 {
		spd.fs.root.invalidateDirListCache(originalFileObj.PID)
	}

	DebugLog("[SavePatternDetector] Merge completed: version=%d, file=%d, deleted=%d",
		oldVersion.ID, updatedFile.ID, tmpFileObj.ID)

	return nil
}

// cleanupLoop 清理过期操作记录
func (spd *SavePatternDetector) cleanupLoop() {
	for {
		select {
		case <-spd.stopChan:
			return
		case <-spd.cleanupTicker.C:
			spd.cleanup()
		}
	}
}

// cleanup 清理过期记录
func (spd *SavePatternDetector) cleanup() {
	spd.mu.Lock()
	defer spd.mu.Unlock()

	now := core.Now()
	cutoff := now - int64(spd.retentionPeriod.Seconds())

	for dirID, ops := range spd.operations {
		// 过滤掉过期的操作
		validOps := make([]*FileOperation, 0, len(ops))
		for _, op := range ops {
			if op.Timestamp >= cutoff {
				validOps = append(validOps, op)
			}
		}

		if len(validOps) == 0 {
			delete(spd.operations, dirID)
		} else {
			spd.operations[dirID] = validOps
		}
	}

	// 清理过期的匹配结果
	for fileID, match := range spd.matches {
		if match.TempFile != nil && match.TempFile.Timestamp < cutoff {
			delete(spd.matches, fileID)
		}
	}
}

// registerBuiltinPatterns 注册内置模式
func (spd *SavePatternDetector) registerBuiltinPatterns() {
	// 模式 1: Office 标准保存模式
	// 创建临时文件 → 写入数据 → 删除原文件 → 重命名临时文件
	spd.patterns = append(spd.patterns, &SavePattern{
		Name:        "Office Standard Save",
		Description: "Create temp → Write → Delete original → Rename temp",
		Matcher:     spd.matchOfficeStandardSave,
	})

	// 模式 2: 原子替换模式
	// 创建临时文件 → 写入数据 → 重命名临时文件（覆盖原文件）
	spd.patterns = append(spd.patterns, &SavePattern{
		Name:        "Atomic Replace",
		Description: "Create temp → Write → Rename temp (replace original)",
		Matcher:     spd.matchAtomicReplace,
	})

	// 模式 3: 写入后删除模式
	// 写入临时文件 → 关闭 → 删除原文件（但没有重命名）
	spd.patterns = append(spd.patterns, &SavePattern{
		Name:        "Write and Delete",
		Description: "Write temp → Close → Delete original (no rename)",
		Matcher:     spd.matchWriteAndDelete,
	})
}

// matchOfficeStandardSave 匹配 Office 标准保存模式
func (spd *SavePatternDetector) matchOfficeStandardSave(ops []*FileOperation) *SavePatternMatch {
	// 查找模式：CREATE(temp) → WRITE(temp) → DELETE(original) → RENAME(temp→original)

	// 至少需要 4 个操作
	if len(ops) < 4 {
		return nil
	}

	// 从后往前查找（最近的操作）
	for i := len(ops) - 1; i >= 3; i-- {
		// 查找 RENAME 操作
		if ops[i].Op != OpRename {
			continue
		}
		renameOp := ops[i]

		// 查找对应的 DELETE 操作（删除原文件）
		var deleteOp *FileOperation
		for j := i - 1; j >= 0 && j >= i-5; j-- {
			if ops[j].Op == OpDelete && ops[j].FileName == renameOp.NewName {
				deleteOp = ops[j]
				break
			}
		}

		if deleteOp == nil {
			continue
		}

		// 查找对应的 WRITE 操作（写入临时文件）
		var writeOp *FileOperation
		for j := i - 1; j >= 0 && j >= i-10; j-- {
			if ops[j].Op == OpWrite && ops[j].FileID == renameOp.FileID {
				writeOp = ops[j]
				break
			}
		}

		if writeOp == nil {
			continue
		}

		// 查找对应的 CREATE 操作（创建临时文件）
		var createOp *FileOperation
		for j := i - 1; j >= 0 && j >= i-15; j-- {
			if ops[j].Op == OpCreate && ops[j].FileID == renameOp.FileID {
				createOp = ops[j]
				break
			}
		}

		// 验证时间顺序
		if createOp != nil && writeOp != nil && deleteOp != nil {
			if createOp.Timestamp <= writeOp.Timestamp &&
				writeOp.Timestamp <= deleteOp.Timestamp &&
				deleteOp.Timestamp <= renameOp.Timestamp {

				// 验证时间间隔（不超过 30 秒）
				if renameOp.Timestamp-createOp.Timestamp <= 30 {
					return &SavePatternMatch{
						Pattern:      spd.patterns[0],
						TempFile:     renameOp,
						OriginalFile: deleteOp,
						DeleteOp:     deleteOp,
						RenameOp:     renameOp,
						Confidence:   0.95,
						ShouldMerge:  true,
						Reason:       "Detected Office standard save pattern",
					}
				}
			}
		}
	}

	return nil
}

// matchAtomicReplace 匹配原子替换模式
func (spd *SavePatternDetector) matchAtomicReplace(ops []*FileOperation) *SavePatternMatch {
	// 查找模式：CREATE(temp) → WRITE(temp) → RENAME(temp→original, 覆盖)

	if len(ops) < 3 {
		return nil
	}

	for i := len(ops) - 1; i >= 2; i-- {
		if ops[i].Op != OpRename {
			continue
		}
		renameOp := ops[i]

		// 查找对应的 WRITE 操作
		var writeOp *FileOperation
		for j := i - 1; j >= 0 && j >= i-5; j-- {
			if ops[j].Op == OpWrite && ops[j].FileID == renameOp.FileID {
				writeOp = ops[j]
				break
			}
		}

		if writeOp == nil {
			continue
		}

		// 查找对应的 CREATE 操作
		var createOp *FileOperation
		for j := i - 1; j >= 0 && j >= i-10; j-- {
			if ops[j].Op == OpCreate && ops[j].FileID == renameOp.FileID {
				createOp = ops[j]
				break
			}
		}

		if createOp != nil {
			// 检查是否有对应的原文件
			// 通过查找之前是否有同名文件的操作
			var originalOp *FileOperation
			for j := i - 1; j >= 0; j-- {
				if ops[j].FileName == renameOp.NewName && ops[j].FileID != renameOp.FileID {
					originalOp = ops[j]
					break
				}
			}

			if originalOp != nil {
				return &SavePatternMatch{
					Pattern:      spd.patterns[1],
					TempFile:     renameOp,
					OriginalFile: originalOp,
					RenameOp:     renameOp,
					Confidence:   0.90,
					ShouldMerge:  true,
					Reason:       "Detected atomic replace pattern",
				}
			}
		}
	}

	return nil
}

// matchWriteAndDelete 匹配写入后删除模式
func (spd *SavePatternDetector) matchWriteAndDelete(ops []*FileOperation) *SavePatternMatch {
	// 查找模式：WRITE(temp) → CLOSE(temp) → DELETE(original)
	// 这种情况下，临时文件已写入但没有重命名，原文件被删除
	// 我们应该自动完成重命名

	if len(ops) < 3 {
		return nil
	}

	for i := len(ops) - 1; i >= 2; i-- {
		if ops[i].Op != OpDelete {
			continue
		}
		deleteOp := ops[i]

		// 查找对应的 CLOSE 操作
		var closeOp *FileOperation
		for j := i - 1; j >= 0 && j >= i-5; j-- {
			if ops[j].Op == OpClose {
				closeOp = ops[j]
				break
			}
		}

		if closeOp == nil {
			continue
		}

		// 查找对应的 WRITE 操作
		var writeOp *FileOperation
		for j := i - 1; j >= 0 && j >= i-10; j-- {
			if ops[j].Op == OpWrite && ops[j].FileID == closeOp.FileID {
				writeOp = ops[j]
				break
			}
		}

		if writeOp != nil {
			// 检查临时文件名是否与删除的文件相关
			// 例如：test~ABC.tmp 和 test.ppt
			if spd.isRelatedFile(writeOp.FileName, deleteOp.FileName) {
				return &SavePatternMatch{
					Pattern:      spd.patterns[2],
					TempFile:     writeOp,
					OriginalFile: deleteOp,
					DeleteOp:     deleteOp,
					Confidence:   0.85,
					ShouldMerge:  true,
					Reason:       "Detected write and delete pattern (incomplete save)",
				}
			}
		}
	}

	return nil
}

// isRelatedFile 判断两个文件是否相关
func (spd *SavePatternDetector) isRelatedFile(tempName, originalName string) bool {
	// 移除扩展名
	tempBase := removeExtension(tempName)
	originalBase := removeExtension(originalName)

	// 检查临时文件名是否包含原文件名
	// 例如：test~ABC.tmp 包含 test
	if len(tempBase) > len(originalBase) {
		if tempBase[:len(originalBase)] == originalBase {
			return true
		}
	}

	return false
}

// removeExtension 移除文件扩展名
func removeExtension(filename string) string {
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] == '.' {
			return filename[:i]
		}
	}
	return filename
}

// ========== 拦截方法（主动检测和处理） ==========

// ShouldInterceptDelete 检查是否应该拦截删除操作
// 返回：(是否拦截, 匹配信息)
func (spd *SavePatternDetector) ShouldInterceptDelete(
	fileID int64, fileName string, parentID int64) (bool, *SavePatternMatch) {
	
	spd.mu.RLock()
	defer spd.mu.RUnlock()
	
	// 快速路径：检查是否有该目录的操作记录
	ops := spd.operations[parentID]
	if len(ops) == 0 {
		return false, nil
	}
	
	// 查找最近的相关临时文件写入操作
	now := core.Now()
	for i := len(ops) - 1; i >= 0 && i >= len(ops)-20; i-- {
		op := ops[i]
		
		// 只检查写入操作
		if op.Op != OpWrite {
			continue
		}
		
		// 检查时间间隔（不超过 30 秒）
		if now-op.Timestamp > 30 {
			break // 太旧了，后面的更旧
		}
		
		// 检查是否是相关的临时文件
		if spd.isRelatedFile(op.FileName, fileName) {
			DebugLog("[SavePatternDetector] Detected delete interception: original=%s, temp=%s, tempFileID=%d",
				fileName, op.FileName, op.FileID)
			
			return true, &SavePatternMatch{
				TempFile: op,
				OriginalFile: &FileOperation{
					FileID:    fileID,
					FileName:  fileName,
					ParentID:  parentID,
					Timestamp: now,
				},
				ShouldMerge: true,
				Confidence:  0.9,
				Reason:      "Detected Office save pattern (delete original before rename)",
			}
		}
	}
	
	return false, nil
}

// ShouldInterceptRename 检查是否应该拦截重命名操作
// 返回：(是否拦截, 匹配信息)
func (spd *SavePatternDetector) ShouldInterceptRename(
	fileID int64, oldName, newName string, oldParent, newParent int64) (bool, *SavePatternMatch) {
	
	spd.mu.RLock()
	defer spd.mu.RUnlock()
	
	// 快速路径：检查是否是临时文件重命名为正式文件
	if !spd.isRelatedFile(oldName, newName) {
		return false, nil
	}
	
	// 检查是否有对应的写入操作
	ops := spd.operations[oldParent]
	if len(ops) == 0 {
		return false, nil
	}
	
	// 查找对应的写入操作
	now := core.Now()
	for i := len(ops) - 1; i >= 0 && i >= len(ops)-20; i-- {
		op := ops[i]
		
		// 只检查写入操作
		if op.Op != OpWrite {
			continue
		}
		
		// 检查是否是同一个文件
		if op.FileID != fileID {
			continue
		}
		
		// 检查时间间隔（不超过 30 秒）
		if now-op.Timestamp > 30 {
			break
		}
		
		DebugLog("[SavePatternDetector] Detected rename interception: %s -> %s, fileID=%d",
			oldName, newName, fileID)
		
		return true, &SavePatternMatch{
			TempFile: op,
			OriginalFile: &FileOperation{
				FileName:  newName,
				ParentID:  newParent,
				Timestamp: now,
			},
			RenameOp: &FileOperation{
				FileID:    fileID,
				FileName:  oldName,
				ParentID:  oldParent,
				NewName:   newName,
				NewParent: newParent,
				Timestamp: now,
			},
			ShouldMerge: true,
			Confidence:  0.9,
			Reason:      "Detected Office save pattern (rename temp to original)",
		}
	}
	
	return false, nil
}

// HandleDeleteWithMerge 处理删除操作并执行合并
// 这个函数只是标记待处理的删除，实际合并在 Rename 时完成
func (spd *SavePatternDetector) HandleDeleteWithMerge(match *SavePatternMatch) error {
	DebugLog("[SavePatternDetector] Handling delete with merge: original=%s (fileID=%d), temp=%s (fileID=%d)",
		match.OriginalFile.FileName, match.OriginalFile.FileID,
		match.TempFile.FileName, match.TempFile.FileID)
	
	// 记录这个待处理的删除操作
	spd.mu.Lock()
	spd.pendingDeletes[match.OriginalFile.FileID] = match
	spd.mu.Unlock()
	
	DebugLog("[SavePatternDetector] Marked file for pending delete: fileID=%d, name=%s",
		match.OriginalFile.FileID, match.OriginalFile.FileName)
	
	return nil
}

// HandleRenameWithMerge 处理重命名操作并执行合并
func (spd *SavePatternDetector) HandleRenameWithMerge(match *SavePatternMatch) error {
	DebugLog("[SavePatternDetector] Handling rename with merge: %s -> %s (fileID=%d)",
		match.RenameOp.FileName, match.RenameOp.NewName, match.RenameOp.FileID)
	
	// 检查是否有待处理的删除操作（同名原文件）
	spd.mu.Lock()
	var pendingDelete *SavePatternMatch
	var pendingDeleteFileID int64
	
	// 查找是否有同名文件的待处理删除
	for fileID, pd := range spd.pendingDeletes {
		if pd.OriginalFile.FileName == match.RenameOp.NewName &&
			pd.OriginalFile.ParentID == match.RenameOp.NewParent {
			pendingDelete = pd
			pendingDeleteFileID = fileID
			break
		}
	}
	
	if pendingDeleteFileID > 0 {
		delete(spd.pendingDeletes, pendingDeleteFileID)
	}
	spd.mu.Unlock()
	
	// 获取原文件对象（如果存在）
	var originalFileObj *core.ObjectInfo
	if pendingDelete != nil {
		// 从数据库获取原文件
		objs, err := spd.fs.h.Get(spd.fs.c, spd.fs.bktID, []int64{pendingDelete.OriginalFile.FileID})
		if err == nil && len(objs) > 0 {
			originalFileObj = objs[0]
			DebugLog("[SavePatternDetector] Found original file from pending delete: fileID=%d, name=%s, dataID=%d",
				originalFileObj.ID, originalFileObj.Name, originalFileObj.DataID)
		}
	} else {
		// 查找同名文件
		children, _, _, err := spd.fs.h.List(spd.fs.c, spd.fs.bktID, match.RenameOp.NewParent, core.ListOptions{
			Count: core.DefaultListPageSize,
		})
		if err == nil {
			for _, child := range children {
				if child.Name == match.RenameOp.NewName && child.Type == core.OBJ_TYPE_FILE {
					originalFileObj = child
					DebugLog("[SavePatternDetector] Found original file from list: fileID=%d, name=%s, dataID=%d",
						originalFileObj.ID, originalFileObj.Name, originalFileObj.DataID)
					break
				}
			}
		}
	}
	
	// 获取临时文件对象
	tmpFileObjs, err := spd.fs.h.Get(spd.fs.c, spd.fs.bktID, []int64{match.TempFile.FileID})
	if err != nil || len(tmpFileObjs) == 0 {
		return fmt.Errorf("temp file not found: fileID=%d", match.TempFile.FileID)
	}
	tmpFileObj := tmpFileObjs[0]
	
	if originalFileObj != nil && originalFileObj.DataID > 0 {
		// 原文件存在且有数据，创建版本并合并
		versionTime := core.Now()
		oldVersion := &core.ObjectInfo{
			ID:     core.NewID(),
			PID:    originalFileObj.ID,
			Type:   core.OBJ_TYPE_VERSION,
			Name:   fmt.Sprintf("%d", versionTime),
			DataID: originalFileObj.DataID,
			Size:   originalFileObj.Size,
			MTime:  versionTime,
		}
		
		_, err = spd.fs.h.Put(spd.fs.c, spd.fs.bktID, []*core.ObjectInfo{oldVersion})
		if err != nil {
			DebugLog("[SavePatternDetector] WARNING: Failed to create version: %v", err)
		} else {
			DebugLog("[SavePatternDetector] Created version %d for file %d (dataID=%d, size=%d)",
				oldVersion.ID, originalFileObj.ID, oldVersion.DataID, oldVersion.Size)
		}
		
		// 更新原文件指向新数据
		originalFileObj.DataID = tmpFileObj.DataID
		originalFileObj.Size = tmpFileObj.Size
		originalFileObj.MTime = core.Now()
		
		_, err = spd.fs.h.Put(spd.fs.c, spd.fs.bktID, []*core.ObjectInfo{originalFileObj})
		if err != nil {
			return fmt.Errorf("failed to update original file: %w", err)
		}
		
		DebugLog("[SavePatternDetector] Updated original file %d with new data: dataID=%d, size=%d",
			originalFileObj.ID, originalFileObj.DataID, originalFileObj.Size)
		
		// 删除临时文件
		err = spd.fs.h.Delete(spd.fs.c, spd.fs.bktID, tmpFileObj.ID)
		if err != nil {
			DebugLog("[SavePatternDetector] WARNING: Failed to delete temp file: %v", err)
		} else {
			DebugLog("[SavePatternDetector] Deleted temp file %d", tmpFileObj.ID)
		}
		
		// 更新缓存
		fileObjCache.Put(originalFileObj.ID, originalFileObj)
		fileObjCache.Del(tmpFileObj.ID)
		spd.fs.root.invalidateDirListCache(originalFileObj.PID)
		
		DebugLog("[SavePatternDetector] Merge completed: file=%d, version=%d, temp=%d deleted",
			originalFileObj.ID, oldVersion.ID, tmpFileObj.ID)
	} else {
		// 原文件不存在或没有数据，直接重命名临时文件
		tmpFileObj.Name = match.RenameOp.NewName
		if match.RenameOp.NewParent != match.RenameOp.ParentID {
			tmpFileObj.PID = match.RenameOp.NewParent
		}
		tmpFileObj.MTime = core.Now()
		
		_, err = spd.fs.h.Put(spd.fs.c, spd.fs.bktID, []*core.ObjectInfo{tmpFileObj})
		if err != nil {
			return fmt.Errorf("failed to rename temp file: %w", err)
		}
		
		fileObjCache.Put(tmpFileObj.ID, tmpFileObj)
		spd.fs.root.invalidateDirListCache(tmpFileObj.PID)
		
		DebugLog("[SavePatternDetector] Renamed temp file: fileID=%d, %s -> %s (no original file to merge)",
			tmpFileObj.ID, match.RenameOp.FileName, match.RenameOp.NewName)
	}
	
	return nil
}
