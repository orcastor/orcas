package core

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// workLocks 用于确保同一个key同一时间只有一个工作在执行
// key: string, value: chan struct{} (关闭时表示处理完成)
var workLocks sync.Map

// acquireWorkLock 获取工作锁，确保同一个key同一时间只有一个工作在执行
// key: 唯一标识符
// 返回值: acquired表示是否成功获取锁，release是释放锁的函数
// 如果已有工作在进行，会等待它完成后再尝试获取
func acquireWorkLock(key string) (acquired bool, release func()) {
	lockChan := make(chan struct{})
	actual, loaded := workLocks.LoadOrStore(key, lockChan)
	if loaded {
		// 已经有其他goroutine在处理，等待它完成
		existingChan := actual.(chan struct{})
		<-existingChan // 等待现有处理完成（channel关闭）
		// 处理完成后，再次尝试获取锁
		lockChan = make(chan struct{})
		_, loaded = workLocks.LoadOrStore(key, lockChan)
		if loaded {
			// 如果还是被占用，说明又有新的处理开始了，返回失败
			return false, nil
		}
	}

	// 返回成功，并提供一个释放锁的函数
	return true, func() {
		workLocks.Delete(key)
		close(lockChan)
	}
}

// ResourceController 资源控制器，用于限制资源密集型操作的使用
type ResourceController struct {
	config          ResourceControlConfig
	startTime       time.Time
	processedItems  int64
	lastCheckTime   time.Time
	rateLimitTokens int64
	rateLimitMutex  sync.Mutex
}

// NewResourceController 创建新的资源控制器
func NewResourceController(config ResourceControlConfig) *ResourceController {
	rc := &ResourceController{
		config:          config,
		startTime:       time.Now(),
		processedItems:  0,
		lastCheckTime:   time.Now(),
		rateLimitTokens: int64(config.MaxItemsPerSecond), // 初始令牌数等于每秒限制
	}
	return rc
}

// ShouldStop 检查是否应该停止处理（超过最大时长）
func (rc *ResourceController) ShouldStop() bool {
	if rc.config.MaxDuration <= 0 {
		return false
	}
	return time.Since(rc.startTime) >= rc.config.MaxDuration
}

// WaitIfNeeded 在批次处理之间等待（实现批次间隔和速率限制）
// itemsProcessed 本次批次处理的项数
func (rc *ResourceController) WaitIfNeeded(itemsProcessed int) {
	rc.processedItems += int64(itemsProcessed)

	// 速率限制（令牌桶算法）
	if rc.config.MaxItemsPerSecond > 0 {
		rc.rateLimitMutex.Lock()
		now := time.Now()
		elapsed := now.Sub(rc.lastCheckTime)

		// 每秒补充令牌
		if elapsed > 0 {
			tokensToAdd := int64(rc.config.MaxItemsPerSecond) * int64(elapsed) / int64(time.Second)
			rc.rateLimitTokens += tokensToAdd
			if rc.rateLimitTokens > int64(rc.config.MaxItemsPerSecond) {
				rc.rateLimitTokens = int64(rc.config.MaxItemsPerSecond)
			}
		}

		// 消耗令牌
		if rc.rateLimitTokens < int64(itemsProcessed) {
			// 令牌不足，需要等待
			deficit := int64(itemsProcessed) - rc.rateLimitTokens
			waitTime := time.Duration(deficit) * time.Second / time.Duration(rc.config.MaxItemsPerSecond)
			if waitTime > 0 {
				time.Sleep(waitTime)
				// 等待后补充令牌
				rc.rateLimitTokens = int64(rc.config.MaxItemsPerSecond)
			}
		} else {
			rc.rateLimitTokens -= int64(itemsProcessed)
		}

		rc.lastCheckTime = now
		rc.rateLimitMutex.Unlock()
	}

	// 批次间隔延迟
	if rc.config.BatchInterval > 0 {
		delay := rc.config.BatchInterval

		// 自适应延迟：根据已处理项数动态调整延迟
		if rc.config.AdaptiveDelay && rc.config.AdaptiveDelayFactor > 0 {
			adaptiveFactor := 1.0 + float64(rc.processedItems)/float64(rc.config.AdaptiveDelayFactor)
			delay = time.Duration(float64(delay) * adaptiveFactor)
			// 限制最大延迟为10倍基础延迟，避免过度延迟
			maxDelay := rc.config.BatchInterval * 10
			if delay > maxDelay {
				delay = maxDelay
			}
		}

		time.Sleep(delay)
	}
}

// GetProcessedItems 获取已处理的项数
func (rc *ResourceController) GetProcessedItems() int64 {
	return rc.processedItems
}

// GetElapsedTime 获取已运行时间
func (rc *ResourceController) GetElapsedTime() time.Duration {
	return time.Since(rc.startTime)
}

// delayedDelete 延迟删除数据文件
// 等待指定时间后检查数据是否仍未被引用，如果未被引用则删除
// 确保同一个dataID同一时间只有一个删除操作在执行
func delayedDelete(c Ctx, bktID, dataID int64, ma MetadataAdapter, da DataAdapter) {
	go func() {
		// 生成唯一key，确保同一个dataID同一时间只有一个删除操作
		key := fmt.Sprintf("delayed_delete_%d_%d", bktID, dataID)

		// 获取工作锁
		acquired, release := acquireWorkLock(key)
		if !acquired {
			// 无法获取锁，说明已有其他goroutine在处理同一个dataID的删除，直接返回
			return
		}
		// 确保在处理完成后释放锁
		defer release()

		// 等待指定时间
		time.Sleep(time.Duration(DeleteDelaySeconds) * time.Second)

		// 检查数据是否仍未被引用
		refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
		if err != nil {
			return // 查询失败，不删除
		}

		if refCounts[dataID] == 0 {
			// 没有引用，可以安全删除
			// 先检查是否是打包数据
			dataInfo, err := ma.GetData(c, bktID, dataID)
			if err == nil && dataInfo != nil {
				if dataInfo.PkgID > 0 && dataInfo.PkgID != dataID {
					// 是打包数据（但不是打包文件本身），不删除打包文件本身
					// 只删除元数据
					ma.DeleteData(c, bktID, []int64{dataID})
				} else {
					// 非打包数据或打包文件本身，删除数据文件和元数据
					// 对于打包文件，如果它本身没有被引用，说明可以安全删除
					// 先删除元数据，再删除文件，避免出现有元数据但没有文件的情况
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						// 元数据删除失败，不继续删除文件
						return
					}

					dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
					if dataSize > 0 {
						ma.DecBktRealUsed(c, bktID, dataSize)
					}
					deleteDataFiles(ORCAS_DATA, bktID, dataID, ma, c)
				}
			}
		}
	}()
}

// asyncApplyVersionRetention 异步应用版本保留策略（后置处理）
// 在版本创建成功后，统一处理：
// 1. 时间窗口内的版本合并（删除时间窗口内的旧版本，只保留最新版本）
// 2. 版本数量限制（删除超出数量的最旧版本）
// 确保同一个父对象同一时间只有一个版本保留处理在执行
func asyncApplyVersionRetention(c Ctx, bktID, fileID int64, newVersionTime int64, ma MetadataAdapter, da DataAdapter) {
	go func() {
		// 获取工作锁
		acquired, release := acquireWorkLock(fmt.Sprintf("version_retention_%d_%d", bktID, fileID))
		if !acquired {
			// 无法获取锁，直接返回
			return
		}
		// 确保在处理完成后释放锁
		defer release()
		// 等待一小段时间，确保版本已经创建成功
		time.Sleep(100 * time.Millisecond)

		// 查询文件的所有版本（包括刚创建的版本）
		versions, err := ma.ListVersions(c, bktID, fileID)
		if err != nil {
			// 查询失败，不处理
			return
		}

		if len(versions) == 0 {
			return
		}

		var versionsToDelete []*ObjectInfo

		config := GetVersionRetentionConfig()
		// 1. 处理时间窗口内的版本合并：删除时间窗口内的旧版本
		if config.MinVersionInterval > 0 && len(versions) > 1 {
			// 最新版本是刚创建的版本（versions[0]）
			// 从第二个版本开始检查，如果时间间隔小于最小间隔，标记为删除
			for i := 1; i < len(versions); i++ {
				version := versions[i]
				timeDiff := newVersionTime - version.MTime

				// 如果时间间隔小于最小间隔，需要删除这个版本（合并）
				if timeDiff < config.MinVersionInterval {
					versionsToDelete = append(versionsToDelete, version)
				} else {
					// 由于版本是按时间降序排列，如果这个版本不在时间窗口内，后面的版本也不会在
					break
				}
			}
		}

		// 2. 处理版本数量限制：如果总版本数（减去待删除的版本）仍然超过限制，删除最旧的版本
		remainingVersions := len(versions) - len(versionsToDelete)
		if config.MaxVersions > 0 && remainingVersions > int(config.MaxVersions) {
			// 需要额外删除的版本数
			additionalToDelete := remainingVersions - int(config.MaxVersions)

			// 从后往前删除最旧的版本（ListVersions返回的是按MTime降序，所以最后的是最旧的）
			// 跳过已经标记为删除的版本
			alreadyMarked := make(map[int64]bool)
			for _, v := range versionsToDelete {
				alreadyMarked[v.ID] = true
			}

			for i := len(versions) - 1; i >= 0 && additionalToDelete > 0; i-- {
				version := versions[i]
				// 如果已经在待删除列表中，跳过
				if alreadyMarked[version.ID] {
					continue
				}

				versionsToDelete = append(versionsToDelete, version)
				additionalToDelete--
			}
		}

		// 3. 执行删除操作
		for _, versionToDelete := range versionsToDelete {
			// 删除版本对象（标记为已删除）
			if err := ma.DeleteObj(c, bktID, versionToDelete.ID); err != nil {
				// 删除失败，记录错误但继续处理其他版本
				continue
			}

			// 如果版本有DataID，使用延迟删除处理数据文件
			if versionToDelete.DataID > 0 && versionToDelete.DataID != EmptyDataID {
				delayedDelete(c, bktID, versionToDelete.DataID, ma, da)
			}
		}
	}()
}

// UpdateFileLatestVersion 循环更新目录的大小和DataID
// 假设：上传新版本时已经更新了文件的DataID和大小，所以只需要更新目录
// 循环处理目录，直到大小和DataID稳定（从叶子目录开始向上累加）
func UpdateFileLatestVersion(c Ctx, bktID int64, ma MetadataAdapter) error {
	// 循环更新目录大小和DataID，直到没有变化
	maxIterations := 100 // 防止无限循环
	for iteration := 0; iteration < maxIterations; iteration++ {
		hasChange := false

		// 查询所有目录对象
		dirs, err := ma.ListObjsByType(c, bktID, OBJ_TYPE_DIR)
		if err != nil {
			break
		}

		// 计算每个目录的大小和DataID
		for _, dir := range dirs {
			var totalSize int64
			var maxDataID int64 = dir.DataID // 默认为目录自己的DataID

			// 查询该目录下的所有子对象（未删除的）
			children, err := ma.ListChildren(c, bktID, dir.ID)
			if err != nil {
				continue
			}

			// 累加子对象的大小，并找到最大的DataID
			for _, child := range children {
				totalSize += child.Size
				// 目录的DataID应该是所有子对象中最大的DataID
				if child.DataID > maxDataID {
					maxDataID = child.DataID
				}
			}

			// 如果有变化，更新目录
			if totalSize != dir.Size || maxDataID != dir.DataID {
				updateObj := &ObjectInfo{
					ID:     dir.ID,
					Size:   totalSize,
					DataID: maxDataID,
				}
				err = ma.SetObj(c, bktID, []string{"size", "did"}, updateObj)
				if err == nil {
					hasChange = true
				}
			}
		}

		// 如果没有变化，退出循环
		if !hasChange {
			break
		}
	}

	return nil
}

// DeleteObject 标记对象为已删除（递归删除子对象）
func DeleteObject(c Ctx, bktID, id int64, ma MetadataAdapter) error {
	// 获取对象信息
	objs, err := ma.GetObj(c, bktID, []int64{id})
	if err != nil || len(objs) == 0 {
		return err
	}
	obj := objs[0]

	// 如果是目录，递归删除所有子对象
	if obj.Type == OBJ_TYPE_DIR {
		// 获取所有子对象（包括已删除的，使用原始SQL查询）
		children, err := listChildrenDirectly(c, bktID, id, ma)
		if err != nil {
			return err
		}
		// 递归删除子对象
		for _, child := range children {
			// 只删除未删除的对象（PID >= 0）
			if child.PID >= 0 {
				if err := DeleteObject(c, bktID, child.ID, ma); err != nil {
					return err
				}
			}
		}
	}

	// 标记当前对象为已删除
	return ma.DeleteObj(c, bktID, id)
}

// PermanentlyDeleteObject 彻底删除对象（物理删除对象和数据文件）
// 确保同一个对象ID同一时间只有一个删除操作在执行
func PermanentlyDeleteObject(c Ctx, bktID, id int64, h Handler, ma MetadataAdapter, da DataAdapter) error {
	// 生成唯一key，确保同一个对象ID同一时间只有一个删除操作
	key := fmt.Sprintf("permanently_delete_object_%d_%d", bktID, id)

	// 获取工作锁
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// 无法获取锁，说明已有其他goroutine在处理，返回错误
		return fmt.Errorf("permanently delete object operation already in progress for object %d", id)
	}
	// 确保在处理完成后释放锁
	defer release()

	// 获取对象信息
	objs, err := ma.GetObj(c, bktID, []int64{id})
	if err != nil || len(objs) == 0 {
		return err
	}
	obj := objs[0]

	// 如果是目录，递归删除所有子对象
	if obj.Type == OBJ_TYPE_DIR {
		// 获取所有子对象（包括已删除的）
		children, err := listChildrenDirectly(c, bktID, id, ma)
		if err != nil {
			return err
		}
		// 递归删除子对象（只删除未删除的对象）
		for _, child := range children {
			if child.PID >= 0 {
				if err := PermanentlyDeleteObject(c, bktID, child.ID, h, ma, da); err != nil {
					return err
				}
			}
		}
	}

	// 减少逻辑使用量（对象的原始大小）
	if obj.Type == OBJ_TYPE_FILE && obj.Size > 0 {
		if err := ma.DecBktUsed(c, bktID, obj.Size); err != nil {
			// 如果减少逻辑使用量失败，记录错误但继续删除
		}
		// 同时减少逻辑占用（对象已删除，不再是有效对象）
		if err := ma.DecBktLogicalUsed(c, bktID, obj.Size); err != nil {
			// 如果减少逻辑占用失败，记录错误但继续删除
		}

		// 检查是否是秒传对象：如果DataID被其他对象引用，说明删除后需要减少秒传节省空间
		if obj.DataID > 0 && obj.DataID != EmptyDataID {
			// 查询DataID的引用计数（不包括当前要删除的对象，因为CountDataRefs只统计pid >= 0的对象）
			// 但删除时对象还未被标记删除，所以引用计数会包含当前对象
			// 如果引用计数 > 1，说明还有其他对象引用，删除后需要减少秒传节省空间
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{obj.DataID})
			if err == nil {
				refCount := refCounts[obj.DataID]
				// 如果引用计数 > 1，说明还有其他对象引用这个DataID，删除当前对象后需要减少秒传节省空间
				if refCount > 1 {
					// 减少秒传节省空间（因为删除后，这个DataID的秒传节省会减少）
					if err := ma.DecBktDedupSavings(c, bktID, obj.Size); err != nil {
						// 如果减少秒传节省空间失败，记录错误但继续删除
					}
				}
			}
		}
	}

	// 处理当前对象的数据文件
	if obj.DataID > 0 && obj.DataID != EmptyDataID {
		// 检查数据的引用计数（在删除对象之前统计，所以会包含当前对象）
		refCounts, err := ma.CountDataRefs(c, bktID, []int64{obj.DataID})
		if err != nil {
			// 如果查询失败，跳过数据清理（避免误删）
			refCounts = make(map[int64]int64)
		}

		// CountDataRefs 只统计 pid >= 0 的对象，所以会包含当前对象
		// 如果 refCount == 1，说明只有当前对象在引用这个数据，可以安全删除
		// 如果 refCount > 1，说明还有其他对象在引用，不能删除数据文件
		refCount := refCounts[obj.DataID]
		if refCount == 1 {
			// 只有当前对象引用，检查是否是打包数据
			dataInfo, err := ma.GetData(c, bktID, obj.DataID)
			if err == nil && dataInfo != nil && dataInfo.PkgID > 0 {
				// 是打包数据，用idgen生成新的负数dataID，更新DataInfo的ID标记删除
				newDataID := h.NewID()
				if newDataID > 0 {
					negativeDataID := -newDataID
					// 只更新当前DataInfo的ID为负数（标记删除）
					dataInfo.ID = negativeDataID
					ma.PutData(c, bktID, []*DataInfo{dataInfo})
				}
				// 不删除打包文件本身，也不减少RealUsed
				// 碎片整理时会清理相同pkgID和pkgOffset的数据块
			} else {
				// 非打包数据，计算数据文件总大小并减少实际使用量
				dataSize := calculateDataSize(ORCAS_DATA, bktID, obj.DataID)
				if dataSize > 0 {
					// 减少桶的实际使用量
					if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
						// 如果减少使用量失败，仍然删除文件（避免数据泄露）
						// 但记录错误
					}
				}
				// 删除数据文件
				deleteDataFiles(ORCAS_DATA, bktID, obj.DataID, ma, c)
			}
		}
		// 如果 refCount == 0，可能是异常情况，为安全起见不删除数据文件
	}

	// 从数据库中物理删除对象
	return deleteObjFromDB(c, bktID, id)
}

// CleanRecycleBin 清理回收站中已标记删除的对象（物理删除未被引用的数据文件和元数据）
// targetID 为 0 表示清理所有符合条件的对象，否则只清理指定对象
// 确保同一个bktID同一时间只有一个清理操作在执行
func CleanRecycleBin(c Ctx, bktID int64, h Handler, ma MetadataAdapter, da DataAdapter, targetID int64) error {
	// 生成唯一key，确保同一个bktID同一时间只有一个清理操作
	key := fmt.Sprintf("clean_recycle_bin_%d", bktID)

	// 获取工作锁
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// 无法获取锁，说明已有其他goroutine在处理，返回错误
		return fmt.Errorf("clean recycle bin operation already in progress for bucket %d", bktID)
	}
	// 确保在处理完成后释放锁
	defer release()

	var deletedObjs []*ObjectInfo
	var err error

	if targetID > 0 {
		// 只清理指定对象
		objs, err := ma.GetObj(c, bktID, []int64{targetID})
		if err != nil {
			return err
		}
		if len(objs) == 0 {
			return nil // 对象不存在
		}
		// 检查是否已删除（PID < 0 表示已删除）
		if objs[0].PID < 0 {
			deletedObjs = objs
		} else {
			return nil // 对象未删除
		}
	} else {
		// 清理所有已删除超过一定时间的对象（留出窗口时间，默认7天）
		// 使用分页处理，避免一次性加载大量数据
		windowTime := time.Now().Unix() - 7*24*3600 // 7天前
		pageSize := DefaultListPageSize             // 每页处理对象数

		// 收集所有需要检查的DataID和要删除的对象ID
		dataIDs := make(map[int64]bool)
		objIDsToDelete := make([]int64, 0)

		for {
			// 分页获取已删除的对象
			pageObjs, err := ma.ListDeletedObjs(c, bktID, windowTime, pageSize)
			if err != nil {
				return err
			}

			if len(pageObjs) == 0 {
				break // 没有更多数据
			}

			// 收集当前页的DataID和对象ID
			for _, obj := range pageObjs {
				if obj.DataID > 0 && obj.DataID != EmptyDataID {
					dataIDs[obj.DataID] = true
				}
				objIDsToDelete = append(objIDsToDelete, obj.ID)
			}

			// 如果返回的数据少于pageSize，说明已经是最后一页
			if len(pageObjs) < pageSize {
				break
			}
		}

		if len(dataIDs) == 0 && len(objIDsToDelete) == 0 {
			return nil
		}

		// 转换为切片
		dataIDList := make([]int64, 0, len(dataIDs))
		for dataID := range dataIDs {
			dataIDList = append(dataIDList, dataID)
		}

		// 统计DataID的引用计数（排除已删除的对象）
		refCounts, err := ma.CountDataRefs(c, bktID, dataIDList)
		if err != nil {
			// 如果查询失败，跳过数据清理，只删除元数据
			refCounts = make(map[int64]int64)
		}

		// 删除未被引用的数据文件
		for _, dataID := range dataIDList {
			if refCounts[dataID] == 0 {
				// 检查是否是打包数据
				dataInfo, err := ma.GetData(c, bktID, dataID)
				if err == nil && dataInfo != nil && dataInfo.PkgID > 0 {
					// 不删除打包文件本身，也不减少RealUsed
					// 碎片整理时会清理相同pkgID和pkgOffset的数据块
				} else {
					// 非打包数据，计算数据文件总大小并减少实际使用量
					dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
					if dataSize > 0 {
						// 减少桶的实际使用量
						if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
							// 如果减少使用量失败，仍然删除文件（避免数据泄露）
						}
					}
					// 数据未被引用，可以安全删除文件
					deleteDataFiles(ORCAS_DATA, bktID, dataID, ma, c)
				}
			}
		}

		// 从数据库中删除已回收的对象元数据
		// 批量删除
		for _, objID := range objIDsToDelete {
			if err := deleteObjFromDB(c, bktID, objID); err != nil {
				// 记录错误但继续处理其他对象
				continue
			}
		}

		return nil
	}

	if len(deletedObjs) == 0 {
		return nil
	}

	// 收集所有需要检查的DataID
	dataIDs := make(map[int64]bool)
	for _, obj := range deletedObjs {
		if obj.DataID > 0 && obj.DataID != EmptyDataID {
			dataIDs[obj.DataID] = true
		}
	}

	if len(dataIDs) == 0 {
		return nil
	}

	// 转换为切片
	dataIDList := make([]int64, 0, len(dataIDs))
	for dataID := range dataIDs {
		dataIDList = append(dataIDList, dataID)
	}

	// 统计DataID的引用计数（排除已删除的对象）
	refCounts, err := ma.CountDataRefs(c, bktID, dataIDList)
	if err != nil {
		// 如果查询失败，跳过数据清理，只删除元数据
		refCounts = make(map[int64]int64)
	}

	// 删除未被引用的数据文件
	for _, dataID := range dataIDList {
		if refCounts[dataID] == 0 {
			// 计算数据文件总大小并减少实际使用量
			dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
			if dataSize > 0 {
				// 减少桶的实际使用量
				if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
					// 如果减少使用量失败，仍然删除文件（避免数据泄露）
				}
			}
			// 数据未被引用，可以安全删除文件
			deleteDataFiles(ORCAS_DATA, bktID, dataID, ma, c)
		}
	}

	// 从数据库中删除已回收的对象元数据
	// 这里可以批量删除，但为了简化，暂时一个个删除
	for _, obj := range deletedObjs {
		if err := deleteObjFromDB(c, bktID, obj.ID); err != nil {
			// 记录错误但继续处理其他对象
			continue
		}
	}

	return nil
}

// listChildrenDirectly 直接查询子对象（包括已删除的）
func listChildrenDirectly(c Ctx, bktID, pid int64, ma MetadataAdapter) ([]*ObjectInfo, error) {
	db, err := GetDB(c, bktID)
	if err != nil {
		return nil, ERR_OPEN_DB
	}
	defer db.Close()

	var children []*ObjectInfo
	// 直接查询所有子对象，不排除已删除的
	query := "SELECT * FROM obj WHERE pid = ?"
	rows, err := db.Query(query, pid)
	if err != nil {
		return nil, ERR_QUERY_DB
	}
	defer rows.Close()

	for rows.Next() {
		var obj ObjectInfo
		err = rows.Scan(&obj.ID, &obj.PID, &obj.DataID, &obj.Size, &obj.MTime, &obj.Type, &obj.Name, &obj.Extra)
		if err != nil {
			continue
		}
		children = append(children, &obj)
	}
	return children, nil
}

// deleteObjFromDB 从数据库中删除对象（物理删除）
func deleteObjFromDB(c Ctx, bktID, id int64) error {
	db, err := GetDB(c, bktID)
	if err != nil {
		return ERR_OPEN_DB
	}
	defer db.Close()

	// 使用原生SQL删除
	_, err = db.Exec("DELETE FROM obj WHERE id = ?", id)
	return err
}

// deleteDataFiles 删除数据文件
// calculateDataSize 计算某个dataID的所有分片文件的总大小
func calculateDataSize(basePath string, bktID, dataID int64) int64 {
	chunks := scanChunks(basePath, bktID, dataID, 0, DEFAULT_CHUNK_SIZE)
	var totalSize int64
	for _, size := range chunks {
		totalSize += size
	}
	return totalSize
}

// deleteDataFiles 删除数据文件
// 如果是打包数据（PkgID > 0），则不删除打包文件本身，打包文件中的区域会在碎片整理时处理
// 如果不是打包数据，则删除所有分片文件
func deleteDataFiles(basePath string, bktID, dataID int64, ma MetadataAdapter, c Ctx) {
	// 如果提供了 MetadataAdapter 和 Ctx，查询 DataInfo 判断是否是打包数据
	if ma != nil && c != nil {
		dataInfo, err := ma.GetData(c, bktID, dataID)
		if err == nil && dataInfo != nil && dataInfo.PkgID > 0 {
			// 是打包数据，不删除打包文件本身
			// 打包文件中的区域会在碎片整理（Defragment）时处理
			// 这里只删除元数据即可（由调用者负责删除元数据）
			return
		}
	}

	// 非打包数据：删除所有分片文件（sn从0开始，直到找不到文件为止）
	sn := 0
	firstFile := true
	for {
		fileName := fmt.Sprintf("%d_%d", dataID, sn)
		hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
		path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)

		if _, err := os.Stat(path); os.IsNotExist(err) {
			break // 文件不存在，说明已经删完了
		}

		// 在删除第一个文件前等待窗口时间，防止有人在访问
		if firstFile {
			time.Sleep(time.Duration(DeleteDelaySeconds) * time.Second)
			firstFile = false
		}

		// 尝试删除文件（忽略错误，可能已经被删除）
		os.Remove(path)
		sn++
	}
}

// ScrubData 审计数据完整性，检查元数据和数据文件的一致性
func ScrubData(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter) (*ScrubResult, error) {
	result := &ScrubResult{
		CorruptedData:      []int64{},
		OrphanedData:       []int64{},
		MismatchedChecksum: []int64{},
	}

	// 初始化资源控制器
	rc := NewResourceController(GetResourceControlConfig())

	// 分页大小
	pageSize := DefaultListPageSize
	offset := 0

	// 用于存储所有数据ID的映射（用于孤立文件检测）
	dataMap := make(map[int64]*DataInfo)
	totalData := int64(0)

	// 1. 分页获取所有元数据中的数据
	for {
		// 检查是否应该停止
		if rc.ShouldStop() {
			break
		}

		pageData, total, err := ma.ListAllData(c, bktID, offset, pageSize)
		if err != nil {
			return nil, err
		}

		// 第一次获取时设置总数
		if totalData == 0 {
			totalData = total
			result.TotalData = int(total)
		}

		// 2. 构建数据ID到DataInfo的映射
		for _, d := range pageData {
			dataMap[d.ID] = d
		}

		// 3. 检查每个元数据中的数据文件是否存在
		for _, dataInfo := range pageData {
			if dataInfo.ID == EmptyDataID {
				continue // 跳过空数据
			}

			// 如果是打包数据，检查打包文件
			if dataInfo.PkgID > 0 {
				// 检查打包文件是否存在
				if !dataFileExists(ORCAS_DATA, bktID, dataInfo.PkgID, 0) {
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}
			} else {
				// 检查分片数据文件（sn从0开始，直到找不到文件为止）
				// 获取桶的分片大小配置
				chunkSize := getChunkSize(c, bktID, ma)
				// 收集所有存在的分片（传入Size和chunkSize用于计算预期的最大sn）
				chunks := scanChunks(ORCAS_DATA, bktID, dataInfo.ID, dataInfo.Size, chunkSize)

				if len(chunks) == 0 {
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}

				// 检查分片是否连续（从0开始，连续递增）
				maxSN := -1
				for sn := range chunks {
					if sn > maxSN {
						maxSN = sn
					}
				}

				// 检查分片是否连续
				isContinuous := true
				for i := 0; i <= maxSN; i++ {
					if _, exists := chunks[i]; !exists {
						isContinuous = false
						break
					}
				}

				if !isContinuous {
					// 分片不连续，标记为损坏数据
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}

				// 计算实际总大小
				var actualSize int64
				for _, size := range chunks {
					actualSize += size
				}

				// 如果实际大小与元数据中的大小不匹配，标记为损坏
				if actualSize != dataInfo.Size {
					result.CorruptedData = append(result.CorruptedData, dataInfo.ID)
					continue
				}

				// 如果数据大小大于0且有校验和，验证校验和
				if dataInfo.Size > 0 && (dataInfo.Cksum > 0 || dataInfo.CRC32 > 0) {
					if !verifyChecksum(c, bktID, dataInfo, da, maxSN) {
						result.MismatchedChecksum = append(result.MismatchedChecksum, dataInfo.ID)
					}
				}
			}
		}

		// 如果返回的数据少于pageSize，说明已经是最后一页
		if len(pageData) < pageSize {
			// 最后一页处理完后等待
			rc.WaitIfNeeded(len(pageData))
			break
		}

		// 批次处理间隔和速率限制
		rc.WaitIfNeeded(len(pageData))
		offset += pageSize
	}

	// 4. 扫描文件系统中的所有数据文件，找出没有元数据引用的孤立文件
	bucketDataPath := filepath.Join(ORCAS_DATA, fmt.Sprint(bktID))
	if _, err := os.Stat(bucketDataPath); err == nil {
		// 使用队列进行层序遍历（BFS）
		queue := []string{bucketDataPath}
		seenDataIDs := make(map[int64]bool) // 用于去重
		processedDirs := 0                  // 处理的目录数，用于资源控制

		for len(queue) > 0 {
			// 检查是否应该停止
			if rc.ShouldStop() {
				break
			}

			// 从队列头部取出目录
			dir := queue[0]
			queue = queue[1:]
			processedDirs++

			// 读取目录内容
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue // 忽略无法访问的目录
			}

			filesInDir := 0
			for _, entry := range entries {
				fullPath := filepath.Join(dir, entry.Name())

				if entry.IsDir() {
					// 如果是目录，加入队列继续遍历
					queue = append(queue, fullPath)
				} else {
					// 如果是文件，处理它
					// 解析文件名：<dataID>_<sn>
					fileName := entry.Name()
					parts := strings.Split(fileName, "_")
					if len(parts) != 2 {
						continue // 格式不对，跳过
					}

					dataID, err1 := strconv.ParseInt(parts[0], 10, 64)
					_, err2 := strconv.Atoi(parts[1])
					if err1 != nil || err2 != nil {
						continue // 解析失败，跳过
					}

					// 检查是否有元数据引用（去重）
					if _, exists := dataMap[dataID]; !exists {
						// 避免重复添加相同的dataID
						if !seenDataIDs[dataID] {
							seenDataIDs[dataID] = true
							result.OrphanedData = append(result.OrphanedData, dataID)
						}
					}
					filesInDir++
				}
			}

			// 每处理一定数量的目录后，应用资源控制
			if processedDirs%100 == 0 {
				rc.WaitIfNeeded(filesInDir)
			}
		}
	}

	return result, nil
}

// ScanDirtyData 扫描脏数据（机器断电、上传失败导致的不完整数据）
// 主要检测：
// 1. 分片不完整的数据（有部分分片缺失）
// 2. 无法读取的数据文件（文件存在但读取失败）
func ScanDirtyData(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter) (*DirtyDataResult, error) {
	result := &DirtyDataResult{
		IncompleteChunks: []int64{},
		UnreadableData:   []int64{},
	}

	// 初始化资源控制器
	rc := NewResourceController(GetResourceControlConfig())

	// 分页大小
	pageSize := DefaultListPageSize
	offset := 0

	// 分页获取所有元数据中的数据
	for {
		// 检查是否应该停止
		if rc.ShouldStop() {
			break
		}

		pageData, _, err := ma.ListAllData(c, bktID, offset, pageSize)
		if err != nil {
			return nil, err
		}

		for _, dataInfo := range pageData {
			if dataInfo.ID == EmptyDataID {
				continue // 跳过空数据
			}

			// 如果是打包数据，检查打包文件是否可读
			if dataInfo.PkgID > 0 {
				if !dataFileExists(ORCAS_DATA, bktID, dataInfo.PkgID, 0) {
					result.UnreadableData = append(result.UnreadableData, dataInfo.ID)
					continue
				}
				// 尝试读取打包数据的片段
				pkgReader, _, err := createPkgDataReader(ORCAS_DATA, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
				if err != nil {
					result.UnreadableData = append(result.UnreadableData, dataInfo.ID)
					continue
				}
				pkgReader.Close()
			} else {
				// 获取桶的分片大小配置
				chunkSize := getChunkSize(c, bktID, ma)
				// 扫描所有分片（传入Size和chunkSize用于计算预期的最大sn）
				chunks := scanChunks(ORCAS_DATA, bktID, dataInfo.ID, dataInfo.Size, chunkSize)

				if len(chunks) == 0 {
					// 没有分片文件，跳过（这应该由ScrubData检测）
					continue
				}

				// 找出最大sn
				maxSN := -1
				for sn := range chunks {
					if sn > maxSN {
						maxSN = sn
					}
				}

				// 检查分片是否连续（从0到maxSN都应该存在）
				isContinuous := true
				for i := 0; i <= maxSN; i++ {
					if _, exists := chunks[i]; !exists {
						isContinuous = false
						break
					}
				}

				if !isContinuous {
					// 分片不连续，标记为不完整
					result.IncompleteChunks = append(result.IncompleteChunks, dataInfo.ID)
					continue
				}

				// 计算实际总大小
				var actualSize int64
				for _, size := range chunks {
					actualSize += size
				}

				// 如果实际大小与元数据中的大小不匹配，标记为不完整
				if actualSize != dataInfo.Size {
					result.IncompleteChunks = append(result.IncompleteChunks, dataInfo.ID)
					continue
				}

				// 尝试读取所有分片，检查是否可读
				for sn := 0; sn <= maxSN; sn++ {
					_, err := da.Read(c, bktID, dataInfo.ID, sn)
					if err != nil {
						result.UnreadableData = append(result.UnreadableData, dataInfo.ID)
						break
					}
				}
			}
		}

		// 如果返回的数据少于pageSize，说明已经是最后一页
		if len(pageData) < pageSize {
			// 最后一页处理完后等待
			rc.WaitIfNeeded(len(pageData))
			break
		}

		// 批次处理间隔和速率限制
		rc.WaitIfNeeded(len(pageData))
		offset += pageSize
	}

	return result, nil
}

// FixScrubIssues 修复 ScrubData 检测到的问题
// 根据 ScrubResult 的结果，可以选择性地修复不同类型的问题
// options:
//   - FixCorrupted: 是否修复损坏数据（删除没有文件的元数据），默认false
//   - FixOrphaned: 是否修复孤立数据（删除没有引用的文件），默认false
//   - FixMismatchedChecksum: 是否修复校验和不匹配的数据（删除损坏的数据），默认false
//
// 返回修复统计信息
func FixScrubIssues(c Ctx, bktID int64, result *ScrubResult, ma MetadataAdapter, da DataAdapter, options struct {
	FixCorrupted          bool
	FixOrphaned           bool
	FixMismatchedChecksum bool
},
) (*FixScrubIssuesResult, error) {
	fixResult := &FixScrubIssuesResult{
		FixedCorrupted:          0,
		FixedOrphaned:           0,
		FixedMismatchedChecksum: 0,
		FreedSize:               0,
		Errors:                  []string{},
	}

	// 1. 修复损坏数据（有元数据但没有文件）
	if options.FixCorrupted && len(result.CorruptedData) > 0 {
		for _, dataID := range result.CorruptedData {
			// 检查是否有对象引用这个数据
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
			if err != nil {
				fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to check refs for corrupted data %d: %v", dataID, err))
				continue
			}

			if refCounts[dataID] == 0 {
				// 没有引用，可以安全删除元数据
				// 检查是否是打包数据
				dataInfo, err := ma.GetData(c, bktID, dataID)
				if err == nil && dataInfo != nil && dataInfo.PkgID > 0 && dataInfo.PkgID != dataID {
					// 是打包数据，只删除元数据，不删除打包文件本身
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to delete corrupted data metadata %d: %v", dataID, err))
					} else {
						fixResult.FixedCorrupted++
					}
				} else {
					// 非打包数据，删除元数据
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to delete corrupted data metadata %d: %v", dataID, err))
					} else {
						fixResult.FixedCorrupted++
					}
				}
			} else {
				// 有引用，不能删除，记录警告
				fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("corrupted data %d still has %d references, cannot delete", dataID, refCounts[dataID]))
			}
		}
	}

	// 2. 修复孤立数据（有文件但没有元数据引用）
	if options.FixOrphaned && len(result.OrphanedData) > 0 {
		for _, dataID := range result.OrphanedData {
			// 孤立数据没有元数据引用，可以直接删除文件
			dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
			if dataSize > 0 {
				// 减少实际使用量
				if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
					fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to decrease real used for orphaned data %d: %v", dataID, err))
				}
				fixResult.FreedSize += dataSize
			}

			// 删除文件
			deleteDataFiles(ORCAS_DATA, bktID, dataID, nil, nil)
			fixResult.FixedOrphaned++
		}
	}

	// 3. 修复校验和不匹配的数据（数据可能已损坏）
	if options.FixMismatchedChecksum && len(result.MismatchedChecksum) > 0 {
		for _, dataID := range result.MismatchedChecksum {
			// 检查是否有对象引用这个数据
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
			if err != nil {
				fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to check refs for mismatched checksum data %d: %v", dataID, err))
				continue
			}

			if refCounts[dataID] == 0 {
				// 没有引用，可以安全删除
				// 检查是否是打包数据
				dataInfo, err := ma.GetData(c, bktID, dataID)
				if err == nil && dataInfo != nil && dataInfo.PkgID > 0 && dataInfo.PkgID != dataID {
					// 是打包数据，只删除元数据
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to delete mismatched checksum data metadata %d: %v", dataID, err))
					} else {
						fixResult.FixedMismatchedChecksum++
					}
				} else {
					// 非打包数据，删除文件和元数据
					// 先删除元数据，再删除文件，避免出现有元数据但没有文件的情况
					if err := ma.DeleteData(c, bktID, []int64{dataID}); err != nil {
						fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to delete mismatched checksum data metadata %d: %v", dataID, err))
						continue // 元数据删除失败，不继续删除文件
					}

					dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
					if dataSize > 0 {
						if err := ma.DecBktRealUsed(c, bktID, dataSize); err != nil {
							fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("failed to decrease real used for mismatched checksum data %d: %v", dataID, err))
						}
						fixResult.FreedSize += dataSize
					}
					deleteDataFiles(ORCAS_DATA, bktID, dataID, ma, c)
					fixResult.FixedMismatchedChecksum++
				}
			} else {
				// 有引用，不能删除，记录警告
				fixResult.Errors = append(fixResult.Errors, fmt.Sprintf("mismatched checksum data %d still has %d references, cannot delete", dataID, refCounts[dataID]))
			}
		}
	}

	return fixResult, nil
}

// dataFileExists 检查数据文件是否存在
func dataFileExists(basePath string, bktID, dataID int64, sn int) bool {
	fileName := fmt.Sprintf("%d_%d", dataID, sn)
	hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
	path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)
	_, err := os.Stat(path)
	return err == nil
}

// 默认分片大小
const DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024 // 4MB

// getChunkSize 获取桶的分片大小（从桶配置中获取，如果未设置则使用默认值）
func getChunkSize(c Ctx, bktID int64, ma MetadataAdapter) int64 {
	buckets, err := ma.GetBkt(c, []int64{bktID})
	if err != nil || len(buckets) == 0 {
		return DEFAULT_CHUNK_SIZE
	}
	chunkSize := buckets[0].ChunkSize
	if chunkSize <= 0 {
		return DEFAULT_CHUNK_SIZE
	}
	return chunkSize
}

// scanChunks 扫描某个dataID的所有分片文件，返回 sn -> size 的映射
// dataSize 为0表示未知大小，否则用于计算预期的最大sn（防止无限循环）
// chunkSize 为分片大小，用于计算预期的最大sn
func scanChunks(basePath string, bktID, dataID int64, dataSize int64, chunkSize int64) map[int]int64 {
	chunks := make(map[int]int64)

	// 根据数据大小计算预期的最大sn
	if dataSize > 0 && chunkSize > 0 {
		// 计算预期分片数：向上取整
		// 例如：如果 dataSize = 10MB，chunkSize = 4MB
		//   分片数 = ceil(10/4) = 3，分片索引为 0, 1, 2
		//   最大sn = 3 - 1 = 2
		expectedChunkCount := (dataSize + chunkSize - 1) / chunkSize // 向上取整
		maxAllowedSN := int(expectedChunkCount) + 1                  // 允许一些容差，防止计算误差
		if maxAllowedSN > 100000 {
			maxAllowedSN = 100000 // 设置一个绝对上限，防止异常情况
		}

		// 从0开始扫描，直到找不到文件或超过预期最大sn
		sn := 0
		for {
			fileName := fmt.Sprintf("%d_%d", dataID, sn)
			hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
			path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)

			info, err := os.Stat(path)
			if err != nil {
				// 文件不存在，停止扫描
				break
			}

			// 记录分片大小
			chunks[sn] = info.Size()
			sn++

			// 如果已知数据大小，当超过预期的最大sn时停止（允许一些容差）
			if sn > maxAllowedSN {
				break
			}
		}
	} else {
		// 如果不知道数据大小，使用原来的逻辑（直到找不到文件或超过安全上限）
		sn := 0
		for {
			fileName := fmt.Sprintf("%d_%d", dataID, sn)
			hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
			path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)

			info, err := os.Stat(path)
			if err != nil {
				// 文件不存在，停止扫描
				break
			}

			// 记录分片大小
			chunks[sn] = info.Size()
			sn++

			// 防止无限循环（设置一个安全上限）
			if sn > 100000 {
				break
			}
		}
	}

	return chunks
}

// MergeDuplicateData 合并秒传重复数据
// 查找具有相同校验值但不同DataID的重复数据，将它们合并为一个DataID
// 确保同一个bktID同一时间只有一个合并操作在执行
func MergeDuplicateData(c Ctx, bktID int64, ma MetadataAdapter, da DataAdapter) (*MergeDuplicateResult, error) {
	// 生成唯一key，确保同一个bktID同一时间只有一个合并操作
	key := fmt.Sprintf("merge_duplicate_%d", bktID)

	// 获取工作锁
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// 无法获取锁，说明已有其他goroutine在处理，返回错误
		return nil, fmt.Errorf("merge duplicate data operation already in progress for bucket %d", bktID)
	}
	// 确保在处理完成后释放锁
	defer release()

	result := &MergeDuplicateResult{
		MergedGroups: 0,
		MergedData:   []map[int64]int64{},
		FreedSize:    0,
	}

	// 初始化资源控制器
	rc := NewResourceController(GetResourceControlConfig())

	// 分页查找重复数据
	pageSize := 100
	offset := 0
	var totalFreedSize int64
	allMergedData := make(map[int64]int64) // oldDataID -> masterDataID

	for {
		// 检查是否应该停止
		if rc.ShouldStop() {
			break
		}

		groups, _, err := ma.FindDuplicateData(c, bktID, offset, pageSize)
		if err != nil {
			return nil, err
		}

		if len(groups) == 0 {
			break
		}

		// 处理每一组重复数据
		for _, group := range groups {
			if len(group.DataIDs) < 2 {
				continue // 不是重复数据
			}

			// 选择最小的ID作为主DataID（保持一致性）
			masterDataID := group.DataIDs[0]
			for _, dataID := range group.DataIDs[1:] {
				if dataID < masterDataID {
					masterDataID = dataID
				}
			}

			// 检查主数据是否存在
			masterData, err := ma.GetData(c, bktID, masterDataID)
			if err != nil || masterData == nil {
				// 主数据不存在，跳过这组
				continue
			}

			// 检查主数据的文件是否存在
			if !dataFileExists(ORCAS_DATA, bktID, masterDataID, 0) && masterData.PkgID == 0 {
				// 主数据文件不存在，尝试找另一个存在的作为主数据
				found := false
				for _, dataID := range group.DataIDs {
					if dataID == masterDataID {
						continue
					}
					data, err := ma.GetData(c, bktID, dataID)
					if err == nil && data != nil {
						if dataFileExists(ORCAS_DATA, bktID, dataID, 0) || data.PkgID > 0 {
							masterDataID = dataID
							masterData = data
							found = true
							break
						}
					}
				}
				if !found {
					// 没有找到有效的主数据，跳过
					continue
				}
			}

			// 更新所有引用重复DataID的对象，将它们指向主DataID
			duplicateDataIDs := make([]int64, 0)
			mergedMap := make(map[int64]int64)

			for _, dataID := range group.DataIDs {
				if dataID == masterDataID {
					continue // 跳过主数据
				}

				// 检查这个数据是否被引用（只统计未删除的对象）
				refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
				if err != nil {
					continue
				}

				refCount := refCounts[dataID]
				if refCount > 0 {
					// 有对象引用这个数据，需要更新引用
					if err := ma.UpdateObjDataID(c, bktID, dataID, masterDataID); err != nil {
						continue // 更新失败，跳过这个数据
					}
					mergedMap[dataID] = masterDataID
					allMergedData[dataID] = masterDataID
				}

				// 计算数据大小（用于计算释放的空间）
				dataSize := calculateDataSize(ORCAS_DATA, bktID, dataID)
				if dataSize > 0 {
					totalFreedSize += dataSize
				}

				// 如果这个数据没有被引用或已经更新完成，可以删除
				// 但是只有确认所有引用都已更新后才能删除数据文件
				// 这里先收集要删除的数据ID
				if refCount == 0 || len(mergedMap) > 0 {
					duplicateDataIDs = append(duplicateDataIDs, dataID)
				}
			}

			// 删除重复的数据文件（只有在没有引用或引用已更新后）
			// 在删除前等待窗口时间，防止有人在访问
			for _, dataID := range duplicateDataIDs {
				// 再次检查引用计数（确保没有遗漏）
				refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataID})
				if err != nil {
					continue
				}

				refCount := refCounts[dataID]
				if refCount == 0 {
					// 没有引用，使用延迟删除处理
					// delayedDelete 会在等待窗口时间后检查引用计数，如果仍为0则删除
					delayedDelete(c, bktID, dataID, ma, da)
				} else {
					// 还有引用，说明UpdateObjDataID可能失败了，不删除
					// 但是元数据可能需要保留（因为引用已更新）
				}
			}

			if len(mergedMap) > 0 {
				result.MergedGroups++
				result.MergedData = append(result.MergedData, mergedMap)
			}
		}

		// 如果返回的组数少于pageSize，说明已经是最后一页
		if len(groups) < pageSize {
			// 最后一页处理完后等待
			rc.WaitIfNeeded(len(groups))
			break
		}

		// 批次处理间隔和速率限制
		rc.WaitIfNeeded(len(groups))
		offset += pageSize
	}

	result.FreedSize = totalFreedSize
	return result, nil
}

// verifyChecksum 验证数据的校验和（使用流式计算，避免全部加载到内存）
func verifyChecksum(c Ctx, bktID int64, dataInfo *DataInfo, da DataAdapter, maxSN int) bool {
	var reader io.Reader
	var err error

	// 如果是打包数据，从打包文件中读取指定片段
	if dataInfo.PkgID > 0 {
		// 打包数据存储在 PkgID 文件中（sn=0），从 PkgOffset 位置读取 Size 大小的数据
		var pkgReader *pkgReader
		pkgReader, _, err = createPkgDataReader(ORCAS_DATA, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
		if err != nil {
			return false
		}
		defer pkgReader.Close()
		reader = pkgReader
	} else {
		// 创建分片数据读取器（流式读取所有分片）
		reader, _, err = createChunkDataReader(c, da, bktID, dataInfo.ID, maxSN)
		if err != nil {
			return false
		}
	}

	// 初始化哈希计算器
	var crc32Hash hash.Hash32
	var md5Hash hash.Hash
	needCksum := dataInfo.Cksum > 0
	needCRC32 := (dataInfo.Kind&DATA_ENDEC_MASK == 0 && dataInfo.Kind&DATA_CMPR_MASK == 0) && dataInfo.CRC32 > 0
	needMD5 := (dataInfo.Kind&DATA_ENDEC_MASK == 0 && dataInfo.Kind&DATA_CMPR_MASK == 0) && dataInfo.MD5 != 0

	if needCksum || needCRC32 {
		crc32Hash = crc32.NewIEEE()
	}
	if needMD5 {
		md5Hash = md5.New()
	}

	// 流式读取并更新哈希，同时计算总大小
	const bufferSize = 64 * 1024 // 64KB 缓冲区
	buf := make([]byte, bufferSize)
	var actualSize int64
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			actualSize += int64(n)
			if crc32Hash != nil {
				crc32Hash.Write(buf[:n])
			}
			if md5Hash != nil {
				md5Hash.Write(buf[:n])
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return false
		}
	}

	// 验证数据大小
	if actualSize != dataInfo.Size {
		return false
	}

	// 验证Cksum（最终数据的CRC32）
	if needCksum {
		calculated := crc32Hash.Sum32()
		if calculated != dataInfo.Cksum {
			return false
		}
	}

	// 如果数据是未加密未压缩的，可以验证CRC32和MD5
	if needCRC32 {
		calculated := crc32Hash.Sum32()
		if calculated != dataInfo.CRC32 {
			return false
		}
	}
	if needMD5 {
		hashSum := md5Hash.Sum(nil)
		// MD5存储为int64，取第4-12字节（索引4到11），使用大端序转换
		// 与 sdk/data.go 中的计算方式保持一致
		md5Int64 := int64(binary.BigEndian.Uint64(hashSum[4:12]))
		if md5Int64 != dataInfo.MD5 {
			return false
		}
	}

	return true
}

// createPkgDataReader 创建打包数据的流式读取器
func createPkgDataReader(basePath string, bktID, pkgID int64, offset, size int) (*pkgReader, int64, error) {
	fileName := fmt.Sprintf("%d_%d", pkgID, 0)
	hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
	path := filepath.Join(basePath, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}

	// 定位到偏移位置
	if offset > 0 {
		if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
			f.Close()
			return nil, 0, err
		}
	}

	// 创建 LimitedReader 限制读取大小，并确保文件正确关闭
	limitedReader := &pkgReader{
		Reader: io.LimitReader(f, int64(size)),
		file:   f,
	}
	return limitedReader, int64(size), nil
}

// pkgReader 包装 LimitedReader 并确保文件关闭
type pkgReader struct {
	io.Reader
	file *os.File
}

func (pr *pkgReader) Close() error {
	if pr.file != nil {
		return pr.file.Close()
	}
	return nil
}

// createChunkDataReader 创建分片数据的流式读取器
func createChunkDataReader(c Ctx, da DataAdapter, bktID, dataID int64, maxSN int) (io.Reader, int64, error) {
	return &chunkReader{
		c:      c,
		da:     da,
		bktID:  bktID,
		dataID: dataID,
		maxSN:  maxSN,
		sn:     0,
	}, 0, nil
}

type chunkReader struct {
	c      Ctx
	da     DataAdapter
	bktID  int64
	dataID int64
	maxSN  int
	sn     int
	buf    []byte
	bufIdx int
}

func (cr *chunkReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 如果当前缓冲区已读完，读取下一个分片
	if cr.bufIdx >= len(cr.buf) {
		if cr.sn > cr.maxSN {
			return 0, io.EOF
		}
		var err error
		cr.buf, err = cr.da.Read(cr.c, cr.bktID, cr.dataID, cr.sn)
		if err != nil {
			return 0, err
		}
		cr.bufIdx = 0
		cr.sn++
	}

	// 从缓冲区复制数据到输出
	n = copy(p, cr.buf[cr.bufIdx:])
	cr.bufIdx += n
	return n, nil
}

// Defragment 碎片整理：小文件离线归并打包，打包中被删除的数据块前移
// maxSize: 最大文件大小（小于此大小的文件会被打包）
// accessWindow: 访问窗口时间（秒），预留参数。目前只基于引用计数判断数据是否正在使用，
// 如果数据有引用（refCount > 0），则跳过。将来如果添加了访问时间字段，可以使用此参数。
// 确保同一个bktID同一时间只有一个碎片整理操作在执行
func Defragment(c Ctx, bktID int64, h Handler, ma MetadataAdapter, da DataAdapter, maxSize int64, accessWindow int64) (*DefragmentResult, error) {
	// 生成唯一key，确保同一个bktID同一时间只有一个碎片整理操作
	key := fmt.Sprintf("defragment_%d", bktID)

	// 获取工作锁
	acquired, release := acquireWorkLock(key)
	if !acquired {
		// 无法获取锁，说明已有其他goroutine在处理，返回错误
		return nil, fmt.Errorf("defragment operation already in progress for bucket %d", bktID)
	}
	// 确保在处理完成后释放锁
	defer release()

	result := &DefragmentResult{
		PackedGroups:  0,
		PackedFiles:   0,
		CompactedPkgs: 0,
		FreedSize:     0,
		SkippedInUse:  0,
	}

	// 初始化资源控制器
	rc := NewResourceController(GetResourceControlConfig())

	// 获取桶配置
	buckets, err := ma.GetBkt(c, []int64{bktID})
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, ERR_QUERY_DB
	}
	chunkSize := getChunkSize(c, bktID, ma)
	if chunkSize <= 0 {
		chunkSize = DEFAULT_CHUNK_SIZE
	}

	// 1. 查找可以打包的小文件数据（分页处理）
	// 注意：由于系统中没有访问时间（ATime）字段，只有MTime（修改时间），
	// 而MTime和访问时间不是同一个概念，所以这里只基于引用计数来判断数据是否正在使用
	pageSize := 500
	offset := 0
	pendingPacking := make([]*DataInfo, 0)
	batchDataIDs := make([]int64, 0, pageSize) // 批量收集DataID用于查询引用计数

	for {
		// 检查是否应该停止
		if rc.ShouldStop() {
			break
		}

		dataList, _, err := ma.FindSmallPackageData(c, bktID, maxSize, offset, pageSize)
		if err != nil {
			return nil, err
		}

		if len(dataList) == 0 {
			break
		}

		// 收集当前页的DataID
		batchDataIDs = batchDataIDs[:0]
		for _, dataInfo := range dataList {
			batchDataIDs = append(batchDataIDs, dataInfo.ID)
		}

		// 批量查询引用计数
		refCounts, err := ma.CountDataRefs(c, bktID, batchDataIDs)
		if err != nil {
			refCounts = make(map[int64]int64)
		}

		for _, dataInfo := range dataList {
			refCount := refCounts[dataInfo.ID]

			if refCount == 0 {
				// 未被引用，这是孤儿数据，应该删除而不是打包
				// 使用延迟删除处理，等待窗口时间后再删除
				delayedDelete(c, bktID, dataInfo.ID, ma, da)
			} else {
				// 被引用，说明数据正在使用中，可以考虑打包
				// 但需要检查访问窗口（目前使用引用计数作为判断）
				// 如果将来添加了访问时间字段，可以使用accessWindow参数
				pendingPacking = append(pendingPacking, dataInfo)
			}
		}

		if len(dataList) < pageSize {
			// 最后一页处理完后等待
			rc.WaitIfNeeded(len(dataList))
			break
		}

		// 批次处理间隔和速率限制
		rc.WaitIfNeeded(len(dataList))
		offset += pageSize
	}

	// 2. 打包小文件（按chunkSize分组）
	if len(pendingPacking) > 0 {
		// 按大小排序，尽量填满每个包
		// 简单的贪心算法：从小到大排序，尽量填满chunkSize
		pkgGroups := make([][]*DataInfo, 0)
		currentGroup := make([]*DataInfo, 0)
		currentGroupSize := int64(0)

		for _, dataInfo := range pendingPacking {
			// 检查是否超过chunkSize
			if currentGroupSize+dataInfo.Size > chunkSize && len(currentGroup) > 0 {
				// 当前组已满，创建新组
				pkgGroups = append(pkgGroups, currentGroup)
				currentGroup = make([]*DataInfo, 0)
				currentGroupSize = 0
			}
			currentGroup = append(currentGroup, dataInfo)
			currentGroupSize += dataInfo.Size
		}

		if len(currentGroup) > 0 {
			pkgGroups = append(pkgGroups, currentGroup)
		}

		// 3. 执行打包：读取每个数据，写入新打包文件
		for _, group := range pkgGroups {
			if len(group) == 0 {
				continue
			}

			// 创建新的打包数据ID
			pkgID := h.NewID()
			if pkgID <= 0 {
				continue
			}

			// 读取所有数据并合并
			pkgBuffer := make([]byte, 0, chunkSize)
			dataInfos := make([]*DataInfo, 0, len(group))

			for _, dataInfo := range group {
				// 读取数据
				var dataBytes []byte
				if dataInfo.PkgID > 0 {
					// 如果已经是打包数据，从打包文件中读取
					pkgReader, _, err := createPkgDataReader(ORCAS_DATA, bktID, dataInfo.PkgID, int(dataInfo.PkgOffset), int(dataInfo.Size))
					if err != nil {
						continue // 读取失败，跳过
					}
					dataBytes = make([]byte, dataInfo.Size)
					_, err = io.ReadFull(pkgReader, dataBytes)
					pkgReader.Close()
					if err != nil {
						continue
					}
				} else {
					// 读取分片数据
					maxSN := int((dataInfo.Size + chunkSize - 1) / chunkSize)
					reader, _, err := createChunkDataReader(c, da, bktID, dataInfo.ID, maxSN)
					if err != nil {
						continue
					}
					dataBytes = make([]byte, dataInfo.Size)
					_, err = io.ReadFull(reader, dataBytes)
					if err != nil {
						continue
					}
				}

				// 记录偏移位置
				offset := len(pkgBuffer)
				pkgBuffer = append(pkgBuffer, dataBytes...)

				// 更新数据信息
				newDataInfo := *dataInfo
				newDataInfo.PkgID = pkgID
				newDataInfo.PkgOffset = uint32(offset)
				dataInfos = append(dataInfos, &newDataInfo)
			}

			if len(dataInfos) == 0 || len(pkgBuffer) == 0 {
				continue
			}

			// 写入新的打包文件
			err := da.Write(c, bktID, pkgID, 0, pkgBuffer)
			if err != nil {
				continue // 写入失败，跳过
			}

			// 更新元数据
			err = ma.PutData(c, bktID, dataInfos)
			if err != nil {
				continue
			}

			// 计算释放的空间（旧数据文件大小）
			var freedSize int64
			for _, dataInfo := range group {
				if dataInfo.PkgID > 0 {
					// 如果原来是打包数据，需要检查是否还有其他数据引用同一个打包文件
					// 简化处理：只统计非打包数据的大小
				} else {
					// 计算分片数据的总大小
					oldSize := calculateDataSize(ORCAS_DATA, bktID, dataInfo.ID)
					freedSize += oldSize - dataInfo.Size // 减去新打包数据的大小
					// 删除旧数据文件
					deleteDataFiles(ORCAS_DATA, bktID, dataInfo.ID, ma, c)
				}
			}

			// 更新实际使用量
			if freedSize > 0 {
				ma.DecBktRealUsed(c, bktID, freedSize)
				result.FreedSize += freedSize
			}

			result.PackedGroups++
			result.PackedFiles += int64(len(dataInfos))
		}
	}

	// 4. 整理已有打包文件：删除打包中被删除的数据块，前移剩余数据
	// 查找所有打包文件（PkgID > 0）
	// 这里需要遍历所有数据，找出打包文件ID列表
	pkgDataMap := make(map[int64][]*DataInfo) // pkgID -> []DataInfo

	offset = 0
	for {
		// 检查是否应该停止
		if rc.ShouldStop() {
			break
		}

		dataList, _, err := ma.ListAllData(c, bktID, offset, pageSize)
		if err != nil {
			break
		}

		for _, dataInfo := range dataList {
			if dataInfo.PkgID > 0 {
				pkgDataMap[dataInfo.PkgID] = append(pkgDataMap[dataInfo.PkgID], dataInfo)
			}
		}

		if len(dataList) < pageSize {
			// 最后一页处理完后等待
			rc.WaitIfNeeded(len(dataList))
			break
		}

		// 批次处理间隔和速率限制
		rc.WaitIfNeeded(len(dataList))
		offset += pageSize
	}

	// 整理每个打包文件
	for pkgID, dataList := range pkgDataMap {
		// 读取整个打包文件
		// 使用与core/data.go中相同的路径计算方式
		fileName := fmt.Sprintf("%d_%d", pkgID, 0)
		hash := fmt.Sprintf("%X", md5.Sum([]byte(fileName)))
		pkgPath := filepath.Join(ORCAS_DATA, fmt.Sprint(bktID), hash[21:24], hash[8:24], fileName)
		pkgFile, err := os.Open(pkgPath)
		if err != nil {
			continue
		}

		// 读取文件大小
		fileInfo, err := pkgFile.Stat()
		if err != nil {
			pkgFile.Close()
			continue
		}
		fileSize := fileInfo.Size()
		pkgData := make([]byte, fileSize)
		_, err = io.ReadFull(pkgFile, pkgData)
		pkgFile.Close()
		if err != nil {
			continue
		}

		// 检查每个数据是否仍被引用
		validData := make([]*DataInfo, 0)
		validOffsets := make([]int, 0) // 有效的偏移位置列表

		for _, dataInfo := range dataList {
			// 检查DataInfo的ID是否为负数（标记为已删除）
			if dataInfo.ID < 0 {
				// DataInfo的ID为负数，说明已标记删除
				// 查询引用这个DataInfo的对象，获取删除时间（MTime）
				objs, err := ma.GetObjByDataID(c, bktID, dataInfo.ID) // 使用负数ID查找对象
				if err == nil && len(objs) > 0 {
					// 使用第一个对象的MTime作为删除时间的参考
					deleteTime := objs[0].MTime
					if deleteTime > 0 {
						// 检查删除时间是否超过窗口时间
						now := time.Now().Unix()
						if now-deleteTime >= accessWindow {
							// 超过窗口时间，可以删除这个数据块
							// 不添加到validData中，后续会被清理
							continue
						} else {
							// 未超过窗口时间，暂时保留（可能被恢复）
							validData = append(validData, dataInfo)
							validOffsets = append(validOffsets, int(dataInfo.PkgOffset))
							continue
						}
					}
				}
				// 如果无法获取删除时间，默认不保留
				continue
			}

			// DataInfo的ID为正数，检查是否仍被引用
			refCounts, err := ma.CountDataRefs(c, bktID, []int64{dataInfo.ID})
			if err != nil {
				continue
			}

			if refCounts[dataInfo.ID] > 0 {
				// 仍被正常引用，保留
				validData = append(validData, dataInfo)
				validOffsets = append(validOffsets, int(dataInfo.PkgOffset))
			}
			// 如果没有引用且ID为正数，可能是孤儿数据，不添加到validData
		}

		if len(validData) == 0 {
			// 打包文件中没有有效数据，可以删除整个打包文件
			// 先删除所有相关数据的元数据，再删除文件，避免出现有元数据但没有文件的情况
			dataIDs := make([]int64, 0, len(dataList))
			for _, di := range dataList {
				dataIDs = append(dataIDs, di.ID)
			}
			if len(dataIDs) > 0 {
				if err := ma.DeleteData(c, bktID, dataIDs); err != nil {
					// 元数据删除失败，不继续删除文件
					continue
				}
			}
			// 这里传入 nil 表示不是打包数据，而是打包文件本身，需要删除整个文件
			deleteDataFiles(ORCAS_DATA, bktID, pkgID, nil, nil)
			result.CompactedPkgs++
			continue
		}

		// 重新打包有效数据（前移）
		newPkgBuffer := make([]byte, 0, chunkSize)
		newDataInfos := make([]*DataInfo, 0, len(validData))

		// 预先计算每个数据在新打包文件中的偏移量
		newOffsets := make([]int, len(validData))
		currentOffset := 0
		for i, dataInfo := range validData {
			newOffsets[i] = currentOffset
			currentOffset += int(dataInfo.Size)
		}

		for i, dataInfo := range validData {
			oldOffset := validOffsets[i]
			dataBytes := pkgData[oldOffset : oldOffset+int(dataInfo.Size)]

			newOffset := newOffsets[i]
			if len(newPkgBuffer) < newOffset+int(dataInfo.Size) {
				// 确保缓冲区足够大
				if cap(newPkgBuffer) < newOffset+int(dataInfo.Size) {
					oldBuf := newPkgBuffer
					newPkgBuffer = make([]byte, newOffset+int(dataInfo.Size))
					copy(newPkgBuffer, oldBuf)
				} else {
					newPkgBuffer = newPkgBuffer[:newOffset+int(dataInfo.Size)]
				}
			}
			copy(newPkgBuffer[newOffset:], dataBytes)

			newDataInfo := *dataInfo
			newDataInfo.PkgOffset = uint32(newOffset)
			newDataInfos = append(newDataInfos, &newDataInfo)
		}

		// 如果新打包文件更小，更新
		if int64(len(newPkgBuffer)) < fileSize {
			// 使用新pkgID方案：创建新的打包文件，更新所有引用，旧数据放入等待队列
			// 1. 生成新的pkgID（作为新的DataID）
			newPkgID := h.NewID()
			if newPkgID <= 0 {
				continue
			}

			// 2. 创建新的打包文件
			err := da.Write(c, bktID, newPkgID, 0, newPkgBuffer)
			if err != nil {
				continue // 写入失败，跳过
			}

			// 3. 收集所有需要更新的对象（引用旧DataInfo的对象）
			oldDataIDs := make([]int64, 0, len(validData))

			for i, dataInfo := range validData {
				oldDataID := dataInfo.ID
				// 创建新的DataInfo，使用新的pkgID
				newDataID := h.NewID()
				if newDataID <= 0 {
					continue
				}

				// 使用预先计算的偏移量
				newOffset := newOffsets[i]

				newDataInfo := *dataInfo
				newDataInfo.ID = newDataID
				newDataInfo.PkgID = newPkgID
				newDataInfo.PkgOffset = uint32(newOffset)

				// 记录旧的DataID
				oldDataIDs = append(oldDataIDs, oldDataID)

				// 更新新数据的元数据
				ma.PutData(c, bktID, []*DataInfo{&newDataInfo})

				// 更新所有引用旧DataID的对象的DataID为新DataID
				ma.UpdateObjDataID(c, bktID, oldDataID, newDataID)
			}

			// 4. 使用延迟删除处理旧pkgID和旧DataID
			// 将旧的pkgID放入延迟删除
			delayedDelete(c, bktID, pkgID, ma, da)

			// 将旧的DataID放入延迟删除
			for _, oldDataID := range oldDataIDs {
				delayedDelete(c, bktID, oldDataID, ma, da)
			}

			// 5. 计算释放的空间（暂时不减少，等待延迟时间后再减少）
			// 因为旧文件还在，只是不再被引用
			freedSize := fileSize - int64(len(newPkgBuffer))
			if freedSize > 0 {
				result.FreedSize += freedSize
			}
			result.CompactedPkgs++
		}
	}

	return result, nil
}
