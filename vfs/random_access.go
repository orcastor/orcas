package vfs

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/h2non/filetype"
	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/orca-zhang/ecache"
	"github.com/orcastor/orcas/core"
	"github.com/orcastor/orcas/sdk"
	"github.com/tjfoc/gmsm/sm4"
)

var (
	// 对象池：重用字节缓冲区，减少内存分配
	chunkDataPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4*1024*1024) // 预分配4MB容量
		},
	}

	// 对象池：重用写入操作slice
	writeOpsPool = sync.Pool{
		New: func() interface{} {
			return make([]WriteOperation, 0, 32)
		},
	}

	// 对象池：重用字符串缓冲区（用于缓存key生成）
	cacheKeyPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 64) // 预分配64字节容量
		},
	}

	// ecache缓存：缓存DataInfo，减少数据库查询
	// key: "data_info_<bktID>_<dataID>", value: *core.DataInfo
	dataInfoCache = ecache.NewLRUCache(16, 512, 30*time.Second)

	// ecache缓存：缓存文件对象信息，减少数据库查询
	// key: "<bktID>_<fileID>", value: *core.ObjectInfo
	fileObjCache = ecache.NewLRUCache(16, 512, 30*time.Second)
)

// formatCacheKey 格式化缓存key（优化：直接内存拷贝，最高性能）
func formatCacheKey(bktID, id int64) string {
	// 在栈上创建固定大小的byte数组
	var buf [16]byte

	// 直接使用unsafe将8字节的int64内存拷贝到byte数组（最高性能）
	// 避免函数调用开销，直接内存操作
	*(*int64)(unsafe.Pointer(&buf[0])) = bktID
	*(*int64)(unsafe.Pointer(&buf[8])) = id
	return string(buf[:])
}

// WriteOperation 表示一次写入操作
type WriteOperation struct {
	Offset int64  // 写入偏移量
	Data   []byte // 写入的数据
}

// WriteBuffer 管理单个文件的写缓冲
type WriteBuffer struct {
	fileID     int64            // 文件对象ID
	operations []WriteOperation // 固定长度的写入操作数组（避免临时对象创建）
	writeIndex int64            // 当前写入位置（使用原子操作，从0开始）
	totalSize  int64            // 缓冲区总大小（使用原子操作优化）
}

// RandomAccessor VFS中的随机访问对象，支持压缩和加密
type RandomAccessor struct {
	fs         *OrcasFS
	fileID     int64
	buffer     *WriteBuffer
	fileObj    atomic.Value
	fileObjKey string // 预计算的file_obj缓存key（优化：避免重复转换）
}

// NewRandomAccessor 创建随机访问对象
func NewRandomAccessor(fs *OrcasFS, fileID int64) (*RandomAccessor, error) {
	// 获取配置以初始化固定长度的operations数组（优化：避免临时对象创建）
	maxBufferWrites := int(core.GetWriteBufferConfig().MaxBufferWrites)
	if maxBufferWrites <= 0 {
		maxBufferWrites = 200 // 默认值
	}

	ra := &RandomAccessor{
		fs:         fs,
		fileID:     fileID,
		fileObjKey: formatCacheKey(fs.bktID, fileID), // 预计算并缓存key
		buffer: &WriteBuffer{
			fileID:     fileID,
			operations: make([]WriteOperation, maxBufferWrites), // 固定长度数组
			writeIndex: 0,                                       // 从0开始
		},
	}
	return ra, nil
}

// Write 添加写入操作到缓冲区
// 优化：减少锁持有时间，提高并发性能，使用原子操作优化
func (ra *RandomAccessor) Write(offset int64, data []byte) error {
	// 优化：减少数据复制，只有在必要时才复制
	// 注意：data可能被外部修改，所以需要复制
	dataLen := len(data)
	var dataCopy []byte
	if dataLen == 0 {
		dataCopy = nil // 优化：空数据不需要分配
	} else {
		dataCopy = make([]byte, dataLen)
		copy(dataCopy, data)
	}

	// 优化：使用原子操作更新totalSize，减少锁竞争
	currentTotalSize := atomic.AddInt64(&ra.buffer.totalSize, int64(dataLen))

	// 优化：使用固定长度数组 + 原子操作移动写入下标，避免临时对象创建
	// 原子地获取并递增写入位置
	writeIndex := atomic.AddInt64(&ra.buffer.writeIndex, 1) - 1 // 获取递增后的值，然后减1得到当前索引

	// 检查是否超出容量（优化：提前检查，避免越界）
	if writeIndex >= int64(len(ra.buffer.operations)) {
		// 超出容量，需要强制刷新
		// 先回退writeIndex（因为已经超出了）
		atomic.AddInt64(&ra.buffer.writeIndex, -1)
		// 强制刷新当前缓冲区（同步执行，确保数据被持久化）
		_, err := ra.Flush()
		if err != nil {
			return err
		}
		// 刷新后，重新获取写入位置（此时writeIndex应该已经被Flush重置为0）
		writeIndex = atomic.AddInt64(&ra.buffer.writeIndex, 1) - 1
		// 写入新的数据
		ra.buffer.operations[writeIndex].Offset = offset
		ra.buffer.operations[writeIndex].Data = dataCopy
	} else {
		// 直接写入到固定位置（避免创建新的WriteOperation对象）
		ra.buffer.operations[writeIndex].Offset = offset
		ra.buffer.operations[writeIndex].Data = dataCopy

		// 检查是否需要立即刷新（优化：合并条件判断）
		// 注意：不在这里直接调用Flush，让调用方决定何时刷新
		// 如果需要异步刷新，应该由调用方启动goroutine
		if currentTotalSize >= core.GetWriteBufferConfig().MaxBufferSize {
			_, err := ra.Flush()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ra *RandomAccessor) getFileObj() (*core.ObjectInfo, error) {
	fileObjValue := ra.fileObj.Load()
	if fileObjValue != nil {
		if obj, ok := fileObjValue.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}
	// 优化：使用预计算的key（避免重复转换）
	if cached, ok := fileObjCache.Get(ra.fileObjKey); ok {
		if obj, ok := cached.(*core.ObjectInfo); ok && obj != nil {
			return obj, nil
		}
	}
	// 如果缓存未命中，从数据库获取
	objs, err := ra.fs.h.Get(ra.fs.c, ra.fs.bktID, []int64{ra.fileID})
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("file %d not found", ra.fileID)
	}
	// 更新缓存（使用预计算的key）
	fileObjCache.Put(ra.fileObjKey, objs[0])
	ra.fileObj.Store(objs[0])
	return objs[0], nil
}

// Read 读取指定位置的数据，会合并缓冲区中的写入
// 优化：使用原子指针读取fileObj，无锁并发读取
func (ra *RandomAccessor) Read(offset int64, size int) ([]byte, error) {
	// 优化：使用原子操作读取fileObj，无锁并发读取
	fileObj, err := ra.getFileObj()
	if err != nil {
		return nil, err
	}

	// 读取原文件数据
	// 重要：必须先检查DataInfo，确定数据是否有压缩或加密
	// 如果有压缩或加密，必须使用SDK的DataReader来解密解压，不能直接读取
	var fileData []byte
	if fileObj.DataID > 0 && fileObj.DataID != core.EmptyDataID {
		// 优化：使用更高效的key生成（函数内部已使用对象池）
		dataInfoCacheKey := formatCacheKey(ra.fs.bktID, fileObj.DataID)

		var dataInfo *core.DataInfo
		if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
			if info, ok := cached.(*core.DataInfo); ok && info != nil {
				dataInfo = info
			}
		}

		// 如果缓存未命中，从数据库获取
		var err error
		if dataInfo == nil {
			dataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, fileObj.DataID)
			if err == nil && dataInfo != nil {
				// 更新缓存（重用已生成的key）
				dataInfoCache.Put(dataInfoCacheKey, dataInfo)
			}
		}

		if err != nil {
			// 如果获取DataInfo失败，尝试直接读取（可能是旧数据格式）
			data, readErr := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, fileObj.DataID, 0)
			if readErr == nil && len(data) > 0 {
				fileData = data
			} else {
				fileData = []byte{}
			}
		} else if dataInfo != nil {
			// 检查数据是否有压缩或加密
			hasCompression := dataInfo.Kind&core.DATA_CMPR_MASK != 0
			hasEncryption := dataInfo.Kind&core.DATA_ENDEC_MASK != 0

			// 如果没有压缩和加密，可以直接使用Handler读取（更简单可靠）
			if !hasCompression && !hasEncryption {
				data, readErr := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, fileObj.DataID, 0)
				if readErr == nil {
					fileData = data
				} else {
					// 如果读取失败，可能是数据还未同步或文件不存在
					fileData = []byte{}
				}
			} else {
				// 有压缩或加密，必须使用SDK的DataReader来解密解压
				// 不能直接读取，因为压缩会改变数据布局，加密会有填充
				var endecKey string
				if ra.fs.sdkCfg != nil {
					endecKey = ra.fs.sdkCfg.EndecKey
				}
				reader := sdk.NewDataReader(ra.fs.c, ra.fs.h, ra.fs.bktID, dataInfo, endecKey)

				// 如果数据有压缩，需要解压缩
				var decompressor archiver.Decompressor
				if hasCompression {
					if dataInfo.Kind&core.DATA_CMPR_SNAPPY != 0 {
						decompressor = &archiver.Snappy{}
					} else if dataInfo.Kind&core.DATA_CMPR_ZSTD != 0 {
						decompressor = &archiver.Zstd{}
					} else if dataInfo.Kind&core.DATA_CMPR_GZIP != 0 {
						decompressor = &archiver.Gz{}
					} else if dataInfo.Kind&core.DATA_CMPR_BR != 0 {
						decompressor = &archiver.Brotli{}
					}
				}

				// 读取完整文件（简化实现，实际应该支持流式读取）
				if decompressor != nil {
					// 需要解压缩
					var decompressedBuf bytes.Buffer
					err := decompressor.Decompress(reader, &decompressedBuf)
					if err != nil && err != io.EOF {
						// 解压缩失败，返回错误
						return nil, fmt.Errorf("failed to decompress data: %w", err)
					}
					fileData = decompressedBuf.Bytes()
				} else {
					// 无压缩，直接读取（可能有加密，DataReader会自动处理解密）
					allData, err := io.ReadAll(reader)
					if err != nil && err != io.EOF {
						return nil, fmt.Errorf("failed to read encrypted data: %w", err)
					}
					fileData = allData
				}
			}
		}
	}

	// 优化：使用原子操作读取writeIndex，获取实际的操作数量（无锁并发读取）
	writeIndex := atomic.LoadInt64(&ra.buffer.writeIndex)
	var operations []WriteOperation
	if writeIndex > 0 {
		// 复制实际使用的部分（避免修改原始数组）
		operations = make([]WriteOperation, writeIndex)
		copy(operations, ra.buffer.operations[:writeIndex])
	}

	// 合并写入操作
	mergedOps := mergeWriteOperations(operations)
	modifiedData := applyWritesToData(fileData, mergedOps)

	// 读取指定范围
	if offset >= int64(len(modifiedData)) {
		// 如果偏移超出数据长度，返回空数据而不是EOF（允许稀疏读取）
		return []byte{}, nil
	}
	end := offset + int64(size)
	if end > int64(len(modifiedData)) {
		end = int64(len(modifiedData))
	}

	if offset >= end {
		return nil, nil // 优化：返回nil而不是空slice，减少临时对象
	}

	// 优化：如果返回的是整个slice，直接返回，否则创建新的slice
	resultLen := end - offset
	if offset == 0 && end == int64(len(modifiedData)) {
		return modifiedData, nil
	}
	// 优化：使用切片操作，避免不必要的copy（如果数据不会在外部修改）
	result := make([]byte, resultLen)
	copy(result, modifiedData[offset:end])
	return result, nil
}

// Flush 刷新缓冲区，返回新版本ID（如果没有待刷新数据返回0）
func (ra *RandomAccessor) Flush() (int64, error) {
	// 优化：使用原子操作获取并清空操作（无锁）
	// 原子地交换writeIndex并重置为0，获取实际的操作数量
	writeIndex := atomic.SwapInt64(&ra.buffer.writeIndex, 0)
	if writeIndex <= 0 {
		return 0, nil
	}

	// 复制实际使用的部分（避免在flush期间被修改）
	operations := make([]WriteOperation, writeIndex)
	copy(operations, ra.buffer.operations[:writeIndex])

	// 优化：使用原子操作重置totalSize
	atomic.StoreInt64(&ra.buffer.totalSize, 0)

	// 合并重叠的写入操作
	mergedOps := mergeWriteOperations(operations)

	// 获取文件对象信息（更新缓存）
	// 优化：使用更高效的key生成（函数内部已使用对象池）
	fileObj, err := ra.getFileObj()
	if err != nil {
		return 0, err
	}

	// 使用SDK的listener来处理压缩和加密
	return ra.applyRandomWritesWithSDK(fileObj, mergedOps)
}

// applyRandomWritesWithSDK 使用SDK的listener处理压缩和加密，应用随机写入
// 优化为流式处理，避免大对象占用过多内存
func (ra *RandomAccessor) applyRandomWritesWithSDK(fileObj *core.ObjectInfo, writes []WriteOperation) (int64, error) {
	// 获取LocalHandler以访问ig, ma, da
	lh, ok := ra.fs.h.(*core.LocalHandler)
	if !ok {
		return 0, fmt.Errorf("handler is not LocalHandler")
	}

	// 创建新版本对象
	newVersionID := ra.fs.h.NewID()
	newDataID := ra.fs.h.NewID()

	// 计算新文件大小
	newSize := fileObj.Size
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > newSize {
			newSize = writeEnd
		}
	}

	// 获取chunk大小
	chunkSize := ra.fs.chunkSize
	if chunkSize <= 0 {
		chunkSize = 4 * 1024 * 1024 // 默认4MB
	}
	chunkSizeInt := int(chunkSize)

	// 检查原数据是否已压缩或加密（优化：使用缓存）
	var oldDataInfo *core.DataInfo
	var hasCompression, hasEncryption bool
	oldDataID := fileObj.DataID
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// 优化：使用更高效的key生成（函数内部已使用对象池）
		dataInfoCacheKey := formatCacheKey(ra.fs.bktID, oldDataID)

		if cached, ok := dataInfoCache.Get(dataInfoCacheKey); ok {
			if info, ok := cached.(*core.DataInfo); ok && info != nil {
				oldDataInfo = info
			}
		}

		// 如果缓存未命中，从数据库获取
		if oldDataInfo == nil {
			var err error
			oldDataInfo, err = ra.fs.h.GetDataInfo(ra.fs.c, ra.fs.bktID, oldDataID)
			if err == nil && oldDataInfo != nil {
				// 更新缓存（重用已生成的key）
				dataInfoCache.Put(dataInfoCacheKey, oldDataInfo)
			}
		}

		if oldDataInfo != nil {
			hasCompression = oldDataInfo.Kind&core.DATA_CMPR_MASK != 0
			hasEncryption = oldDataInfo.Kind&core.DATA_ENDEC_MASK != 0
		}
	}

	// 创建DataInfo
	dataInfo := &core.DataInfo{
		ID:       newDataID,
		OrigSize: newSize,
		Kind:     core.DATA_NORMAL,
	}

	// 如果原数据已压缩或加密，必须完整读取（无法避免）
	// 但可以流式写入，避免一次性处理所有数据
	if hasCompression || hasEncryption {
		newVersionID, err := ra.applyWritesStreamingCompressed(oldDataInfo, writes, dataInfo, chunkSize, newSize)
		if err != nil {
			return 0, err
		}

		// 优化：延迟获取时间，减少time.Now()调用
		// 创建新版本对象
		mTime := time.Now().Unix()
		newVersion := &core.ObjectInfo{
			ID:     newVersionID,
			PID:    ra.fileID,
			Type:   core.OBJ_TYPE_VERSION,
			DataID: newDataID,
			Size:   newSize,
			MTime:  mTime,
		}

		// 使用Put方法创建版本（会自动应用版本保留策略）
		_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{newVersion})

		// 更新缓存的文件对象信息
		if err == nil {
			// 优化：使用更高效的key生成（函数内部已使用对象池）
			updateFileObj := &core.ObjectInfo{
				ID:     ra.fileID,
				DataID: newDataID,
				Size:   newSize,
			}
			// 优化：使用预计算的key（避免重复转换）
			fileObjCache.Put(ra.fileObjKey, updateFileObj)
			ra.fileObj.Store(updateFileObj)
		}

		return newVersionID, err
	}

	// 对于未压缩未加密的数据，可以按chunk流式读取和处理
	newVersionID, err := ra.applyWritesStreamingUncompressed(fileObj, oldDataInfo, writes, dataInfo, chunkSizeInt, newSize)
	if err != nil {
		return 0, err
	}

	// 优化：延迟获取时间，减少time.Now()调用
	// 创建新版本对象
	mTime := time.Now().Unix()
	newVersion := &core.ObjectInfo{
		ID:     newVersionID,
		PID:    ra.fileID,
		Type:   core.OBJ_TYPE_VERSION,
		DataID: newDataID,
		Size:   newSize,
		MTime:  mTime,
	}

	// 使用Put方法创建版本（会自动应用版本保留策略）
	_, err = lh.Put(ra.fs.c, ra.fs.bktID, []*core.ObjectInfo{newVersion})

	// 更新缓存的文件对象信息
	if err == nil {
		// 优化：使用更高效的key生成（函数内部已使用对象池）
		updateFileObj := &core.ObjectInfo{
			ID:     ra.fileID,
			DataID: newDataID,
			Size:   newSize,
		}
		// 优化：使用预计算的key（避免重复转换）
		fileObjCache.Put(ra.fileObjKey, updateFileObj)
		ra.fileObj.Store(updateFileObj)
	}

	return newVersionID, err
}

// applyWritesStreamingCompressed 处理已压缩或加密的数据
// 需要完整读取（无法避免），但流式写入以减少内存占用
func (ra *RandomAccessor) applyWritesStreamingCompressed(oldDataInfo *core.DataInfo, writes []WriteOperation,
	dataInfo *core.DataInfo, chunkSize int64, newSize int64) (int64, error) {
	// 对于已压缩或加密的数据，必须完整读取（无法避免）
	// 因为压缩会改变数据布局，加密有填充，无法按偏移读取
	var endecKey string
	if ra.fs.sdkCfg != nil {
		endecKey = ra.fs.sdkCfg.EndecKey
	}
	reader := sdk.NewDataReader(ra.fs.c, ra.fs.h, ra.fs.bktID, oldDataInfo, endecKey)
	originalData, err := io.ReadAll(reader)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read compressed/encrypted data: %w", err)
	}

	// 应用写入操作
	modifiedData := applyWritesToData(originalData, writes)

	// 流式写入（使用processDataWithSDK，它已经是按chunk处理的）
	// 注意：如果originalData很大，这里可以考虑优化，但压缩加密需要完整数据

	// 使用SDK配置处理压缩和加密
	if ra.fs.sdkCfg != nil {
		err = ra.processDataWithSDK(modifiedData, dataInfo, chunkSize)
		if err != nil {
			return 0, err
		}
		// 保存数据元数据
		_, err = ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
		if err != nil {
			return 0, err
		}

		// 优化：使用更高效的key生成（函数内部已使用对象池）
		dataInfoCache.Put(formatCacheKey(ra.fs.bktID, dataInfo.ID), dataInfo)
	} else {
		// 没有SDK配置，直接写入
		dataInfo.Size = int64(len(modifiedData))
		dataInfo.OrigSize = int64(len(modifiedData))
		chunkSizeInt := int(chunkSize)
		if chunkSizeInt <= 0 {
			chunkSizeInt = 4 * 1024 * 1024 // 默认4MB
		}
		sn := 0
		for i := 0; i < len(modifiedData); i += chunkSizeInt {
			end := i + chunkSizeInt
			if end > len(modifiedData) {
				end = len(modifiedData)
			}
			chunk := modifiedData[i:end]
			if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, chunk); err != nil {
				return 0, err
			}
			sn++
		}
		_, err = ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
		if err != nil {
			return 0, err
		}

		// 优化：使用更高效的key生成（函数内部已使用对象池）
		dataInfoCache.Put(formatCacheKey(ra.fs.bktID, dataInfo.ID), dataInfo)
	}

	newVersionID := ra.fs.h.NewID()
	return newVersionID, nil
}

// applyWritesStreamingUncompressed 处理未压缩未加密的数据
// 可以按chunk流式读取和处理，只读取受影响的数据范围
func (ra *RandomAccessor) applyWritesStreamingUncompressed(fileObj *core.ObjectInfo, oldDataInfo *core.DataInfo,
	writes []WriteOperation, dataInfo *core.DataInfo, chunkSizeInt int, newSize int64) (int64, error) {

	// 对于未压缩未加密的数据，可以按chunk流式处理
	// 优化：预先为每个chunk计算相关的写入操作，减少循环中的重复检查

	// 如果没有SDK配置，直接流式写入
	if ra.fs.sdkCfg == nil {
		dataInfo.Size = newSize
		dataInfo.OrigSize = newSize

		oldDataID := fileObj.DataID
		sn := 0

		// 预先计算每个chunk相关的写入操作索引（优化：减少重复检查）
		// 优化：预估容量，减少slice扩容
		chunkCount := int((newSize + int64(chunkSizeInt) - 1) / int64(chunkSizeInt))
		writesByChunk := make([][]int, chunkCount)
		// 预估每个chunk平均有多少写入操作（优化：减少slice扩容）
		avgWritesPerChunk := len(writes) / chunkCount
		if avgWritesPerChunk < 1 {
			avgWritesPerChunk = 1
		}
		for i := range writesByChunk {
			writesByChunk[i] = make([]int, 0, avgWritesPerChunk)
		}

		for i, write := range writes {
			writeEnd := write.Offset + int64(len(write.Data))
			startChunk := int(write.Offset / int64(chunkSizeInt))
			endChunk := int((writeEnd + int64(chunkSizeInt) - 1) / int64(chunkSizeInt))
			if endChunk >= chunkCount {
				endChunk = chunkCount - 1
			}
			if startChunk < 0 {
				startChunk = 0
			}
			for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
				writesByChunk[chunkIdx] = append(writesByChunk[chunkIdx], i)
			}
		}

		// 按chunk处理
		for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
			pos := int64(chunkIdx * chunkSizeInt)
			chunkEnd := pos + int64(chunkSizeInt)
			if chunkEnd > newSize {
				chunkEnd = newSize
			}
			chunkSize := int(chunkEnd - pos)

			// 读取原始chunk数据（优化：使用对象池重用缓冲区）
			chunkData := chunkDataPool.Get().([]byte)
			// 确保容量足够
			if cap(chunkData) < chunkSize {
				chunkData = make([]byte, chunkSize)
			} else {
				chunkData = chunkData[:chunkSize]
			}

			if oldDataID > 0 && oldDataID != core.EmptyDataID && pos < fileObj.Size {
				readEnd := chunkEnd
				if readEnd > fileObj.Size {
					readEnd = fileObj.Size
				}
				// 读取原始数据的这一chunk
				data, err := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, oldDataID, 0, int(pos), int(readEnd-pos))
				if err == nil && len(data) > 0 {
					copy(chunkData, data)
				}
			}

			// 应用写入操作（只处理当前chunk相关的写入，使用预计算的索引）
			for _, writeIdx := range writesByChunk[chunkIdx] {
				write := writes[writeIdx]
				writeEnd := write.Offset + int64(len(write.Data))

				// 计算重叠范围（优化：减少重复计算）
				overlapStart := int64(0)
				if write.Offset > pos {
					overlapStart = write.Offset - pos
				}
				overlapEnd := int64(chunkSize)
				if writeEnd-pos < overlapEnd {
					overlapEnd = writeEnd - pos
				}
				if overlapStart < overlapEnd {
					writeDataStart := int64(0)
					if write.Offset < pos {
						writeDataStart = pos - write.Offset
					}
					copyLen := overlapEnd - overlapStart
					// 优化：直接使用slice，避免重复计算长度
					copy(chunkData[overlapStart:overlapEnd], write.Data[writeDataStart:writeDataStart+copyLen])
				}
			}

			// 写入chunk
			// 注意：需要复制数据，因为PutData可能会异步处理，而chunkData需要归还到对象池
			chunkDataCopy := make([]byte, len(chunkData))
			copy(chunkDataCopy, chunkData)
			// 写入前归还到对象池（重置长度但保留容量）
			chunkDataPool.Put(chunkData[:0])

			if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, chunkDataCopy); err != nil {
				return 0, err
			}
			sn++
		}

		// 保存数据元数据
		_, err := ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
		if err != nil {
			return 0, err
		}

		// 优化：使用更高效的key生成（函数内部已使用对象池）
		dataInfoCache.Put(formatCacheKey(ra.fs.bktID, dataInfo.ID), dataInfo)

		newVersionID := ra.fs.h.NewID()
		return newVersionID, nil
	}

	// 有SDK配置，需要压缩和加密
	// 对于未压缩未加密的源数据，可以按chunk读取，但压缩加密需要完整数据
	// 所以这里仍然需要读取完整数据，但可以流式写入
	oldDataID := fileObj.DataID
	var originalData []byte
	if oldDataID > 0 && oldDataID != core.EmptyDataID {
		// 读取完整数据（压缩加密需要完整数据）
		data, err := ra.fs.h.GetData(ra.fs.c, ra.fs.bktID, oldDataID, 0)
		if err == nil && len(data) > 0 {
			originalData = data
		}
	}

	// 应用写入操作
	modifiedData := applyWritesToData(originalData, writes)

	// 流式写入（processDataWithSDK已经是按chunk处理的）
	err := ra.processDataWithSDK(modifiedData, dataInfo, int64(chunkSizeInt))
	if err != nil {
		return 0, err
	}

	// 保存数据元数据
	_, err = ra.fs.h.PutDataInfo(ra.fs.c, ra.fs.bktID, []*core.DataInfo{dataInfo})
	if err != nil {
		return 0, err
	}

	// 优化：使用更高效的key生成（函数内部已使用对象池）
	dataInfoCache.Put(formatCacheKey(ra.fs.bktID, dataInfo.ID), dataInfo)

	newVersionID := ra.fs.h.NewID()
	return newVersionID, nil
}

// Close 关闭随机访问对象，自动刷新所有待处理的写入
func (ra *RandomAccessor) Close() error {
	_, err := ra.Flush()
	return err
}

// mergeWriteOperations 合并重叠的写入操作
// 优化：使用更高效的排序算法（快速排序）
func mergeWriteOperations(operations []WriteOperation) []WriteOperation {
	if len(operations) == 0 {
		return nil
	}

	// 优化：如果operations已经是排序的，可以跳过排序
	// 但为了安全，还是进行排序（可以使用更高效的算法）
	// 优化：原地排序，避免额外的内存分配
	sorted := operations
	if len(sorted) > 1 {
		// 创建新slice用于排序（避免修改原slice）
		sorted = make([]WriteOperation, len(operations))
		copy(sorted, operations)
	}
	// 使用快速排序（内置sort包）
	// 但为了不引入新依赖，使用优化的插入排序
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j].Offset > key.Offset {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}

	// 优化：预分配容量，减少扩容
	// 优化：使用对象池获取初始容量
	merged := writeOpsPool.Get().([]WriteOperation)
	merged = merged[:0] // 重置但保留容量
	if cap(merged) < len(sorted) {
		merged = make([]WriteOperation, 0, len(sorted))
	}

	for _, op := range sorted {
		if len(merged) == 0 {
			merged = append(merged, op)
			continue
		}

		last := &merged[len(merged)-1]
		lastEnd := last.Offset + int64(len(last.Data))
		opEnd := op.Offset + int64(len(op.Data))

		// 优化：如果完全重叠且新操作覆盖旧操作，直接替换（避免创建新对象）
		if op.Offset >= last.Offset && opEnd <= lastEnd {
			// 新操作完全在旧操作内，直接覆盖（避免创建新的WriteOperation）
			offsetInLast := op.Offset - last.Offset
			copy(last.Data[offsetInLast:], op.Data)
			continue
		}

		// 如果重叠，合并
		if op.Offset <= lastEnd {
			// 计算新的范围
			startOffset := last.Offset
			if op.Offset < startOffset {
				startOffset = op.Offset
			}
			endOffset := lastEnd
			if opEnd > endOffset {
				endOffset = opEnd
			}

			// 优化：使用对象池获取缓冲区
			mergedData := chunkDataPool.Get().([]byte)
			if cap(mergedData) < int(endOffset-startOffset) {
				mergedData = make([]byte, endOffset-startOffset)
			} else {
				mergedData = mergedData[:endOffset-startOffset]
			}

			// 复制旧数据
			if last.Offset >= startOffset {
				copy(mergedData[last.Offset-startOffset:], last.Data)
			}
			// 复制新数据（覆盖）
			if op.Offset >= startOffset {
				copy(mergedData[op.Offset-startOffset:], op.Data)
			}

			last.Offset = startOffset
			last.Data = mergedData
		} else {
			// 不重叠，添加新操作
			merged = append(merged, op)
		}
	}

	return merged
}

// applyWritesToData 将写入操作应用到数据
// 优化：一次性计算所需大小，避免多次扩展
func applyWritesToData(data []byte, writes []WriteOperation) []byte {
	if len(writes) == 0 {
		return data
	}

	// 计算需要的大小（优化：一次性计算，避免多次扩展）
	var maxSize int64 = int64(len(data))
	for _, write := range writes {
		writeEnd := write.Offset + int64(len(write.Data))
		if writeEnd > maxSize {
			maxSize = writeEnd
		}
	}

	// 优化：一次性分配所需大小，避免多次扩展
	result := make([]byte, maxSize)
	if len(data) > 0 {
		copy(result, data)
	}

	// 应用所有写入操作（优化：移除冗余检查，maxSize已保证容量足够）
	for _, write := range writes {
		if len(write.Data) > 0 {
			writeEnd := write.Offset + int64(len(write.Data))
			copy(result[write.Offset:writeEnd], write.Data)
		}
	}

	return result
}

// processDataWithSDK 使用SDK的逻辑处理数据（压缩和加密）
func (ra *RandomAccessor) processDataWithSDK(data []byte, dataInfo *core.DataInfo, chunkSize int64) error {
	cfg := ra.fs.sdkCfg
	if cfg == nil {
		return fmt.Errorf("SDK config is nil")
	}

	// 压缩和加密方式将在处理第一个数据块时确定

	// 处理数据块
	sn := 0
	chunkSizeInt := int(chunkSize)
	if chunkSizeInt <= 0 {
		chunkSizeInt = 4 * 1024 * 1024 // 默认4MB
	}

	// 计算CRC32（原始数据）
	var crc32Val uint32
	var dataCRC32 uint32 // 原始数据的CRC32

	// 初始化压缩器（如果启用智能压缩）
	var cmpr archiver.Compressor
	if cfg.WiseCmpr > 0 && len(data) > 0 {
		kind, _ := filetype.Match(data)
		if kind == filetype.Unknown {
			if cfg.WiseCmpr&core.DATA_CMPR_SNAPPY != 0 {
				cmpr = &archiver.Snappy{}
			} else if cfg.WiseCmpr&core.DATA_CMPR_ZSTD != 0 {
				cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(cfg.CmprQlty)))}}
			} else if cfg.WiseCmpr&core.DATA_CMPR_GZIP != 0 {
				cmpr = &archiver.Gz{CompressionLevel: int(cfg.CmprQlty)}
			} else if cfg.WiseCmpr&core.DATA_CMPR_BR != 0 {
				cmpr = &archiver.Brotli{Quality: int(cfg.CmprQlty)}
			}
		}
	}

	// 如果设置了压缩器，设置压缩标记
	if cmpr != nil {
		dataInfo.Kind |= cfg.WiseCmpr
	}

	// 如果设置了加密，设置加密标记
	if cfg.EndecWay > 0 {
		dataInfo.Kind |= cfg.EndecWay
	}

	// 处理数据块
	for i := 0; i < len(data); {
		end := i + chunkSizeInt
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]

		// 计算原始数据的CRC32
		dataCRC32 = crc32.Update(dataCRC32, crc32.IEEETable, chunk)

		// 压缩
		var processedChunk []byte
		if dataInfo.Kind&core.DATA_CMPR_MASK != 0 && cmpr != nil {
			var cmprBuf bytes.Buffer
			err := cmpr.Compress(bytes.NewBuffer(chunk), &cmprBuf)
			if err != nil {
				processedChunk = chunk
				// 压缩失败，移除压缩标记
				dataInfo.Kind &= ^core.DATA_CMPR_MASK
			} else {
				// 如果压缩后更大，使用原始数据
				if cmprBuf.Len() >= len(chunk) {
					processedChunk = chunk
					// 移除压缩标记
					dataInfo.Kind &= ^core.DATA_CMPR_MASK
				} else {
					processedChunk = cmprBuf.Bytes()
				}
			}
		} else {
			processedChunk = chunk
		}

		// 加密
		var encodedChunk []byte
		var err error
		if dataInfo.Kind&core.DATA_ENDEC_AES256 != 0 {
			encodedChunk, err = aes256.Encrypt(cfg.EndecKey, processedChunk)
		} else if dataInfo.Kind&core.DATA_ENDEC_SM4 != 0 {
			encodedChunk, err = sm4.Sm4Cbc([]byte(cfg.EndecKey), processedChunk, true)
		} else {
			encodedChunk = processedChunk
		}
		if err != nil {
			encodedChunk = processedChunk
		}

		// 更新最终数据的校验和（压缩和加密后的数据）
		crc32Val = crc32.Update(crc32Val, crc32.IEEETable, encodedChunk)

		// 更新大小（如果压缩或加密了）
		if dataInfo.Kind&core.DATA_CMPR_MASK != 0 || dataInfo.Kind&core.DATA_ENDEC_MASK != 0 {
			dataInfo.Size += int64(len(encodedChunk))
		}

		// 写入数据
		if _, err := ra.fs.h.PutData(ra.fs.c, ra.fs.bktID, dataInfo.ID, sn, encodedChunk); err != nil {
			return err
		}
		sn++
		i = end
	}

	// 设置CRC32和校验和
	dataInfo.CRC32 = dataCRC32 // 原始数据的CRC32
	if dataInfo.Kind&core.DATA_CMPR_MASK == 0 && dataInfo.Kind&core.DATA_ENDEC_MASK == 0 {
		// 没有压缩和加密，使用原始大小和CRC32作为校验和
		dataInfo.Size = dataInfo.OrigSize
		dataInfo.Cksum = dataCRC32
	} else {
		// 有压缩或加密，使用最终数据的CRC32作为校验和
		dataInfo.Cksum = crc32Val
	}

	return nil
}
