package sdk

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/h2non/filetype"
	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archiver/v3"
	"github.com/mkmueller/aes256"
	"github.com/orcastor/orcas/core"
	"github.com/tjfoc/gmsm/sm4"
)

const (
	UPLOAD_DATA = 1 << iota
	CRC32_MD5
	HDR_CRC32
)

const (
	PKG_ALIGN     = 4096
	PKG_SIZE      = 4194304 // 默认分片大小，如果配置未设置则使用此值
	HDR_SIZE      = 102400
	DEFAULT_CHUNK = 4 * 1024 * 1024 // 默认4MB
)

type listener struct {
	bktID     int64
	d         *core.DataInfo
	cfg       Config
	action    uint32
	md5Hash   hash.Hash
	once      bool
	sn, cnt   int
	cmprBuf   bytes.Buffer
	cmpr      archiver.Compressor
	chunkSize int64 // 从桶配置中获取的分片大小
}

// getChunkSize 获取分片大小（从桶配置中获取，如果未设置则使用默认值）
func (l *listener) getChunkSize() int64 {
	if l.chunkSize > 0 {
		return l.chunkSize
	}
	return DEFAULT_CHUNK
}

func newListener(bktID int64, d *core.DataInfo, cfg Config, action uint32, chunkSize int64) *listener {
	l := &listener{bktID: bktID, d: d, cfg: cfg, action: action, chunkSize: chunkSize}
	if action&CRC32_MD5 != 0 {
		l.md5Hash = md5.New()
	}
	return l
}

func (l *listener) Once() *listener {
	l.once = true
	return l
}

func (l *listener) encode(src []byte) (dst []byte, err error) {
	// 加密
	if l.d.Kind&core.DATA_ENDEC_AES256 != 0 {
		// AES256加密
		dst, err = aes256.Encrypt(l.cfg.EndecKey, src)
	} else if l.d.Kind&core.DATA_ENDEC_SM4 != 0 {
		// SM4加密
		dst, err = sm4.Sm4Cbc([]byte(l.cfg.EndecKey), src, true) // sm4Cbc模式PKSC7填充加密
	} else {
		dst = src
	}
	if err != nil {
		dst = src
	}
	return
}

func (l *listener) OnData(c core.Ctx, h core.Handler, dp *dataPkger, buf []byte) (once bool, err error) {
	if l.cnt == 0 {
		if l.action&HDR_CRC32 != 0 {
			if len(buf) > HDR_SIZE {
				l.d.HdrCRC32 = crc32.ChecksumIEEE(buf[0:HDR_SIZE])
			} else {
				l.d.HdrCRC32 = crc32.ChecksumIEEE(buf)
			}
		}
		// 如果开启智能压缩的，检查文件类型确定是否要压缩
		if l.cfg.WiseCmpr > 0 {
			kind, _ := filetype.Match(buf)
			if kind == filetype.Unknown { // 多媒体、存档、应用
				l.d.Kind |= l.cfg.WiseCmpr
				if l.cfg.WiseCmpr&core.DATA_CMPR_SNAPPY != 0 {
					l.cmpr = &archiver.Snappy{}
				} else if l.cfg.WiseCmpr&core.DATA_CMPR_ZSTD != 0 {
					l.cmpr = &archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(l.cfg.CmprQlty)))}}
				} else if l.cfg.WiseCmpr&core.DATA_CMPR_GZIP != 0 {
					l.cmpr = &archiver.Gz{CompressionLevel: int(l.cfg.CmprQlty)}
				} else if l.cfg.WiseCmpr&core.DATA_CMPR_BR != 0 {
					l.cmpr = &archiver.Brotli{Quality: int(l.cfg.CmprQlty)}
				}
			}
			// 如果是黑名单类型，不压缩
			// fmt.Println(kind.MIME.Value)
		}
		if l.cfg.EndecWay > 0 {
			l.d.Kind |= l.cfg.EndecWay
		}
	}
	isFirstChunk := l.cnt == 0
	l.cnt++

	if l.action&CRC32_MD5 != 0 {
		l.d.CRC32 = crc32.Update(l.d.CRC32, crc32.IEEETable, buf)
		l.md5Hash.Write(buf)
	}

	// 上传数据
	// 重要：先切块（buf已经是chunk大小的数据），再对每个chunk独立压缩加密
	if l.action&UPLOAD_DATA != 0 {
		// 对当前chunk进行处理
		var processedChunk []byte

		// 1. 先压缩（如果启用）
		if l.d.Kind&core.DATA_CMPR_MASK != 0 && l.cmpr != nil {
			l.cmprBuf.Reset()
			err := l.cmpr.Compress(bytes.NewBuffer(buf), &l.cmprBuf)
			if err != nil {
				// 压缩失败，使用原始数据，移除压缩标记
				// 注意：这个逻辑只在第一个chunk时执行，因为一旦决定压缩，后续chunk都应该保持一致
				if isFirstChunk {
					l.d.Kind &= ^core.DATA_CMPR_MASK
				}
				processedChunk = buf
			} else {
				// 如果压缩后更大或相等，使用原始数据，移除压缩标记
				// 注意：这个逻辑只在第一个chunk时执行，因为一旦决定压缩，后续chunk都应该保持一致
				if isFirstChunk && l.cmprBuf.Len() >= len(buf) {
					l.d.Kind &= ^core.DATA_CMPR_MASK
					processedChunk = buf
				} else {
					processedChunk = l.cmprBuf.Bytes()
				}
			}
		} else {
			processedChunk = buf
		}

		// 2. 再加密（如果启用）
		encodedBuf, encodeErr := l.encode(processedChunk)
		if encodeErr != nil {
			encodedBuf = processedChunk // 加密失败，使用未加密的数据
		}

		// 3. 更新校验和和大小
		if l.d.Kind&core.DATA_CMPR_MASK != 0 || l.d.Kind&core.DATA_ENDEC_MASK != 0 {
			l.d.Cksum = crc32.Update(l.d.Cksum, crc32.IEEETable, encodedBuf)
			l.d.Size += int64(len(encodedBuf))
		}

		// 4. 上传数据块
		chunkSize := l.getChunkSize()
		if l.d.OrigSize < chunkSize {
			// 小文件，检查是否要打包
			var pushOk bool
			if pushOk, err = dp.Push(c, h, l.bktID, encodedBuf, l.d); err == nil {
				if pushOk {
					encodedBuf = nil // 打包成功，不再上传
				}
			} else {
				return l.once, err
			}
		}

		if encodedBuf != nil {
			if l.d.ID, err = h.PutData(c, l.bktID, l.d.ID, l.sn, encodedBuf); err != nil {
				return l.once, err
			}
			l.sn++
		}
	}
	return l.once, nil
}

func (l *listener) OnFinish(c core.Ctx, h core.Handler) error {
	if l.action&CRC32_MD5 != 0 {
		l.d.MD5 = int64(binary.BigEndian.Uint64(l.md5Hash.Sum(nil)[4:12]))
	}
	// 注意：现在每个chunk都在OnData中立即处理并上传，不需要在这里处理残留数据
	// 如果压缩后产生了残留数据（理论上不应该，因为每个chunk独立压缩），这里处理
	if l.cmprBuf.Len() > 0 {
		// 对残留数据进行加密（如果启用）
		encodedBuf, err := l.encode(l.cmprBuf.Bytes())
		if err != nil {
			return err
		}
		if l.d.ID, err = h.PutData(c, l.bktID, l.d.ID, l.sn, encodedBuf); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		l.d.Cksum = crc32.Update(l.d.Cksum, crc32.IEEETable, encodedBuf)
		l.d.Size += int64(len(encodedBuf))
	}
	// 如果既没有压缩也没有加密，使用原始数据的CRC32和大小
	if l.d.Kind&core.DATA_CMPR_MASK == 0 && l.d.Kind&core.DATA_ENDEC_MASK == 0 {
		l.d.Cksum = l.d.CRC32
		l.d.Size = l.d.OrigSize
	}
	return nil
}

func (osi *OrcasSDKImpl) readFile(c core.Ctx, path string, dp *dataPkger, l *listener) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var n int
	var once bool
	chunkSize := l.getChunkSize()
	buf := make([]byte, chunkSize)
	for {
		if n, err = f.Read(buf); n <= 0 || err != nil {
			break
		}
		if once, err = l.OnData(c, osi.h, dp, buf[0:n]); once || err != nil {
			break
		}
	}
	if err != io.EOF {
		return err
	}
	err = l.OnFinish(c, osi.h)
	return err
}

func (osi *OrcasSDKImpl) putOne(c core.Ctx, bktID int64, o *core.ObjectInfo) (int64, error) {
	ids, err := osi.h.Put(c, bktID, []*core.ObjectInfo{o})
	if err != nil {
		return 0, err
	}
	if len(ids) > 0 && ids[0] > 0 {
		return ids[0], nil
	}
	return 0, fmt.Errorf("remote object exists, pid:%d, name:%s", o.PID, o.Name)
}

func (osi *OrcasSDKImpl) getRename(o *core.ObjectInfo, i int) *core.ObjectInfo {
	x := *o
	if i <= 0 {
		x.Name = fmt.Sprintf(osi.cfg.NameTmpl, x.Name)
	} else {
		x.Name = fmt.Sprintf(osi.cfg.NameTmpl+"%d", x.Name, i+1)
	}
	return &x
}

func (osi *OrcasSDKImpl) putObjects(c core.Ctx, bktID int64, o []*core.ObjectInfo) ([]int64, error) {
	if len(o) <= 0 {
		return nil, nil
	}

	ids, err := osi.h.Put(c, bktID, o)
	if err != nil {
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return nil, err
	}

	switch osi.cfg.Conflict {
	case COVER: // 合并或覆盖
		var vers []*core.ObjectInfo
		for i := range ids {
			if ids[i] <= 0 {
				var pathErr error
				ids[i], pathErr = osi.Path2ID(c, bktID, o[i].PID, o[i].Name)
				if pathErr != nil {
					err = pathErr
				}
				// 如果是文件，需要新建一个版本，版本更新的逻辑需要jobs来完成
				if o[i].Type == core.OBJ_TYPE_FILE {
					vers = append(vers, &core.ObjectInfo{
						PID:   ids[i],
						MTime: o[i].MTime,
						Type:  core.OBJ_TYPE_VERSION,
						Size:  o[i].Size,
					})
				}
			}
		}
		if len(vers) > 0 {
			_, err = osi.h.Put(c, bktID, vers)
			if err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
				return nil, err
			}
			// 需要定时任务把首版本的DataID更新掉
		}
	case RENAME: // 重命名
		m := make(map[int]int)
		var rename []*core.ObjectInfo
		for i := range ids {
			// 需要重命名重新创建
			if ids[i] <= 0 {
				// 先直接用NameTmpl创建，覆盖大部分场景
				m[len(rename)] = i
				rename = append(rename, osi.getRename(o[i], 0))
			}
		}
		if len(rename) > 0 {
			ids2, err2 := osi.h.Put(c, bktID, rename)
			if err2 != nil {
				return ids, err2
			}
			for i := range ids2 {
				if ids2[i] > 0 {
					ids[m[i]] = ids2[i]
					continue
				}
				// 还是失败，用NameTmpl找有多少个目录，然后往后一个一个尝试
				_, cnt, _, err3 := osi.h.List(c, bktID, rename[i].PID, core.ListOptions{
					Word: rename[i].Name + "*",
				})
				if err3 != nil {
					return ids, err3
				}
				// 假设有 test、test的副本、test的副本2，cnt为2
				for j := 0; j <= int(cnt/2)+1; j++ {
					// 先试试个数后面一个，正常顺序查找，最大概率命中的分支
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], int(cnt)+j)); err3 == nil {
						break
					}
					// 从最前面往后找
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], j)); err3 == nil {
						break
					}
					// 从cnt个开始往前找
					if ids[m[i]], err3 = osi.putOne(c, bktID,
						osi.getRename(o[i], int(cnt)-1-j)); err3 == nil {
						break
					}
				}
			}
		}
	case THROW: // 报错
		for i := range ids {
			if ids[i] <= 0 {
				return ids, fmt.Errorf("remote object exists, pid:%d, name:%s", o[i].PID, o[i].Name)
			}
		}
	case SKIP: // 跳过
		break
	}
	return ids, nil
}

type sizeSort []uploadInfo

func (u sizeSort) Len() int           { return len(u) }
func (u sizeSort) Less(i, j int) bool { return u[i].o.Size < u[j].o.Size || u[i].o.Name < u[j].o.Name }
func (u sizeSort) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }

func (osi *OrcasSDKImpl) uploadFiles(c core.Ctx, bktID int64, u []uploadInfo,
	d []*core.DataInfo, dp *dataPkger, level, doneAction uint32,
) error {
	if len(u) <= 0 {
		return nil
	}

	if len(d) <= 0 {
		// 先按文件大小排序一下，尽量让它们可以打包
		sort.Sort(sizeSort(u))
		d = make([]*core.DataInfo, len(u))
		for i := range u {
			d[i] = &core.DataInfo{
				Kind:     core.DATA_NORMAL,
				OrigSize: u[i].o.Size,
			}
		}
	}

	// 从桶配置中获取chunkSize
	var chunkSize int64 = DEFAULT_CHUNK
	buckets, err := osi.h.GetBkt(c, []int64{bktID})
	if err == nil && len(buckets) > 0 && buckets[0].ChunkSize > 0 {
		chunkSize = buckets[0].ChunkSize
	}
	if dp == nil {
		dp = newDataPkger(osi.cfg.PkgThres, chunkSize)
	}

	var u1, u2 []uploadInfo
	var d1, d2 []*core.DataInfo

	// 如果是文件，先看是否要秒传
	switch level {
	case FAST:
		// 如果要预先秒传的，先读取hdrCRC32，排队检查
		// 使用从桶配置中获取的chunkSize
		for i, fi := range u {
			if fi.o.Size > chunkSize {
				u1 = append(u1, fi)
				d1 = append(d1, d[i])
			} else {
				u2 = append(u2, fi)
				d2 = append(d2, d[i])
			}
		}
		if len(u1) > 0 {
			u, d, u1, d1 = u1, d1, nil, nil
			for i, fi := range u {
				if err := osi.readFile(c, filepath.Join(fi.path, fi.o.Name), dp,
					newListener(bktID, d[i], osi.cfg, HDR_CRC32&^doneAction, chunkSize).Once()); err != nil {
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
				}
			}
			ids, err := osi.h.Ref(c, bktID, d)
			if err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
			}
			for i, id := range ids {
				if id > 0 {
					u2 = append(u2, u[i])
					d2 = append(d2, d[i])
				} else {
					u1 = append(u1, u[i])
					d1 = append(d1, d[i])
				}
			}
			if err := osi.uploadFiles(c, bktID, u1, d1, dp, OFF, doneAction|HDR_CRC32); err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
				return err
			}
		}
		if err := osi.uploadFiles(c, bktID, u2, d2, dp, FULL, doneAction); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case FULL:
		// 如果不需要预先秒传或者预先秒传失败的，整个读取crc32和md5以后尝试秒传
		for i, fi := range u {
			if err := osi.readFile(c, filepath.Join(fi.path, fi.o.Name), dp,
				newListener(bktID, d[i], osi.cfg, (HDR_CRC32|CRC32_MD5)&^doneAction, chunkSize)); err != nil {
				fmt.Println(runtime.Caller(0))
				fmt.Println(err)
			}
		}
		ids, err := osi.h.Ref(c, bktID, d)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
		}
		var f []*core.ObjectInfo
		var gap int64
		for i, id := range ids {
			// 设置DataID，如果是有的，说明秒传成功，不再需要上传数据了
			if id > 0 {
				u[i].o.DataID = id
				f = append(f, u[i].o)
				gap++
			} else {
				if id < 0 { // 小于0说明是引用的数据
					d[i].ID = id + gap // 有人走了，那需要补充空隙
				}
				u1 = append(u1, u[i])
				d1 = append(d1, d[i])
			}
		}
		if _, err := osi.putObjects(c, bktID, f); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		// 秒传失败的（包括超过大小或者预先秒传失败），普通上传
		// 注意：这里递归调用 uploadFiles，chunkSize 已经在上层获取并传递给了 dp
		if err := osi.uploadFiles(c, bktID, u1, d1, dp, OFF, doneAction|HDR_CRC32|CRC32_MD5); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	case OFF:
		// 直接上传的对象
		for i, fi := range u {
			// 如果是引用别人的，也就是<0的，不用再传了就
			if d[i].ID >= 0 {
				if err := osi.readFile(c, filepath.Join(fi.path, fi.o.Name), dp,
					newListener(bktID, d[i], osi.cfg, (UPLOAD_DATA|HDR_CRC32|CRC32_MD5)&^doneAction, chunkSize)); err != nil {
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
				}
			}
		}
		// 刷新一下打包数据
		if err := dp.Flush(c, osi.h, bktID); err != nil {
			return err
		}
		ids, err := osi.h.PutDataInfo(c, bktID, d)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
		var f []*core.ObjectInfo
		// 处理打包上传的对象
		for i, id := range ids {
			if u[i].o.DataID != id { // 包括小于0的
				u[i].o.DataID = id
			}
			f = append(f, u[i].o)
		}
		if _, err := osi.putObjects(c, bktID, f); err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	}
	return nil
}

type DummyArchiver struct{}

func (DummyArchiver) CheckExt(string) error { return nil }
func (DummyArchiver) Compress(in io.Reader, out io.Writer) error {
	_, err := io.Copy(out, in)
	return err
}

func (DummyArchiver) Decompress(in io.Reader, out io.Writer) error {
	_, err := io.Copy(out, in)
	return err
}

type dataReader struct {
	c               core.Ctx
	h               core.Handler
	buf             bytes.Buffer
	bktID           int64
	dataID          int64
	size, sn        int
	getSize, remain int
	offset, kind    uint32
	endecKey        string
}

func NewDataReader(c core.Ctx, h core.Handler, bktID int64, d *core.DataInfo, endecKey string) *dataReader {
	dr := &dataReader{c: c, h: h, bktID: bktID, remain: int(d.Size), kind: d.Kind, endecKey: endecKey}
	if d.PkgID > 0 {
		dr.dataID = d.PkgID
		dr.offset = d.PkgOffset
		dr.getSize = int(d.Size)
	} else {
		dr.dataID = d.ID
		dr.getSize = -1
	}
	return dr
}

func (dr *dataReader) Read(p []byte) (n int, err error) {
	if len(p) <= dr.buf.Len() {
		return dr.buf.Read(p)
	}
	if dr.remain > 0 {
		// GetData 的 offsetOrSize 参数：只传一个参数说明是sn，传两个参数说明是sn+offset，传三个参数说明是sn+offset+size
		buf, err := dr.h.GetData(dr.c, dr.bktID, dr.dataID, dr.sn, int(dr.offset), dr.getSize)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return 0, err
		}
		dr.remain -= len(buf)
		dr.sn++

		// 重要：先解密，再解压缩（因为每个chunk是独立压缩加密的）
		// 1. 先解密（如果启用）
		decodeBuf := buf
		if dr.kind&core.DATA_ENDEC_AES256 != 0 {
			// AES256解密
			decodeBuf, err = aes256.Decrypt(dr.endecKey, buf)
		} else if dr.kind&core.DATA_ENDEC_SM4 != 0 {
			// SM4解密
			decodeBuf, err = sm4.Sm4Cbc([]byte(dr.endecKey), buf, false) // sm4Cbc模式pksc7填充解密
		}
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			decodeBuf = buf
		}

		// 2. 再解压缩（如果启用）
		// 注意：每个chunk是独立压缩的，所以需要为每个chunk创建独立的解压缩器
		finalBuf := decodeBuf
		if dr.kind&core.DATA_CMPR_MASK != 0 {
			var decompressor archiver.Decompressor
			if dr.kind&core.DATA_CMPR_SNAPPY != 0 {
				decompressor = &archiver.Snappy{}
			} else if dr.kind&core.DATA_CMPR_ZSTD != 0 {
				decompressor = &archiver.Zstd{}
			} else if dr.kind&core.DATA_CMPR_GZIP != 0 {
				decompressor = &archiver.Gz{}
			} else if dr.kind&core.DATA_CMPR_BR != 0 {
				decompressor = &archiver.Brotli{}
			}

			if decompressor != nil {
				var decompressedBuf bytes.Buffer
				err := decompressor.Decompress(bytes.NewReader(decodeBuf), &decompressedBuf)
				if err != nil {
					// 解压缩失败，使用解密后的数据（可能是压缩数据损坏或格式不对）
					fmt.Println(runtime.Caller(0))
					fmt.Println(err)
					finalBuf = decodeBuf
				} else {
					finalBuf = decompressedBuf.Bytes()
				}
			}
		}

		dr.buf.Write(finalBuf)
	}
	return dr.buf.Read(p)
}

func (osi *OrcasSDKImpl) downloadFile(c core.Ctx, bktID int64, o *core.ObjectInfo, lpath string) (err error) {
	if o.Type != core.OBJ_TYPE_FILE {
		return nil
	}

	dataID := o.DataID
	// 如果不是首版本
	if dataID == 0 {
		os, _, _, listErr := osi.h.List(c, bktID, o.ID, core.ListOptions{
			Type:  core.OBJ_TYPE_VERSION,
			Count: 1,
			Order: "-id",
		})
		if listErr != nil || len(os) < 1 {
			fmt.Println(runtime.Caller(0))
			fmt.Println(listErr)
			return listErr
		}
		dataID = os[0].DataID
	}

	var d *core.DataInfo
	if dataID == core.EmptyDataID {
		d = core.EmptyDataInfo()
	} else {
		d, err = osi.h.GetDataInfo(c, bktID, dataID)
		if err != nil {
			fmt.Println(runtime.Caller(0))
			fmt.Println(err)
			return err
		}
	}

	os.MkdirAll(filepath.Dir(lpath), 0o766)

	f, err := os.OpenFile(lpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()
	b := bufio.NewWriter(f)
	defer b.Flush()

	var decmpr archiver.Decompressor
	if d.Kind&core.DATA_CMPR_MASK != 0 {
		if d.Kind&core.DATA_CMPR_SNAPPY != 0 {
			decmpr = &archiver.Snappy{}
		} else if d.Kind&core.DATA_CMPR_ZSTD != 0 {
			decmpr = &archiver.Zstd{}
		} else if d.Kind&core.DATA_CMPR_GZIP != 0 {
			decmpr = &archiver.Gz{}
		} else if d.Kind&core.DATA_CMPR_BR != 0 {
			decmpr = &archiver.Brotli{}
		}
	} else {
		decmpr = &DummyArchiver{}
	}

	if err = decmpr.Decompress(NewDataReader(c, osi.h, bktID, d, osi.cfg.EndecKey), b); err != nil {
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return err
	}
	return nil
}

type dataPkger struct {
	buf       *bytes.Buffer
	infos     []*core.DataInfo
	thres     uint32
	chunkSize int64 // 分片大小
}

func newDataPkger(thres uint32, chunkSize int64) *dataPkger {
	if chunkSize <= 0 {
		chunkSize = DEFAULT_CHUNK
	}
	return &dataPkger{
		buf:       bytes.NewBuffer(make([]byte, 0, chunkSize)),
		thres:     thres,
		chunkSize: chunkSize,
	}
}

func (dp *dataPkger) SetThres(thres uint32) {
	dp.thres = thres
}

func (dp *dataPkger) Push(c core.Ctx, h core.Handler, bktID int64, b []byte, d *core.DataInfo) (bool, error) {
	offset := dp.buf.Len()
	// 使用配置的chunkSize而不是PKG_SIZE常量
	chunkSize := dp.chunkSize
	if chunkSize <= 0 {
		chunkSize = DEFAULT_CHUNK
	}
	if offset+ /*offset%PKG_ALIGN+*/ len(b) > int(chunkSize) || len(dp.infos) >= int(dp.thres) || len(b) >= int(chunkSize) {
		return false, dp.Flush(c, h, bktID)
	}
	// 写入前再处理对齐，最后一块就不用补齐了，PS：需要测试一下读性能差多少
	/*if offset%PKG_ALIGN > 0 {
		if padding := PKG_ALIGN - offset%PKG_ALIGN; padding > 0 {
			dp.buf = append(dp.buf, make([]byte, padding)...)
			offset = len(dp.buf)
		}
	}*/
	// 填充内容
	dp.buf.Write(b)
	// 记录偏移
	d.PkgOffset = uint32(offset)
	// 记录下来要设置打包数据的数据信息
	dp.infos = append(dp.infos, d)
	return true, nil
}

func (dp *dataPkger) Flush(c core.Ctx, h core.Handler, bktID int64) error {
	if dp.buf.Len() <= 0 {
		return nil
	}
	// 上传打包的数据包
	pkgID, err := h.PutData(c, bktID, 0, 0, dp.buf.Bytes())
	if err != nil {
		fmt.Println(runtime.Caller(0))
		fmt.Println(err)
		return err
	}
	if len(dp.infos) == 1 {
		dp.infos[0].ID = pkgID
	} else {
		for i := range dp.infos {
			dp.infos[i].PkgID = pkgID
		}
	}
	dp.buf.Reset()
	dp.infos = nil
	return nil
}
