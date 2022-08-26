package sdk

import (
	"errors"
	"fmt"
	"strings"

	"github.com/orcastor/orcas/core"
)

type Config struct {
	DataSync bool   // 断电保护策略(Power-off Protection Policy)，强制每次写入数据后刷到磁盘
	RefLevel int    // 0: OFF / 1: Ref / 2: TryRef+Ref
	RefThres int    // 秒传大小限制，不限制默认为0
	PkgThres int    // 打包个数限制，不设置默认50个
	Conflict int    // 同名冲突后，0: Throw / 1: Merge / 2: Rename
	NameTail string // 重命名尾巴，"-副本" / "{\d}"
	WiseCmpr bool   // 智能压缩，根据文件类型决定是否压缩
	ChkPtDir string // 断点续传记录目录，不设置路径默认不开启
	// BEDecmpr bool   // 后端解压，PS：必须是非加密数据
}

type OrcasSDK interface {
	SetConfig(cfg Config)

	GetObjectIDByPath(c core.Ctx, path string) (id int64, err error)

	UploadDirByPID(c core.Ctx, pid int64, path string) error
	UploadFileByPID(c core.Ctx, pid int64, path string) error
}

type OrcasSDKImpl struct {
	h   core.Hanlder
	cfg Config
}

func New(h core.Hanlder) OrcasSDK {
	return &OrcasSDKImpl{h: h}
}

func (osi *OrcasSDKImpl) SetConfig(cfg Config) {
	osi.cfg = cfg
}

func (osi *OrcasSDKImpl) GetObjectIDByPath(c core.Ctx, path string) (id int64, err error) {
	for _, child := range strings.Split(path, "/") {
		if child == "" {
			continue
		}
		os, _, _, err := osi.h.List(c, id, core.ListOptions{
			Word:  child,
			Count: 1,
			Brief: 2,
		})
		if err != nil {
			return 0, fmt.Errorf("open remote path error(%s): %+v", path, err)
		}
		if len(os) <= 0 {
			return 0, errors.New("open remote path error: " + path)
		}
		id = os[0].ID
	}
	return
}

func (osi *OrcasSDKImpl) UploadDirByPID(c core.Ctx, pid int64, path string) error {
	// 遍历本地目录（支持断点续传）
	// 1. 如果是目录，直接创建
	// 2. 如果是文件，先看是否要秒传
	// 3. 如果要预先秒传的，先读取hdrCrc32，排队检查
	// 4. 如果不需要预先秒传或者预先秒传失败的，整个读取crc32和md5以后尝试秒传
	// 5. 秒传失败的（包括超过大小或者预先秒传失败），丢到待上传对列
	// 6. 如果开启智能压缩的，检查文件类型确定是否要压缩
	// 7. 检查是否要打包，不要打包的直接上传
	// 打包，默认一次不超过50个，大小不超过4MB
	return nil
}

func (osi *OrcasSDKImpl) UploadFileByPID(c core.Ctx, pid int64, path string) error {
	return nil
}

const (
	PKG_ALIGN = 8192
	PKG_SIZE  = 4194304
)

type DataPkg struct {
	buf     []byte
	offsets []int
	thres   int
}

func NewDataPkg(thres int) *DataPkg {
	return &DataPkg{
		buf:   make([]byte, 0, PKG_SIZE),
		thres: thres,
	}
}

func (dp *DataPkg) Push(b []byte) bool {
	offset := len(dp.buf)
	if offset+len(b) > PKG_SIZE || len(dp.offsets) >= dp.thres {
		return false
	}
	// 填充内容
	dp.buf = append(dp.buf, b...)
	// 记录偏移
	dp.offsets = append(dp.offsets, offset)
	// 处理对齐
	padding := PKG_ALIGN - len(b)%PKG_ALIGN
	if padding > 0 {
		dp.buf = append(dp.buf, make([]byte, padding)...)
	}
	return true
}

func (dp *DataPkg) GetOffsets() []int {
	return dp.offsets
}
