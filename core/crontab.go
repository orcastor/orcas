package core

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CronSchedule 解析cron表达式并判断是否应该执行
// 格式：分钟 小时 天 月 星期
// 支持通配符 * 和数字范围
type CronSchedule struct {
	Minute  []int // 分钟（0-59）
	Hour    []int // 小时（0-23）
	Day     []int // 天（1-31）
	Month   []int // 月（1-12）
	Weekday []int // 星期（0-6，0=周日）
}

// ParseCronSchedule 解析cron表达式
// 格式：分钟 小时 天 月 星期
// 示例："0 2 * * *" 表示每天凌晨2点
// 示例："0 4 * * 0" 表示每周日凌晨4点
func ParseCronSchedule(schedule string) (*CronSchedule, error) {
	parts := strings.Fields(schedule)
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid cron schedule format, expected 5 fields, got %d", len(parts))
	}

	cs := &CronSchedule{}

	// 解析分钟
	minute, err := parseCronField(parts[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("invalid minute field: %v", err)
	}
	cs.Minute = minute

	// 解析小时
	hour, err := parseCronField(parts[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("invalid hour field: %v", err)
	}
	cs.Hour = hour

	// 解析天
	day, err := parseCronField(parts[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("invalid day field: %v", err)
	}
	cs.Day = day

	// 解析月
	month, err := parseCronField(parts[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("invalid month field: %v", err)
	}
	cs.Month = month

	// 解析星期
	weekday, err := parseCronField(parts[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("invalid weekday field: %v", err)
	}
	cs.Weekday = weekday

	return cs, nil
}

// parseCronField 解析单个cron字段
// 支持：*（所有值）、数字、范围（如1-5）、列表（如1,3,5）
func parseCronField(field string, min, max int) ([]int, error) {
	if field == "*" {
		// 返回所有可能的值
		result := make([]int, max-min+1)
		for i := min; i <= max; i++ {
			result[i-min] = i
		}
		return result, nil
	}

	var result []int
	// 处理列表（逗号分隔）
	parts := strings.Split(field, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// 处理范围（如1-5）
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range format: %s", part)
			}
			start, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			if err != nil {
				return nil, err
			}
			end, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err != nil {
				return nil, err
			}
			if start < min || end > max || start > end {
				return nil, fmt.Errorf("range %d-%d out of bounds [%d-%d]", start, end, min, max)
			}
			for i := start; i <= end; i++ {
				result = append(result, i)
			}
		} else {
			// 单个数字
			value, err := strconv.Atoi(part)
			if err != nil {
				return nil, err
			}
			if value < min || value > max {
				return nil, fmt.Errorf("value %d out of bounds [%d-%d]", value, min, max)
			}
			result = append(result, value)
		}
	}

	// 去重
	seen := make(map[int]bool)
	var unique []int
	for _, v := range result {
		if !seen[v] {
			seen[v] = true
			unique = append(unique, v)
		}
	}

	return unique, nil
}

// ShouldRun 判断给定时间是否应该执行
func (cs *CronSchedule) ShouldRun(t time.Time) bool {
	minute := t.Minute()
	hour := t.Hour()
	day := t.Day()
	month := int(t.Month())
	weekday := int(t.Weekday()) // 0=Sunday, 6=Saturday

	// 检查分钟
	if !contains(cs.Minute, minute) {
		return false
	}

	// 检查小时
	if !contains(cs.Hour, hour) {
		return false
	}

	// 检查天
	if !contains(cs.Day, day) {
		return false
	}

	// 检查月
	if !contains(cs.Month, month) {
		return false
	}

	// 检查星期
	if !contains(cs.Weekday, weekday) {
		return false
	}

	return true
}

// contains 检查切片是否包含指定值
func contains(slice []int, value int) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// CronScheduler 定时任务调度器
type CronScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	config CronJobConfig
	h      Handler
	ma     MetadataAdapter
	da     DataAdapter
}

// NewCronScheduler 创建定时任务调度器
func NewCronScheduler(ctx context.Context, config CronJobConfig, h Handler, ma MetadataAdapter, da DataAdapter) *CronScheduler {
	schedulerCtx, cancel := context.WithCancel(ctx)
	return &CronScheduler{
		ctx:    schedulerCtx,
		cancel: cancel,
		config: config,
		h:      h,
		ma:     ma,
		da:     da,
	}
}

// Start 启动定时任务调度器
func (cs *CronScheduler) Start() error {
	// 启动各个定时任务
	if cs.config.ScrubEnabled {
		schedule, err := ParseCronSchedule(cs.config.ScrubSchedule)
		if err != nil {
			return fmt.Errorf("invalid scrub schedule: %v", err)
		}
		go cs.runScheduledJob("scrub", schedule, cs.runScrubJob)
	}

	if cs.config.MergeEnabled {
		schedule, err := ParseCronSchedule(cs.config.MergeSchedule)
		if err != nil {
			return fmt.Errorf("invalid merge schedule: %v", err)
		}
		go cs.runScheduledJob("merge", schedule, cs.runMergeJob)
	}

	if cs.config.DefragmentEnabled {
		schedule, err := ParseCronSchedule(cs.config.DefragmentSchedule)
		if err != nil {
			return fmt.Errorf("invalid defragment schedule: %v", err)
		}
		go cs.runScheduledJob("defragment", schedule, cs.runDefragmentJob)
	}

	return nil
}

// Stop 停止定时任务调度器
func (cs *CronScheduler) Stop() {
	cs.cancel()
}

// runScheduledJob 运行定时任务
func (cs *CronScheduler) runScheduledJob(name string, schedule *CronSchedule, jobFunc func(context.Context) error) {
	ticker := time.NewTicker(1 * time.Minute) // 每分钟检查一次
	defer ticker.Stop()

	// 立即检查一次是否可以执行
	now := time.Now()
	if schedule.ShouldRun(now) {
		go func() {
			if err := jobFunc(cs.ctx); err != nil {
				// 可以在这里添加日志记录
				_ = err
			}
		}()
	}

	lastRun := now
	for {
		select {
		case <-cs.ctx.Done():
			return
		case now := <-ticker.C:
			// 避免同一分钟重复执行
			if now.Minute() == lastRun.Minute() {
				continue
			}
			if schedule.ShouldRun(now) {
				lastRun = now
				go func() {
					if err := jobFunc(cs.ctx); err != nil {
						// 可以在这里添加日志记录
						_ = err
					}
				}()
			}
		}
	}
}

// runScrubJob 运行ScrubData任务（针对所有bucket）
func (cs *CronScheduler) runScrubJob(ctx context.Context) error {
	// 获取所有bucket
	buckets, err := cs.ma.ListAllBuckets(ctx)
	if err != nil {
		return err
	}

	for _, bkt := range buckets {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, err := ScrubData(ctx, bkt.ID, cs.ma, cs.da)
			if err != nil {
				// 记录错误但继续处理其他bucket
				continue
			}
		}
	}
	return nil
}

// runMergeJob 运行MergeDuplicateData任务（针对所有bucket）
func (cs *CronScheduler) runMergeJob(ctx context.Context) error {
	// 获取所有bucket
	buckets, err := cs.ma.ListAllBuckets(ctx)
	if err != nil {
		return err
	}

	for _, bkt := range buckets {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, err := MergeDuplicateData(ctx, bkt.ID, cs.ma, cs.da)
			if err != nil {
				// 记录错误但继续处理其他bucket
				continue
			}
		}
	}
	return nil
}

// runDefragmentJob 运行Defragment任务（针对所有bucket）
// 只有满足空间占用阈值时才执行碎片整理
func (cs *CronScheduler) runDefragmentJob(ctx context.Context) error {
	// 获取所有bucket
	buckets, err := cs.ma.ListAllBuckets(ctx)
	if err != nil {
		return err
	}

	for _, bkt := range buckets {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// 检查空间占用阈值
			if !cs.shouldDefragment(bkt) {
				// 不满足阈值，跳过
				continue
			}

			_, err := Defragment(ctx, bkt.ID, cs.h, cs.ma, cs.da, cs.config.DefragmentMaxSize, cs.config.DefragmentAccessWindow)
			if err != nil {
				// 记录错误但继续处理其他bucket
				continue
			}
		}
	}
	return nil
}

// shouldDefragment 判断是否应该执行碎片整理
// 碎片率 = (RealUsed - LogicalUsed对应的物理占用) / RealUsed * 100
// 但由于无法精确计算LogicalUsed对应的物理占用（因为去重），
// 我们使用近似方法：碎片率 = (RealUsed - LogicalUsed) / RealUsed * 100
// 这个值可能为负（当去重节省大于碎片时），此时不需要碎片整理
func (cs *CronScheduler) shouldDefragment(bkt *BucketInfo) bool {
	// 如果阈值设置为0，表示不检查阈值，总是执行
	if cs.config.DefragmentThreshold == 0 {
		return true
	}

	// 如果RealUsed为0，无法计算碎片率，跳过
	if bkt.RealUsed <= 0 {
		return false
	}

	// 计算碎片大小（近似值）
	// 碎片 = 实际使用量 - 逻辑占用
	// 注意：这个值可能为负（当去重节省大于碎片时），此时不需要碎片整理
	fragmentedSize := bkt.RealUsed - bkt.LogicalUsed
	if fragmentedSize <= 0 {
		// 没有碎片（或去重节省大于碎片），不需要整理
		return false
	}

	// 碎片率 = (碎片大小 / 实际使用量) * 100
	fragmentationRate := float64(fragmentedSize) * 100.0 / float64(bkt.RealUsed)

	// 只有当碎片率 >= 阈值时才执行
	return fragmentationRate >= float64(cs.config.DefragmentThreshold)
}
