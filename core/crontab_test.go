package core

import (
	"context"
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseCronSchedule(t *testing.T) {
	Convey("Parse Cron Schedule", t, func() {
		Convey("parse valid cron schedule", func() {
			schedule, err := ParseCronSchedule("0 2 * * *")
			So(err, ShouldBeNil)
			So(schedule, ShouldNotBeNil)
			So(len(schedule.Minute), ShouldEqual, 1)
			So(schedule.Minute[0], ShouldEqual, 0)
			So(len(schedule.Hour), ShouldEqual, 1)
			So(schedule.Hour[0], ShouldEqual, 2)
			So(len(schedule.Day), ShouldEqual, 31)    // 1-31
			So(len(schedule.Month), ShouldEqual, 12)  // 1-12
			So(len(schedule.Weekday), ShouldEqual, 7) // 0-6
		})

		Convey("parse cron schedule with range", func() {
			schedule, err := ParseCronSchedule("0-5 1-3 * * *")
			So(err, ShouldBeNil)
			So(schedule, ShouldNotBeNil)
			So(len(schedule.Minute), ShouldEqual, 6) // 0-5
			So(len(schedule.Hour), ShouldEqual, 3)   // 1-3
		})

		Convey("parse cron schedule with list", func() {
			schedule, err := ParseCronSchedule("0,15,30,45 * * * *")
			So(err, ShouldBeNil)
			So(schedule, ShouldNotBeNil)
			So(len(schedule.Minute), ShouldEqual, 4)
			So(schedule.Minute[0], ShouldEqual, 0)
			So(schedule.Minute[1], ShouldEqual, 15)
			So(schedule.Minute[2], ShouldEqual, 30)
			So(schedule.Minute[3], ShouldEqual, 45)
		})

		Convey("parse invalid cron schedule", func() {
			_, err := ParseCronSchedule("invalid")
			So(err, ShouldNotBeNil)

			_, err = ParseCronSchedule("0 2 *")
			So(err, ShouldNotBeNil)

			_, err = ParseCronSchedule("0 25 * * *") // 小时超出范围
			So(err, ShouldNotBeNil)
		})
	})
}

func TestCronScheduleShouldRun(t *testing.T) {
	Convey("Cron Schedule ShouldRun", t, func() {
		Convey("should run at specific time", func() {
			schedule, err := ParseCronSchedule("30 14 * * *") // 每天14:30
			So(err, ShouldBeNil)

			// 创建一个14:30的时间
			testTime := time.Date(2024, 1, 15, 14, 30, 0, 0, time.Local)
			So(schedule.ShouldRun(testTime), ShouldBeTrue)

			// 创建一个14:31的时间（不应该运行）
			testTime2 := time.Date(2024, 1, 15, 14, 31, 0, 0, time.Local)
			So(schedule.ShouldRun(testTime2), ShouldBeFalse)
		})

		Convey("should run on specific weekday", func() {
			schedule, err := ParseCronSchedule("0 4 * * 0") // 每周日凌晨4点
			So(err, ShouldBeNil)

			// 周日（Weekday=0）
			sunday := time.Date(2024, 1, 7, 4, 0, 0, 0, time.Local) // 2024-01-07是周日
			So(schedule.ShouldRun(sunday), ShouldBeTrue)

			// 周一（Weekday=1）
			monday := time.Date(2024, 1, 8, 4, 0, 0, 0, time.Local) // 2024-01-08是周一
			So(schedule.ShouldRun(monday), ShouldBeFalse)
		})

		Convey("should run with wildcard", func() {
			schedule, err := ParseCronSchedule("* * * * *") // 每分钟
			So(err, ShouldBeNil)

			testTime := time.Date(2024, 1, 15, 14, 30, 0, 0, time.Local)
			So(schedule.ShouldRun(testTime), ShouldBeTrue)

			testTime2 := time.Date(2024, 1, 15, 14, 31, 0, 0, time.Local)
			So(schedule.ShouldRun(testTime2), ShouldBeTrue)
		})
	})
}

func TestCronJobConfig(t *testing.T) {
	Convey("Cron Job Configuration", t, func() {
		// 保存原始环境变量
		originalEnv := make(map[string]string)
		envVars := []string{
			"ORCAS_CRON_SCRUB_ENABLED",
			"ORCAS_CRON_SCRUB_SCHEDULE",
			"ORCAS_CRON_MERGE_ENABLED",
			"ORCAS_CRON_MERGE_SCHEDULE",
			"ORCAS_CRON_DEFRAGMENT_ENABLED",
			"ORCAS_CRON_DEFRAGMENT_SCHEDULE",
			"ORCAS_CRON_DEFRAGMENT_MAX_SIZE",
			"ORCAS_CRON_DEFRAGMENT_ACCESS_WINDOW",
		}

		// 保存原始值
		for _, key := range envVars {
			originalEnv[key] = os.Getenv(key)
			os.Unsetenv(key)
		}

		// 测试完成后恢复
		defer func() {
			for key, value := range originalEnv {
				if value != "" {
					os.Setenv(key, value)
				} else {
					os.Unsetenv(key)
				}
			}
		}()

		Convey("default values", func() {
			config := GetCronJobConfig()
			So(config.ScrubEnabled, ShouldBeFalse)
			So(config.ScrubSchedule, ShouldEqual, "0 2 * * *")
			So(config.MergeEnabled, ShouldBeFalse)
			So(config.MergeSchedule, ShouldEqual, "0 3 * * *")
			So(config.DefragmentEnabled, ShouldBeFalse)
			So(config.DefragmentSchedule, ShouldEqual, "0 4 * * 0")
			So(config.DefragmentMaxSize, ShouldEqual, 10*1024*1024)
			So(config.DefragmentAccessWindow, ShouldEqual, 0)
		})

		Convey("custom scrub config", func() {
			os.Setenv("ORCAS_CRON_SCRUB_ENABLED", "true")
			os.Setenv("ORCAS_CRON_SCRUB_SCHEDULE", "0 3 * * *")
			config := GetCronJobConfig()
			So(config.ScrubEnabled, ShouldBeTrue)
			So(config.ScrubSchedule, ShouldEqual, "0 3 * * *")
			os.Unsetenv("ORCAS_CRON_SCRUB_ENABLED")
			os.Unsetenv("ORCAS_CRON_SCRUB_SCHEDULE")
		})

		Convey("custom merge config", func() {
			os.Setenv("ORCAS_CRON_MERGE_ENABLED", "1")
			os.Setenv("ORCAS_CRON_MERGE_SCHEDULE", "0 5 * * *")
			config := GetCronJobConfig()
			So(config.MergeEnabled, ShouldBeTrue)
			So(config.MergeSchedule, ShouldEqual, "0 5 * * *")
			os.Unsetenv("ORCAS_CRON_MERGE_ENABLED")
			os.Unsetenv("ORCAS_CRON_MERGE_SCHEDULE")
		})

		Convey("custom defragment config", func() {
			os.Setenv("ORCAS_CRON_DEFRAGMENT_ENABLED", "true")
			os.Setenv("ORCAS_CRON_DEFRAGMENT_SCHEDULE", "0 6 * * 0")
			os.Setenv("ORCAS_CRON_DEFRAGMENT_MAX_SIZE", "20971520")
			os.Setenv("ORCAS_CRON_DEFRAGMENT_ACCESS_WINDOW", "3600")
			config := GetCronJobConfig()
			So(config.DefragmentEnabled, ShouldBeTrue)
			So(config.DefragmentSchedule, ShouldEqual, "0 6 * * 0")
			So(config.DefragmentMaxSize, ShouldEqual, 20971520)
			So(config.DefragmentAccessWindow, ShouldEqual, 3600)
			os.Unsetenv("ORCAS_CRON_DEFRAGMENT_ENABLED")
			os.Unsetenv("ORCAS_CRON_DEFRAGMENT_SCHEDULE")
			os.Unsetenv("ORCAS_CRON_DEFRAGMENT_MAX_SIZE")
			os.Unsetenv("ORCAS_CRON_DEFRAGMENT_ACCESS_WINDOW")
		})

	})
}

func TestCronScheduler(t *testing.T) {
	Convey("Cron Scheduler", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dma := &DefaultMetadataAdapter{}
		dda := &DefaultDataAdapter{}
		lh := NewLocalHandler("", "").(*LocalHandler)
		lh.SetAdapter(dma, dda)

		config := GetCronJobConfig()

		Convey("create scheduler", func() {
			scheduler := NewCronScheduler(ctx, config, lh, dma, dda)
			So(scheduler, ShouldNotBeNil)
			So(scheduler.ctx, ShouldNotBeNil)
		})

		Convey("start and stop scheduler", func() {
			// 创建一个禁用所有任务的配置
			config := CronJobConfig{
				ScrubEnabled:      false,
				MergeEnabled:      false,
				DefragmentEnabled: false,
			}

			scheduler := NewCronScheduler(ctx, config, lh, dma, dda)
			err := scheduler.Start()
			So(err, ShouldBeNil)

			// 等待一小段时间
			time.Sleep(100 * time.Millisecond)

			// 停止调度器
			scheduler.Stop()
		})

		Convey("start scheduler with invalid schedule", func() {
			config := CronJobConfig{
				ScrubEnabled:  true,
				ScrubSchedule: "invalid",
			}

			scheduler := NewCronScheduler(ctx, config, lh, dma, dda)
			err := scheduler.Start()
			So(err, ShouldNotBeNil)
		})
	})
}
