package core

import (
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEnvironmentConfig(t *testing.T) {
	Convey("Environment Variable Configuration", t, func() {
		// 保存原始环境变量
		originalEnv := make(map[string]string)
		envVars := []string{
			"ORCAS_DELETE_DELAY",
			"ORCAS_BATCH_INTERVAL_MS",
			"ORCAS_MAX_DURATION_SEC",
			"ORCAS_MAX_ITEMS_PER_SEC",
			"ORCAS_ADAPTIVE_DELAY",
			"ORCAS_ADAPTIVE_DELAY_FACTOR",
			"ORCAS_MIN_VERSION_INTERVAL_SEC",
			"ORCAS_MAX_VERSIONS",
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

		Convey("DeleteDelaySeconds", func() {
			// 测试默认值
			os.Unsetenv("ORCAS_DELETE_DELAY")
			// 注意：DeleteDelaySeconds是包级别变量，在init时初始化
			// 所以这里只能测试设置了环境变量后的行为
			So(DeleteDelaySeconds, ShouldBeGreaterThan, 0)
		})

		Convey("ResourceControlConfig", func() {
			Convey("default values", func() {
				config := GetResourceControlConfig()
				So(config.BatchInterval, ShouldEqual, 100*time.Millisecond)
				So(config.MaxDuration, ShouldEqual, 0)
				So(config.MaxItemsPerSecond, ShouldEqual, 0)
				So(config.AdaptiveDelay, ShouldBeTrue)
				So(config.AdaptiveDelayFactor, ShouldEqual, 1000)
			})

			Convey("custom batch interval", func() {
				os.Setenv("ORCAS_BATCH_INTERVAL_MS", "200")
				config := GetResourceControlConfig()
				So(config.BatchInterval, ShouldEqual, 200*time.Millisecond)
				os.Unsetenv("ORCAS_BATCH_INTERVAL_MS")
			})

			Convey("custom max duration", func() {
				os.Setenv("ORCAS_MAX_DURATION_SEC", "3600")
				config := GetResourceControlConfig()
				So(config.MaxDuration, ShouldEqual, 3600*time.Second)
				os.Unsetenv("ORCAS_MAX_DURATION_SEC")
			})

			Convey("custom max items per second", func() {
				os.Setenv("ORCAS_MAX_ITEMS_PER_SEC", "1000")
				config := GetResourceControlConfig()
				So(config.MaxItemsPerSecond, ShouldEqual, 1000)
				os.Unsetenv("ORCAS_MAX_ITEMS_PER_SEC")
			})

			Convey("custom adaptive delay", func() {
				os.Setenv("ORCAS_ADAPTIVE_DELAY", "false")
				config := GetResourceControlConfig()
				So(config.AdaptiveDelay, ShouldBeFalse)
				os.Unsetenv("ORCAS_ADAPTIVE_DELAY")

				os.Setenv("ORCAS_ADAPTIVE_DELAY", "1")
				config = GetResourceControlConfig()
				So(config.AdaptiveDelay, ShouldBeTrue)
				os.Unsetenv("ORCAS_ADAPTIVE_DELAY")
			})

			Convey("custom adaptive delay factor", func() {
				os.Setenv("ORCAS_ADAPTIVE_DELAY_FACTOR", "2000")
				config := GetResourceControlConfig()
				So(config.AdaptiveDelayFactor, ShouldEqual, 2000)
				os.Unsetenv("ORCAS_ADAPTIVE_DELAY_FACTOR")
			})

			Convey("invalid values should use defaults", func() {
				os.Setenv("ORCAS_BATCH_INTERVAL_MS", "invalid")
				config := GetResourceControlConfig()
				So(config.BatchInterval, ShouldEqual, 100*time.Millisecond)
				os.Unsetenv("ORCAS_BATCH_INTERVAL_MS")
			})
		})

		Convey("VersionRetentionConfig", func() {
			Convey("default values", func() {
				config := GetVersionRetentionConfig()
				So(config.MinVersionInterval, ShouldEqual, 0)
				So(config.MaxVersions, ShouldEqual, 0)
			})

			Convey("custom min version interval", func() {
				os.Setenv("ORCAS_MIN_VERSION_INTERVAL_SEC", "300")
				config := GetVersionRetentionConfig()
				So(config.MinVersionInterval, ShouldEqual, 300)
				os.Unsetenv("ORCAS_MIN_VERSION_INTERVAL_SEC")
			})

			Convey("custom max versions", func() {
				os.Setenv("ORCAS_MAX_VERSIONS", "10")
				config := GetVersionRetentionConfig()
				So(config.MaxVersions, ShouldEqual, 10)
				os.Unsetenv("ORCAS_MAX_VERSIONS")
			})

			Convey("zero values are allowed", func() {
				os.Setenv("ORCAS_MIN_VERSION_INTERVAL_SEC", "0")
				os.Setenv("ORCAS_MAX_VERSIONS", "0")
				config := GetVersionRetentionConfig()
				So(config.MinVersionInterval, ShouldEqual, 0)
				So(config.MaxVersions, ShouldEqual, 0)
				os.Unsetenv("ORCAS_MIN_VERSION_INTERVAL_SEC")
				os.Unsetenv("ORCAS_MAX_VERSIONS")
			})
		})
	})
}

func TestWorkLock(t *testing.T) {
	Convey("Work Lock", t, func() {
		Convey("acquire and release lock", func() {
			key := "test_lock_1"
			acquired, release := acquireWorkLock(key)
			So(acquired, ShouldBeTrue)
			So(release, ShouldNotBeNil)

			// 释放锁
			release()

			// 再次获取应该成功
			acquired2, release2 := acquireWorkLock(key)
			So(acquired2, ShouldBeTrue)
			So(release2, ShouldNotBeNil)
			release2()
		})

		Convey("concurrent access to same key", func() {
			key := "test_lock_2"
			done := make(chan bool, 2)
			resultChan := make(chan bool, 1)

			// 第一个goroutine获取锁
			acquired1, release1 := acquireWorkLock(key)
			So(acquired1, ShouldBeTrue)

			// 第二个goroutine尝试获取同一把锁
			go func() {
				acquired2, release2 := acquireWorkLock(key)
				// 应该等待第一个完成后再获取
				resultChan <- acquired2
				if release2 != nil {
					release2()
				}
				done <- true
			}()

			// 等待一小段时间，确保第二个goroutine开始等待
			time.Sleep(50 * time.Millisecond)

			// 释放第一个锁
			release1()

			// 等待第二个goroutine完成
			<-done
			acquired2 := <-resultChan
			So(acquired2, ShouldBeTrue)
		})

		Convey("different keys can be acquired concurrently", func() {
			key1 := "test_lock_3"
			key2 := "test_lock_4"

			acquired1, release1 := acquireWorkLock(key1)
			So(acquired1, ShouldBeTrue)

			acquired2, release2 := acquireWorkLock(key2)
			So(acquired2, ShouldBeTrue)

			// 两个不同的key应该可以同时持有锁
			release1()
			release2()
		})

		Convey("multiple concurrent attempts on same key", func() {
			key := "test_lock_5"
			results := make(chan bool, 5)
			done := make(chan bool, 4)

			// 第一个获取锁
			acquired1, release1 := acquireWorkLock(key)
			So(acquired1, ShouldBeTrue)

			// 启动多个goroutine尝试获取同一把锁
			for i := 0; i < 4; i++ {
				go func() {
					acquired, release := acquireWorkLock(key)
					results <- acquired
					if release != nil {
						release()
					}
					done <- true
				}()
			}

			// 等待一小段时间
			time.Sleep(100 * time.Millisecond)

			// 释放第一个锁
			release1()

			// 等待所有goroutine完成
			for i := 0; i < 4; i++ {
				<-done
			}

			// 收集结果
			acquiredCount := 0
			for i := 0; i < 4; i++ {
				if <-results {
					acquiredCount++
				}
			}

			// 至少有一个应该成功获取锁
			So(acquiredCount, ShouldBeGreaterThan, 0)
		})
	})
}
