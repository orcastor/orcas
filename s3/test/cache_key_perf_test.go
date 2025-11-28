package s3_test

import (
	"testing"

	"github.com/orcastor/orcas/s3/util"
)

// BenchmarkCacheKeyFormatting benchmarks cache key formatting functions
func TestCacheKeyFormatting(t *testing.T) {
	testID1 := int64(1234567890123456789)
	testID2 := int64(9876543210987654)

	b.Run("FormatCacheKeyInt", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = util.FormatCacheKeyInt(testID1, testID2)
		}
	})

	b.Run("FormatCacheKeySingleInt", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = util.FormatCacheKeySingleInt(testID1)
		}
	})

	b.Run("FastSplitPath", func(b *testing.B) {
		testPaths := []string{
			"/",
			"/file.txt",
			"/dir1/file.txt",
			"/dir1/dir2/dir3/file.txt",
			"dir1/dir2/file.txt",
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			path := testPaths[i%len(testPaths)]
			_ = util.FastSplitPath(path)
		}
	})

	b.Run("FastBase", func(b *testing.B) {
		testPaths := []string{
			"/file.txt",
			"/dir1/file.txt",
			"/dir1/dir2/file.txt",
			"file.txt",
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			path := testPaths[i%len(testPaths)]
			_ = util.FastBase(path)
		}
	})

	b.Run("FastDir", func(b *testing.B) {
		testPaths := []string{
			"/file.txt",
			"/dir1/file.txt",
			"/dir1/dir2/file.txt",
			"file.txt",
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			path := testPaths[i%len(testPaths)]
			_ = util.FastDir(path)
		}
	})

	b.Run("FastTrimPrefix", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = util.FastTrimPrefix("/test/path", "/")
		}
	})
}
