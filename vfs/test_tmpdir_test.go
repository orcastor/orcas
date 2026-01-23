package vfs

import (
	"os"
	"path/filepath"
)

// testTmpDir returns a deterministic temp directory rooted at /tmp by default.
//
// macOS os.TempDir() points to /var/folders/... which can be harder to inspect and
// sometimes accumulates large test artifacts. For these tests we prefer /tmp.
//
// Override base path via ORCAS_TEST_TMPDIR if needed.
func testTmpDir(name string) string {
	base := os.Getenv("ORCAS_TEST_TMPDIR")
	if base == "" {
		base = "/tmp"
	}
	return filepath.Join(base, name)
}

