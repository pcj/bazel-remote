// +build windows

package main

import (
	"github.com/buchgr/bazel-remote/cache"
)

// On unix, we need to raise the limit on the number of open files.
// Unsure if anything is required on windows, or if bazel-remote even
// works on windows. But let's not intentionally prevent compiling
// for windows.
func adjustRlimit(logger cache.Logger) {
}
