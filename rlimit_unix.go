// +build !darwin
// +build !windows

package main

import (
	"syscall"

	"github.com/buchgr/bazel-remote/cache"
)

// Raise the limit on the number of open files.
func adjustRlimit(logger cache.Logger) {
	var limits syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limits)
	if err != nil {
		logger.Printf("Failed to find rlimit from getrlimit: %v", err)
		return
	}

	logger.Printf("Initial RLIMIT_NOFILE cur: %d max: %d",
		limits.Cur, limits.Max)

	limits.Cur = limits.Max

	logger.Printf("Setting RLIMIT_NOFILE cur: %d max: %d",
		limits.Cur, limits.Max)

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limits)
	if err != nil {
		logger.Printf("Failed to set rlimit: %v", err)
		return
	}

	return
}
