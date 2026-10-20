//go:build !windows

package testutils

import "syscall"

// ProcessRunning reports whether the process with pid is still running.
func ProcessRunning(pid int) bool { return syscall.Kill(pid, 0) == nil }
