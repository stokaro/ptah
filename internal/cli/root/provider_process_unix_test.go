//go:build integration && !windows

package root_test

import "syscall"

func providerProcessRunning(pid int) bool { return syscall.Kill(pid, 0) == nil }
