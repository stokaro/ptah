//go:build integration && !windows

package providercli_test

import "syscall"

func providerProcessRunning(pid int) bool { return syscall.Kill(pid, 0) == nil }
