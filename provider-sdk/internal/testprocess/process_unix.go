//go:build !windows

// Package testprocess observes fixture process lifetime in integration tests.
package testprocess

import "syscall"

// Running reports whether the fixture process has not exited.
func Running(pid int) bool { return syscall.Kill(pid, 0) == nil }
