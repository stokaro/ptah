//go:build windows

// Package testprocess observes fixture process lifetime in integration tests.
package testprocess

import "golang.org/x/sys/windows"

// Running reports whether the fixture process has not exited.
func Running(pid int) bool {
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid)) // #nosec G115 -- PID is read from the fixture process
	if err != nil {
		return false
	}
	defer windows.CloseHandle(handle)
	result, err := windows.WaitForSingleObject(handle, 0)
	return err == nil && result == uint32(windows.WAIT_TIMEOUT)
}
