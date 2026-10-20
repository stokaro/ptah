//go:build windows

package testutils

import "golang.org/x/sys/windows"

// ProcessRunning reports whether the process with pid is still running.
func ProcessRunning(pid int) bool {
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid)) // #nosec G115 -- PID is read from the fixture process
	if err != nil {
		return false
	}
	defer windows.CloseHandle(handle)
	result, err := windows.WaitForSingleObject(handle, 0)
	return err == nil && result == uint32(windows.WAIT_TIMEOUT)
}
