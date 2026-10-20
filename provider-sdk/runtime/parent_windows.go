package runtime

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// watchParent holds the actual process handle, so PID reuse cannot attach the
// provider to an unrelated process after its host exits.
func watchParent() (func(), error) {
	pid, err := parentPID()
	if err != nil {
		return nil, err
	}
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid)) // #nosec G115 -- a Windows process ID is a DWORD supplied by the parent
	if err != nil {
		return nil, fmt.Errorf("open provider parent handle: %w", err)
	}
	done := make(chan struct{})
	go func() {
		defer windows.CloseHandle(handle)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				result, waitErr := windows.WaitForSingleObject(handle, 0)
				if waitErr != nil || result != uint32(windows.WAIT_TIMEOUT) {
					os.Exit(1) //revive:disable-line:deep-exit the provider process must not survive its host
				}
			}
		}
	}()
	return func() { close(done) }, nil
}
