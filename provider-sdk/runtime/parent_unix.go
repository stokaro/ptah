//go:build !windows

package runtime

import (
	"fmt"
	"os"
	"time"
)

// watchParent also covers a host killed without running its deferred cleanup.
// Reparenting cannot be undone by PID reuse, unlike checking whether that PID
// happens to exist. The bounded poll does not carry database state.
func watchParent() (func(), error) {
	pid, err := parentPID()
	if err != nil {
		return nil, err
	}
	if os.Getppid() != pid {
		return nil, fmt.Errorf("provider parent exited before startup")
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if os.Getppid() != pid {
					os.Exit(1) //revive:disable-line:deep-exit the provider process must not survive its host
				}
			}
		}
	}()
	return func() { close(done) }, nil
}
