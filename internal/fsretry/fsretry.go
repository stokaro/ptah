// Package fsretry retries a file replacement that a platform refuses only
// while another handle holds the file.
//
// Windows refuses to replace a file that is open without delete sharing,
// which is how Go opens every file there, with ERROR_ACCESS_DENIED or
// ERROR_SHARING_VIOLATION. A reader holding the file for a moment is enough:
// eight parallel provider installs, each reading the install record before it
// writes its own, failed on windows-latest with "rename ...: Access is denied".
// Every other system replaces an open file. The replacement succeeds once the
// reader lets go, so it is retried for a bounded time rather than reported.
package fsretry

import (
	"time"
)

// Replace runs replace until it succeeds, until it fails with an error
// transient does not accept, or until window has passed since the first
// attempt, and returns its last error. The waits between attempts start at a
// millisecond and double up to 50 milliseconds.
func Replace(replace func() error, transient func(error) bool, window time.Duration) error {
	deadline := time.Now().Add(window)
	wait := time.Millisecond
	for {
		err := replace()
		if err == nil || !transient(err) || !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(wait)
		wait = min(2*wait, 50*time.Millisecond)
	}
}
