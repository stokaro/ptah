//go:build windows

package embedprovider

import (
	"io/fs"
)

// FilePermissionsEnforced reports whether this build can refuse a credential
// file the filesystem lets others read.
//
// It is false here. Windows does not carry POSIX permission bits, and what Go
// reports for a file on it is a fixed 0666 -- or 0444 for a read-only one --
// derived from the read-only attribute rather than from any access control.
// Access is decided by an ACL the standard library does not expose, so a check
// over those bits would refuse every credential file on the platform while
// measuring nothing about who can read it.
//
// Reporting that is the point. A run record saying the credential came from a
// file must be able to say whether its permissions were checked, and saying
// nothing reads as saying they were fine.
const FilePermissionsEnforced = false

// refuseReadableBeyondOwner cannot decide this on Windows, so it decides
// nothing.
func refuseReadableBeyondOwner(_ string, _ fs.FileInfo) error {
	return nil
}
