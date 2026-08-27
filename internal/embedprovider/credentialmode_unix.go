//go:build !windows

package embedprovider

import (
	"fmt"
	"io/fs"
)

// FilePermissionsEnforced reports whether this build can refuse a credential
// file the filesystem lets others read.
//
// It is a constant a caller reports rather than a detail this package keeps to
// itself: a run record saying the credential came from a file must be able to
// say whether that file's permissions were actually checked, and on a platform
// where they were not, saying nothing reads as saying they were fine
// (stokaro/ptah#2068).
const FilePermissionsEnforced = true

// refuseReadableBeyondOwner refuses a credential file others can read.
//
// The check is on the permission bits rather than on ownership because those
// are what a reader can act on: `chmod 600` is the fix the error names.
func refuseReadableBeyondOwner(path string, info fs.FileInfo) error {
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return fmt.Errorf(
			"credential file %s is mode %04o and readable beyond its owner; chmod 600 it",
			path, mode)
	}
	return nil
}
