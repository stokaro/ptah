//go:build windows

package fsdurable

import (
	"fmt"
	"path/filepath"
)

// MoveDirNoReplace publishes the directory at oldPath as newPath and refuses an
// entry that already exists there, of any kind. Both paths must be absolute.
//
// On Windows this is the file move: MoveFileEx moves a directory and a file
// alike, and without MOVEFILE_REPLACE_EXISTING it refuses a destination of
// either kind. The two differ only away from Windows, where MoveFileNoReplace
// claims the destination with a hard link and directories cannot be linked.
//
// What it replaces at the call site is os.Rename, which refuses an existing
// destination on Unix and was observed not to refuse one here: the OCI install
// test that expects a refusal got a nil error from the ptah-go-lint-windows
// job (stokaro/ptah#1547). Asking MoveFileEx not to replace makes the refusal
// explicit rather than inherited from whatever os.Rename maps to.
func MoveDirNoReplace(oldPath, newPath string) error {
	if !filepath.IsAbs(oldPath) || !filepath.IsAbs(newPath) {
		return fmt.Errorf(
			"move directory without replacing: absolute paths required, got %q and %q",
			oldPath,
			newPath,
		)
	}
	return MoveFileNoReplace(oldPath, newPath)
}
