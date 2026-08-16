//go:build !windows

package fsdurable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// MoveDirNoReplace publishes the directory at oldPath as newPath and refuses an
// entry that already exists there, of any kind. Both paths must be absolute.
//
// MoveFileNoReplace cannot do this: it claims the destination with os.Link,
// and hard links to directories are not permitted.
//
// os.Rename already refuses an existing destination here -- measured on APFS
// and on ext4, a non-empty directory renamed onto an existing empty directory
// failed with EEXIST and left the destination intact. What this adds on Unix is
// therefore not the refusal but where it lives: the destination is bound by the
// move itself rather than by a check that precedes one, so there is no window
// between deciding the path is free and taking it. The platform that had no
// refusal at all is Windows.
//
// A filesystem with no conditional rename reports
// ErrConditionalPublicationUnsupported rather than degrading to rename(2),
// for the reason classifyConditionalRename gives: the fallback is precisely
// the unconditional replacement this package exists to prevent.
func MoveDirNoReplace(oldPath, newPath string) error {
	if !filepath.IsAbs(oldPath) || !filepath.IsAbs(newPath) {
		return fmt.Errorf(
			"move directory without replacing: absolute paths required, got %q and %q",
			oldPath,
			newPath,
		)
	}
	return classifyConditionalRename(
		"renameat-noreplace",
		oldPath,
		newPath,
		withDirFD(filepath.Dir(newPath), func(dirfd int) error {
			return renameNoReplaceAt(dirfd, oldPath, newPath)
		}),
	)
}

// withDirFD runs fn with a descriptor for dir, borrowed under
// SyscallConn.Control so the runtime cannot close it while the syscall is in
// flight -- the same loan withRootDirFD makes, for a directory named by path
// rather than one reached through an os.Root.
//
// The conditional rename takes a directory descriptor even when both of its
// paths are absolute, in which case it ignores the descriptor. Opening the
// parent of the destination rather than passing a borrowed constant keeps the
// argument meaningful: it is the directory the entry is about to appear in,
// and a caller whose parent has gone away learns it here.
func withDirFD(dir string, fn func(dirfd int) error) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	conn, err := handle.SyscallConn()
	if err != nil {
		return errors.Join(err, handle.Close())
	}
	var opErr error
	controlErr := conn.Control(func(fd uintptr) {
		opErr = fn(int(fd))
	})
	closeErr := handle.Close()
	if opErr != nil {
		return errors.Join(opErr, controlErr, closeErr)
	}
	return errors.Join(controlErr, closeErr)
}
