package pathguard

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"go.5x5.cz/ptah/internal/fsdurable"
)

const createTempAttempts = 10_000

// Open opens name through the rooted directory handle.
func (d *OpenedDirectory) Open(name string) (*os.File, error) {
	return d.root.Open(name)
}

// Lstat returns information about name without following its final symlink.
func (d *OpenedDirectory) Lstat(name string) (fs.FileInfo, error) {
	return d.root.Lstat(name)
}

// ReadFile reads name through the rooted directory handle.
func (d *OpenedDirectory) ReadFile(name string) ([]byte, error) {
	return d.root.ReadFile(name)
}

// Remove removes name through the rooted directory handle.
func (d *OpenedDirectory) Remove(name string) error {
	return d.root.Remove(name)
}

// ReadDir lists name through the rooted directory handle. Pass "." for the
// opened directory itself.
func (d *OpenedDirectory) ReadDir(name string) ([]fs.DirEntry, error) {
	return fs.ReadDir(d.root.FS(), name)
}

// OpenFile opens name through the rooted directory handle with the supplied
// flags. The caller owns the returned file and must close it.
func (d *OpenedDirectory) OpenFile(name string, flag int, perm fs.FileMode) (*os.File, error) {
	return d.root.OpenFile(name, flag, perm)
}

// Link creates newName as a hard link to oldName through the rooted directory
// handle. An existing newName is reported through fs.ErrExist, so the link is
// already conditional on the destination being absent.
func (d *OpenedDirectory) Link(oldName, newName string) error {
	return d.root.Link(oldName, newName)
}

// Mkdir creates name as a directory inside the opened directory. An entry that
// already exists is reported through fs.ErrExist.
func (d *OpenedDirectory) Mkdir(name string, perm fs.FileMode) error {
	return d.root.Mkdir(name, perm)
}

// MkdirAll creates path and every missing parent inside the opened directory.
// Path may be spelled absolutely as long as it stays within the handle.
func (d *OpenedDirectory) MkdirAll(path string, perm fs.FileMode) error {
	target, err := rootedRelativePath(path, d.path)
	if err != nil {
		return err
	}
	if target.relative == "." {
		return nil
	}
	return d.root.MkdirAll(target.relative, perm)
}

// CreateTemp creates a new exclusively owned file directly in the opened
// directory. Pattern may contain one or more asterisks; the last is replaced
// with random text. A pattern without an asterisk receives a random suffix.
// The caller owns the returned file and must close it. If the file is not
// published, the caller must also remove the returned relative name.
func (d *OpenedDirectory) CreateTemp(pattern string) (*os.File, string, error) {
	prefix, suffix, err := splitTempPattern(pattern)
	if err != nil {
		return nil, "", err
	}
	for range createTempAttempts {
		name := prefix + rand.Text() + suffix
		file, openErr := d.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(openErr, fs.ErrExist) {
			continue
		}
		if openErr != nil {
			return nil, "", fmt.Errorf("create rooted temporary file %s: %w", name, openErr)
		}
		return file, name, nil
	}
	return nil, "", &fs.PathError{
		Op:   "createtemp",
		Path: pattern,
		Err:  fs.ErrExist,
	}
}

// ReplaceFile atomically replaces newName with oldName through the rooted
// directory handle. On Unix, callers must follow successful replacement with
// Sync. On Windows, replacement flushes the published file and Sync is a no-op.
// Windows requires oldName to identify a writable regular file. An error
// wrapping fsdurable.ErrReplacementCommitted means the rooted rename succeeded,
// so callers must not treat it as a pre-commit failure.
func (d *OpenedDirectory) ReplaceFile(oldName, newName string) error {
	return fsdurable.ReplaceFileAt(d.root, oldName, newName)
}

// PublishFile durably replaces newName with the staged regular file identified
// by expectedInfo from os.Stat or os.File.Stat. The final mode is applied before
// rename. Both names must identify direct children. An error wrapping
// fsdurable.ErrReplacementCommitted means the rename succeeded.
//
// The commit is conditional on dest, so a destination that changed after the
// caller's own validation is reported through fsdurable.ErrDestinationChanged
// and left untouched instead of being overwritten.
func (d *OpenedDirectory) PublishFile(
	oldName, newName string,
	expectedInfo fs.FileInfo,
	finalMode fs.FileMode,
	dest fsdurable.Destination,
) error {
	return fsdurable.PublishFileAt(d.root, oldName, newName, expectedInfo, finalMode, dest)
}

// FinalizeFile applies finalMode to a staged regular file with the supplied
// identity from os.Stat or os.File.Stat and durably preserves it without
// renaming it. Name must identify a direct child.
func (d *OpenedDirectory) FinalizeFile(
	name string,
	expectedInfo fs.FileInfo,
	finalMode fs.FileMode,
) error {
	return fsdurable.FinalizeFileAt(d.root, name, expectedInfo, finalMode)
}

// Revalidate verifies that Path still identifies this opened directory.
func (d *OpenedDirectory) Revalidate() error {
	openedInfo, openedErr := d.root.Stat(".")
	pathInfo, pathErr := os.Stat(d.path)
	if err := errors.Join(openedErr, pathErr); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrDirectoryChanged, d.path, err)
	}
	if !openedInfo.IsDir() ||
		!pathInfo.IsDir() ||
		!os.SameFile(d.info, openedInfo) ||
		!os.SameFile(openedInfo, pathInfo) {
		return fmt.Errorf("%w: %s", ErrDirectoryChanged, d.path)
	}
	return nil
}

// Sync flushes rooted directory-entry changes where the platform supports it.
// Call it after ReplaceFile and Remove before reporting a durable transaction.
func (d *OpenedDirectory) Sync() error {
	return fsdurable.SyncRoot(d.root)
}

func splitTempPattern(pattern string) (prefix, suffix string, err error) {
	if pattern != "" && (filepath.Base(pattern) != pattern || pattern == "." || pattern == "..") {
		return "", "", &fs.PathError{
			Op:   "createtemp",
			Path: pattern,
			Err:  fs.ErrInvalid,
		}
	}
	index := strings.LastIndexByte(pattern, '*')
	if index < 0 {
		return pattern, "", nil
	}
	return pattern[:index], pattern[index+1:], nil
}
