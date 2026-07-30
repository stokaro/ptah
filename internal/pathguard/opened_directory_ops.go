package pathguard

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/internal/fsdurable"
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
