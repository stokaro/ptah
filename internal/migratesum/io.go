package migratesum

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/internal/fsdurable"
	"go.5x5.cz/ptah/migration/migrator"
)

// Write computes the sum of the migrations directory at dir and writes it to
// dir/ptah.sum, returning the computed sum. The ptah.sum file is excluded from
// its own hash because it is not a migration file.
func Write(dir string) (*SumFile, error) {
	return WriteWithFormat(dir, migrator.MigrationDirFormatAuto)
}

// WriteWithFormat computes the sum of the migrations directory at dir using
// format and writes it to the format's integrity file.
func WriteWithFormat(dir string, format migrator.MigrationDirFormat) (*SumFile, error) {
	sum, err := ComputeWithFormat(os.DirFS(dir), format)
	if err != nil {
		return nil, err
	}
	name, err := FileNameForFormat(format)
	if err != nil {
		return nil, err
	}
	if err := writeAtomicSumFile(filepath.Join(dir, name), sum.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write %s: %w", name, err)
	}
	return sum, nil
}

// WritePrecomputedWithFormat writes sum to the integrity file selected by
// format without reading the live migration directory again.
func WritePrecomputedWithFormat(
	dir string,
	format migrator.MigrationDirFormat,
	sum *SumFile,
) error {
	if sum == nil {
		return errors.New("migration checksum must not be nil")
	}
	name, err := FileNameForFormat(format)
	if err != nil {
		return err
	}
	if err := writeAtomicSumFile(filepath.Join(dir, name), sum.Bytes()); err != nil {
		return fmt.Errorf("failed to write %s: %w", name, err)
	}
	return nil
}

// RootedDir is the retained rooted directory handle a durable checksum write
// needs. It is satisfied by pathguard.OpenedDirectory, which this package
// cannot name without depending on it.
type RootedDir interface {
	CreateTemp(pattern string) (*os.File, string, error)
	ReplaceFile(oldName, newName string) error
	Remove(name string) error
	Sync() error
}

// WritePrecomputedWithFormatIn writes sum to the integrity file selected by
// format through a retained rooted directory handle. A directory whose pathname
// is replaced after the caller validated it therefore cannot receive the
// checksum: every name is resolved through the handle that was opened once.
func WritePrecomputedWithFormatIn(
	dir RootedDir,
	format migrator.MigrationDirFormat,
	sum *SumFile,
) error {
	if sum == nil {
		return errors.New("migration checksum must not be nil")
	}
	name, err := FileNameForFormat(format)
	if err != nil {
		return err
	}
	if err := writeAtomicSumFileIn(dir, name, sum.Bytes()); err != nil {
		return fmt.Errorf("failed to write %s: %w", name, err)
	}
	return nil
}

func writeAtomicSumFileIn(dir RootedDir, name string, contents []byte) error {
	file, tempName, err := dir.CreateTemp("." + name + ".*.tmp")
	if err != nil {
		return err
	}

	// The sum file is committed alongside migrations and checked in, so it
	// uses the same 0644 permissions as generated migration files.
	if err := file.Chmod(0644); err != nil {
		return errors.Join(err, file.Close(), removeRootedFile(dir, tempName))
	}
	if _, err := file.Write(contents); err != nil {
		return errors.Join(err, file.Close(), removeRootedFile(dir, tempName))
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close(), removeRootedFile(dir, tempName))
	}
	if err := file.Close(); err != nil {
		return errors.Join(err, removeRootedFile(dir, tempName))
	}
	if err := dir.ReplaceFile(tempName, name); err != nil {
		if errors.Is(err, fsdurable.ErrReplacementCommitted) {
			return &CommitUncertainError{Err: err}
		}
		return errors.Join(err, removeRootedFile(dir, tempName))
	}
	if err := dir.Sync(); err != nil {
		return &CommitUncertainError{Err: err}
	}
	return nil
}

func removeRootedFile(dir RootedDir, name string) error {
	err := dir.Remove(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// CommitUncertainError reports that the checksum file was atomically replaced
// but syncing the containing directory failed. The visible checksum may be the
// old or new commit marker after a crash, so callers must retain recovery state.
type CommitUncertainError struct {
	Err error
}

func (e *CommitUncertainError) Error() string {
	return e.Err.Error()
}

func (e *CommitUncertainError) Unwrap() error {
	return e.Err
}

// IsCommitUncertain reports whether err means the checksum rename succeeded
// but its directory durability could not be confirmed.
func IsCommitUncertain(err error) bool {
	var target *CommitUncertainError
	return errors.As(err, &target)
}

func writeAtomicSumFile(path string, contents []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tempPath := file.Name()

	// The sum file is committed alongside migrations and checked in, so it
	// uses the same 0644 permissions as generated migration files.
	if err := file.Chmod(0644); err != nil {
		return errors.Join(err, file.Close(), removeFile(tempPath))
	}
	if _, err := file.Write(contents); err != nil {
		return errors.Join(err, file.Close(), removeFile(tempPath))
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close(), removeFile(tempPath))
	}
	if err := file.Close(); err != nil {
		return errors.Join(err, removeFile(tempPath))
	}
	if err := fsdurable.ReplaceFile(tempPath, path); err != nil {
		return errors.Join(err, removeFile(tempPath))
	}
	if err := fsdurable.SyncDir(filepath.Dir(path)); err != nil {
		return &CommitUncertainError{Err: err}
	}
	return nil
}

func removeFile(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// VerifyDir verifies the migrations directory at dir against its ptah.sum.
func VerifyDir(dir string) (*Result, error) {
	return VerifyDirWithFormat(dir, migrator.MigrationDirFormatAuto)
}

// VerifyDirWithFormat verifies the migrations directory at dir against the
// selected format's integrity file.
func VerifyDirWithFormat(dir string, format migrator.MigrationDirFormat) (*Result, error) {
	return VerifyWithFormat(os.DirFS(dir), format)
}
