package fsdurable

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// PublishFileAt durably replaces targetName with the staged regular file
// identified by stagedInfo from an os stat operation. It applies finalMode and
// syncs the staged file before rename, then verifies the same filesystem object
// at targetName and syncs both the published file and its parent. Names must
// identify direct children of root. Errors after rename wrap
// ErrReplacementCommitted. FinalMode follows os.Chmod semantics; on Windows,
// its owner-write bit controls the read-only attribute.
//
// The publication is conditional on dest: the commit primitive itself binds the
// state the caller expects at targetName, so a destination that changed after
// the caller's own checks is reported through ErrDestinationChanged and left
// byte-intact rather than replaced. A caller cannot opt out; the zero
// Destination is rejected. Platforms and filesystems without a conditional
// rename primitive fail with ErrConditionalPublicationUnsupported instead of
// degrading to an unconditional rename.
func PublishFileAt(
	root *os.Root,
	stagedName, targetName string,
	stagedInfo fs.FileInfo,
	finalMode fs.FileMode,
	dest Destination,
) error {
	return publishFileAt(
		root,
		stagedName,
		targetName,
		stagedInfo,
		finalMode,
		dest,
		publicationHooks{},
	)
}

// publicationHooks injects deterministic waits into the commit sequence. The
// window this package closes is a single syscall wide, so the tests that prove
// it cannot reach it by polling from another goroutine.
type publicationHooks struct {
	beforeCommit func()
	afterCommit  func()
}

func (h publicationHooks) runBeforeCommit() {
	if h.beforeCommit != nil {
		h.beforeCommit()
	}
}

func (h publicationHooks) runAfterCommit() {
	if h.afterCommit != nil {
		h.afterCommit()
	}
}

func publishFileAt(
	root *os.Root,
	stagedName, targetName string,
	stagedInfo fs.FileInfo,
	finalMode fs.FileMode,
	dest Destination,
	hooks publicationHooks,
) error {
	if err := validateDirectChildName("publishat", stagedName); err != nil {
		return err
	}
	if err := validateDirectChildName("publishat", targetName); err != nil {
		return err
	}
	if err := dest.validate(targetName); err != nil {
		return err
	}
	staged, err := openExpectedWritableFile(root, stagedName, stagedInfo)
	if err != nil {
		return err
	}
	if err := prepareOpenedFile(root, staged, stagedName, stagedInfo, finalMode); err != nil {
		return errors.Join(err, staged.Close())
	}
	renamed, renameErr := commitPublicationFile(root, staged, stagedName, targetName, dest, hooks)
	if !renamed {
		return errors.Join(renameErr, staged.Close())
	}
	hooks.runAfterCommit()

	publishedInfo, verifyErr := root.Lstat(targetName)
	if verifyErr != nil {
		verifyErr = fmt.Errorf("lstat published file after rooted publication %s: %w", targetName, verifyErr)
	} else {
		verifyErr = validateExpectedFile(publishedInfo, stagedInfo, finalMode)
	}
	syncFileErr := staged.Sync()
	syncRootErr := SyncRoot(root)
	closeErr := staged.Close()
	return replacementCommittedError(errors.Join(
		renameErr,
		verifyErr,
		syncFileErr,
		syncRootErr,
		closeErr,
	))
}

// FinalizeFileAt applies finalMode to a staged regular file whose identity was
// captured by the caller through an os.Stat or os.File.Stat operation. It syncs
// the file and its parent directory. Name must identify a direct child of root.
// Unlike PublishFileAt, it does not rename the entry. FinalMode follows
// os.Chmod semantics.
func FinalizeFileAt(
	root *os.Root,
	name string,
	expectedInfo fs.FileInfo,
	finalMode fs.FileMode,
) error {
	if err := validateDirectChildName("finalizeat", name); err != nil {
		return err
	}
	file, err := openExpectedWritableFile(root, name, expectedInfo)
	if err != nil {
		return err
	}
	prepareErr := prepareOpenedFile(root, file, name, expectedInfo, finalMode)
	syncRootErr := error(nil)
	if prepareErr == nil {
		syncRootErr = SyncRoot(root)
	}
	return errors.Join(prepareErr, syncRootErr, file.Close())
}

func openExpectedWritableFile(
	root *os.Root,
	name string,
	expectedInfo fs.FileInfo,
) (*os.File, error) {
	if expectedInfo == nil {
		return nil, fmt.Errorf("%w: expected identity is nil", ErrStagedFileChanged)
	}
	entryInfo, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if err := validateExpectedFile(entryInfo, expectedInfo, expectedInfo.Mode()); err != nil {
		return nil, fmt.Errorf("validate staged file %s: %w", name, err)
	}
	file, err := openPublicationFile(root, name)
	if err != nil {
		return nil, err
	}
	openedInfo, statErr := file.Stat()
	restatedInfo, restatErr := root.Lstat(name)
	validationErr := errors.Join(statErr, restatErr)
	if validationErr == nil {
		validationErr = validateExpectedFile(openedInfo, expectedInfo, expectedInfo.Mode())
	}
	if validationErr == nil && !os.SameFile(openedInfo, restatedInfo) {
		validationErr = fmt.Errorf("%w: %s", ErrStagedFileChanged, name)
	}
	if validationErr != nil {
		return nil, errors.Join(validationErr, file.Close())
	}
	return file, nil
}

func prepareOpenedFile(
	root *os.Root,
	file *os.File,
	name string,
	expectedInfo fs.FileInfo,
	finalMode fs.FileMode,
) error {
	if err := file.Chmod(finalMode); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	openedInfo, statErr := file.Stat()
	entryInfo, entryErr := root.Lstat(name)
	validationErr := errors.Join(statErr, entryErr)
	if validationErr == nil {
		validationErr = validateExpectedFile(openedInfo, expectedInfo, finalMode)
	}
	if validationErr == nil {
		validationErr = validateExpectedFile(entryInfo, expectedInfo, finalMode)
	}
	if validationErr == nil && !os.SameFile(openedInfo, entryInfo) {
		validationErr = fmt.Errorf("%w: %s", ErrStagedFileChanged, name)
	}
	return validationErr
}

func validateExpectedFile(
	actual, expected fs.FileInfo,
	expectedMode fs.FileMode,
) error {
	if actual == nil ||
		!actual.Mode().IsRegular() ||
		!os.SameFile(actual, expected) ||
		!modesEqual(actual.Mode(), expectedMode) ||
		actual.Size() != expected.Size() ||
		!actual.ModTime().Equal(expected.ModTime()) {
		return ErrStagedFileChanged
	}
	return nil
}

func validateDirectChildName(op, name string) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		return &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	return nil
}
