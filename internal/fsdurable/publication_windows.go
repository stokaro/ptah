package fsdurable

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const fileRenameInformationExClass = 65

type fileRenameInformationEx struct {
	flags          uint32
	rootDirectory  windows.Handle
	fileNameLength uint32
	fileName       [1]uint16
}

func openPublicationFile(root *os.Root, name string) (*os.File, error) {
	return openRootedHandle(
		root,
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.DELETE,
	)
}

// openPublicationDestination opens the entry a publication intends to replace.
// It requests only what the conditional commit needs, because a destination
// carrying the read-only attribute cannot be opened for writing and still has
// to be verifiable and movable.
func openPublicationDestination(root *os.Root, name string) (*os.File, error) {
	return openRootedHandle(
		root,
		name,
		windows.DELETE|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
	)
}

func openRootedHandle(root *os.Root, name string, access uint32) (*os.File, error) {
	rootDir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, errors.Join(err, rootDir.Close())
	}
	attributes := &windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: windows.Handle(rootDir.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	var (
		handle         windows.Handle
		ioStatus       windows.IO_STATUS_BLOCK
		allocationSize int64
	)
	openErr := windows.NtCreateFile(
		&handle,
		access,
		attributes,
		&ioStatus,
		&allocationSize,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	)
	closeRootErr := rootDir.Close()
	if openErr != nil {
		return nil, errors.Join(
			&fs.PathError{Op: "openat", Path: name, Err: mapNTStatus(openErr)},
			closeRootErr,
		)
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		return nil, errors.Join(
			fmt.Errorf("create os file for rooted publication %s", name),
			windows.CloseHandle(handle),
			closeRootErr,
		)
	}
	if closeRootErr != nil {
		return nil, errors.Join(closeRootErr, file.Close())
	}
	return file, nil
}

// commitPublicationFile publishes the staged entry over targetName only while
// the target still holds dest.
//
// Windows has no exchange primitive, but it renames by handle rather than by
// name, which supplies the same guarantee from the other direction: the
// expected destination is opened, verified, and then moved aside through that
// handle, so the move can only ever act on the object that was verified. The
// staged file is then published with no-replace semantics, which fails closed
// if anyone reached the freed name first.
func commitPublicationFile(
	root *os.Root,
	staged *os.File,
	stagedName, targetName string,
	dest Destination,
	hooks publicationHooks,
) (bool, error) {
	if dest.kind == destinationAbsent {
		hooks.runBeforeCommit()
		if err := renameFileHandle(root, staged, stagedName, targetName); err != nil {
			return false, classifyWindowsPublicationError(targetName, err)
		}
		return true, nil
	}
	return replaceVerifiedDestination(root, staged, stagedName, targetName, dest, hooks)
}

func replaceVerifiedDestination(
	root *os.Root,
	staged *os.File,
	stagedName, targetName string,
	dest Destination,
	hooks publicationHooks,
) (bool, error) {
	destination, err := openPublicationDestination(root, targetName)
	if err != nil {
		return false, destinationChangedError(targetName, err)
	}
	destinationInfo, statErr := destination.Stat()
	if statErr != nil || !dest.matches(destinationInfo) {
		return false, errors.Join(
			destinationChangedError(targetName, errors.Join(errDisplacedDestination, statErr)),
			destination.Close(),
		)
	}
	hooks.runBeforeCommit()
	displacedName := targetName + ".ptah-displaced-" + rand.Text()
	if err := renameFileHandle(root, destination, targetName, displacedName); err != nil {
		return false, errors.Join(
			classifyWindowsPublicationError(targetName, err),
			destination.Close(),
		)
	}
	publishErr := renameFileHandle(root, staged, stagedName, targetName)
	if publishErr == nil {
		return true, errors.Join(destination.Close(), removeDisplacedFile(root, displacedName))
	}
	restoreErr := renameFileHandle(root, destination, displacedName, targetName)
	if restoreErr == nil {
		return false, errors.Join(
			classifyWindowsPublicationError(targetName, publishErr),
			destination.Close(),
		)
	}
	closeErr := destination.Close()
	recovery, preserveErr := preserveDisplacedFile(root, displacedName, targetName)
	return false, fmt.Errorf(
		"%w: %s: the displaced destination could not be restored and is preserved at %s: %w",
		ErrDestinationChanged,
		targetName,
		recovery,
		errors.Join(errDisplacedDestination, publishErr, restoreErr, preserveErr, closeErr),
	)
}

// renameFileHandle renames the object behind file to newName inside root
// without replacing an existing entry. Replacement is never requested: every
// destination this package replaces is moved aside first, so a collision here
// always means someone else reached the name.
func renameFileHandle(root *os.Root, file *os.File, oldName, newName string) error {
	rootDir, err := root.Open(".")
	if err != nil {
		return err
	}
	targetUTF16, err := windows.UTF16FromString(newName)
	if err != nil {
		return errors.Join(
			&os.LinkError{Op: "renameat", Old: oldName, New: newName, Err: err},
			rootDir.Close(),
		)
	}
	nameLength := (len(targetUTF16) - 1) * 2
	var layout fileRenameInformationEx
	bufferSize := int(unsafe.Offsetof(layout.fileName)) + nameLength
	buffer := make([]byte, bufferSize)
	info := (*fileRenameInformationEx)(unsafe.Pointer(&buffer[0]))
	info.flags = windows.FILE_RENAME_POSIX_SEMANTICS |
		windows.FILE_RENAME_IGNORE_READONLY_ATTRIBUTE
	info.rootDirectory = windows.Handle(rootDir.Fd())
	info.fileNameLength = uint32(nameLength)
	copy(
		unsafe.Slice(&info.fileName[0], nameLength/2),
		targetUTF16[:len(targetUTF16)-1],
	)
	var ioStatus windows.IO_STATUS_BLOCK
	renameErr := windows.NtSetInformationFile(
		windows.Handle(file.Fd()),
		&ioStatus,
		&buffer[0],
		uint32(bufferSize),
		fileRenameInformationExClass,
	)
	closeErr := rootDir.Close()
	if renameErr != nil {
		return errors.Join(
			&os.LinkError{
				Op:  "renameat",
				Old: oldName,
				New: newName,
				Err: mapNTStatus(renameErr),
			},
			closeErr,
		)
	}
	return closeErr
}

func rootRenameNoReplace(root *os.Root, oldName, newName string) error {
	file, err := openPublicationDestination(root, oldName)
	if err != nil {
		return err
	}
	return errors.Join(renameFileHandle(root, file, oldName, newName), file.Close())
}

func classifyWindowsPublicationError(targetName string, err error) error {
	if errors.Is(err, fs.ErrExist) {
		return destinationChangedError(targetName, err)
	}
	if errors.Is(err, errConditionalRenameUnavailable) {
		return unsupportedPublicationError(targetName, err)
	}
	return err
}

// mapNTStatus turns the NT statuses this package acts on into the portable
// io/fs sentinels, so callers can match a lost race the same way on every
// platform.
func mapNTStatus(err error) error {
	var status windows.NTStatus
	if !errors.As(err, &status) {
		return err
	}
	switch status {
	case windows.STATUS_OBJECT_NAME_COLLISION:
		return fmt.Errorf("%w: %w", fs.ErrExist, err)
	case windows.STATUS_OBJECT_NAME_NOT_FOUND, windows.STATUS_OBJECT_PATH_NOT_FOUND:
		return fmt.Errorf("%w: %w", fs.ErrNotExist, err)
	case windows.STATUS_NOT_SUPPORTED,
		windows.STATUS_INVALID_PARAMETER,
		windows.STATUS_INVALID_INFO_CLASS,
		windows.STATUS_INVALID_DEVICE_REQUEST:
		return fmt.Errorf("%w: %w", errConditionalRenameUnavailable, err)
	}
	return err
}

func modesEqual(actual, expected fs.FileMode) bool {
	return actual&0o200 == expected&0o200
}
