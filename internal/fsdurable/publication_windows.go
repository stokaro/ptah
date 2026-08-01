package fsdurable

import (
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
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.DELETE,
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
			&fs.PathError{Op: "openat", Path: name, Err: openErr},
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

func renamePublicationFile(
	root *os.Root,
	staged *os.File,
	stagedName string,
	targetName string,
) (bool, error) {
	rootDir, err := root.Open(".")
	if err != nil {
		return false, err
	}
	targetUTF16, err := windows.UTF16FromString(targetName)
	if err != nil {
		return false, errors.Join(
			&os.LinkError{Op: "renameat", Old: stagedName, New: targetName, Err: err},
			rootDir.Close(),
		)
	}
	nameLength := (len(targetUTF16) - 1) * 2
	var layout fileRenameInformationEx
	bufferSize := int(unsafe.Offsetof(layout.fileName)) + nameLength
	buffer := make([]byte, bufferSize)
	info := (*fileRenameInformationEx)(unsafe.Pointer(&buffer[0]))
	info.flags = windows.FILE_RENAME_REPLACE_IF_EXISTS |
		windows.FILE_RENAME_POSIX_SEMANTICS |
		windows.FILE_RENAME_IGNORE_READONLY_ATTRIBUTE
	info.rootDirectory = windows.Handle(rootDir.Fd())
	info.fileNameLength = uint32(nameLength)
	copy(
		unsafe.Slice(&info.fileName[0], nameLength/2),
		targetUTF16[:len(targetUTF16)-1],
	)
	var ioStatus windows.IO_STATUS_BLOCK
	renameErr := windows.NtSetInformationFile(
		windows.Handle(staged.Fd()),
		&ioStatus,
		&buffer[0],
		uint32(bufferSize),
		fileRenameInformationExClass,
	)
	if renameErr != nil {
		return false, errors.Join(
			&os.LinkError{
				Op:  "renameat",
				Old: stagedName,
				New: targetName,
				Err: renameErr,
			},
			rootDir.Close(),
		)
	}
	return true, rootDir.Close()
}

func modesEqual(actual, expected fs.FileMode) bool {
	return actual&0o200 == expected&0o200
}
