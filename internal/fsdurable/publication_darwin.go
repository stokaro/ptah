//go:build darwin

package fsdurable

import "golang.org/x/sys/unix"

// renameNoReplaceAt fails with EEXIST instead of replacing newName. Measured on
// APFS and on a FAT32 volume, where plain rename(2) silently replaced the
// destination and os.Link is not supported at all.
func renameNoReplaceAt(dirfd int, oldName, newName string) error {
	return unix.RenameatxNp(dirfd, oldName, dirfd, newName, unix.RENAME_EXCL)
}

// renameExchangeAt swaps the two entries atomically, which is the only way to
// replace an existing destination while keeping the file that was replaced
// reachable for verification.
func renameExchangeAt(dirfd int, oldName, newName string) error {
	return unix.RenameatxNp(dirfd, oldName, dirfd, newName, unix.RENAME_SWAP)
}
