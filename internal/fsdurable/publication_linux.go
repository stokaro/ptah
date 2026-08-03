//go:build linux

package fsdurable

import "golang.org/x/sys/unix"

// renameNoReplaceAt fails with EEXIST instead of replacing newName. Measured on
// overlayfs, tmpfs and virtiofs; filesystems that reject the flag surface
// EINVAL and are reported as unsupported rather than retried unconditionally.
func renameNoReplaceAt(dirfd int, oldName, newName string) error {
	return unix.Renameat2(dirfd, oldName, dirfd, newName, unix.RENAME_NOREPLACE)
}

// renameExchangeAt swaps the two entries atomically, which is the only way to
// replace an existing destination while keeping the file that was replaced
// reachable for verification.
func renameExchangeAt(dirfd int, oldName, newName string) error {
	return unix.Renameat2(dirfd, oldName, dirfd, newName, unix.RENAME_EXCHANGE)
}
