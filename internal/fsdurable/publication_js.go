//go:build js

package fsdurable

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"syscall"
)

// js/wasm has no renameat(2) and therefore neither of the flags the rest of
// this package publishes with. What it does have instead is the reason those
// flags exist: a wasm instance is a single scheduler over a filesystem no
// other process can reach, so nothing can change an entry between the check
// and the rename. The conditional renames are composed from ordinary
// operations here, and the guarantee they are supposed to give -- never
// replace a destination the caller did not expect -- still holds.
//
// This is the same argument internal/devlock and internal/atlasmigrate make
// for their js locks. It stops being true the moment two instances share one
// filesystem, which is why it is confined to this platform.

func rootRenameNoReplace(root *os.Root, oldName, newName string) error {
	if _, err := root.Lstat(newName); err == nil {
		return &os.LinkError{Op: "renameat-noreplace", Old: oldName, New: newName, Err: syscall.EEXIST}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return classifyConditionalRename("renameat-noreplace", oldName, newName, err)
	}
	return classifyConditionalRename("renameat-noreplace", oldName, newName,
		root.Rename(oldName, newName))
}

// rootRenameExchange swaps the two entries: after it returns, newName holds
// what oldName held and oldName holds what newName held. Three renames through
// a scratch name, which is atomic enough on a filesystem with one user; a
// failure part-way leaves the scratch name behind rather than either entry
// silently gone, and says where it is.
func rootRenameExchange(root *os.Root, oldName, newName string) error {
	const op = "renameat-exchange"
	if _, err := root.Lstat(newName); err != nil {
		return classifyConditionalRename(op, oldName, newName, err)
	}
	scratch := fmt.Sprintf("%s.ptah-exchange-%08x", newName, rand.Uint32())
	if err := rootRenameNoReplace(root, newName, scratch); err != nil {
		return classifyConditionalRename(op, oldName, newName, err)
	}
	if err := root.Rename(oldName, newName); err != nil {
		// Put the destination back before reporting, so a failed exchange
		// leaves the tree as it was found.
		return classifyConditionalRename(op, oldName, newName,
			errors.Join(err, root.Rename(scratch, newName)))
	}
	if err := root.Rename(scratch, oldName); err != nil {
		return &os.LinkError{
			Op:  op,
			Old: oldName,
			New: newName,
			Err: fmt.Errorf("the displaced destination is preserved at %s: %w", scratch, err),
		}
	}
	return nil
}
