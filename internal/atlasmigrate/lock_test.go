package atlasmigrate_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrate"
)

func TestRecoverPendingPublication_ReusesHeldDirectoryLock(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	dir := c.TempDir()

	err := atlasmigrate.WithMigrationDirectoryLock(ctx, dir, 0, func(lockedCtx context.Context) error {
		return atlasmigrate.RecoverPendingPublication(lockedCtx, dir)
	})

	c.Assert(err, qt.IsNil)
}

func TestRecoverPendingPublication_DoesNotReuseReleasedDirectoryLock(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	var releasedCtx context.Context
	err := atlasmigrate.WithMigrationDirectoryLock(
		t.Context(),
		dir,
		0,
		func(lockedCtx context.Context) error {
			releasedCtx = lockedCtx
			return nil
		},
	)
	c.Assert(err, qt.IsNil)

	recoveryCtx, cancel := context.WithTimeout(releasedCtx, 50*time.Millisecond)
	defer cancel()
	err = atlasmigrate.WithMigrationDirectoryLock(
		t.Context(),
		dir,
		0,
		func(context.Context) error {
			return atlasmigrate.RecoverPendingPublication(recoveryCtx, dir)
		},
	)

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}

func TestRecoverPendingPublication_DoesNotReuseAnotherDirectoryLock(t *testing.T) {
	c := qt.New(t)
	heldDir := c.TempDir()
	recoveryDir := c.TempDir()
	recoveryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := atlasmigrate.WithMigrationDirectoryLock(
		t.Context(),
		recoveryDir,
		0,
		func(context.Context) error {
			return atlasmigrate.WithMigrationDirectoryLock(
				t.Context(),
				heldDir,
				0,
				func(heldCtx context.Context) error {
					return atlasmigrate.RecoverPendingPublication(recoveryCtx, recoveryDir)
				},
			)
		},
	)

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}
