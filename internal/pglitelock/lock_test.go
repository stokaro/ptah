package pglitelock_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pglitelock"
)

func TestAcquireSerializesOnePGliteRealm(t *testing.T) {
	c := qt.New(t)
	first, err := pglitelock.Acquire(c.Context(), "test-cluster", "postgres", "schema-apply", -1)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(first.Release(), qt.IsNil) }()

	blocked, err := pglitelock.Acquire(c.Context(), "test-cluster", "postgres", "schema-apply", -1)
	c.Assert(err, qt.ErrorIs, pglitelock.ErrHeld)
	c.Assert(blocked, qt.IsNil)

	other, err := pglitelock.Acquire(c.Context(), "other-cluster", "postgres", "schema-apply", -1)
	c.Assert(err, qt.IsNil)
	c.Assert(other.Release(), qt.IsNil)
	c.Assert(first.Release(), qt.IsNil)

	reacquired, err := pglitelock.Acquire(c.Context(), "test-cluster", "postgres", "schema-apply", -1)
	c.Assert(err, qt.IsNil)
	c.Assert(reacquired.Release(), qt.IsNil)
}
