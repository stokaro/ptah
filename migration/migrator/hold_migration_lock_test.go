package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// HoldMigrationLock lets a caller read, decide and run under one lock. An
// operation of the same migrator, or of a copy with the same lock name, called
// with the context it hands out takes no second lock; anything else takes its
// own, and a migrator on another connection keeps to that connection.
//
// The connections here are an executable provider's whose own session takes
// the lock, so every acquisition reaches the fake and is recorded.

// namedLockRecorder is a sessionRecorder whose connection's own session takes
// the migration lock, as a provider's does, and records the name of every
// lock it takes.
type namedLockRecorder struct {
	*sessionRecorder
	taken *[]string
}

func newNamedLockRecorder() namedLockRecorder {
	return namedLockRecorder{sessionRecorder: newSessionRecorder(), taken: new([]string)}
}

func (r namedLockRecorder) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return r, nil
}

func (r namedLockRecorder) SessionLocker() (dbschema.SessionLocker, error) { return r, nil }

func (r namedLockRecorder) TryLock(_ context.Context, name string) (dbschema.SessionLock, error) {
	*r.taken = append(*r.taken, name)
	return heldSessionLock{}, nil
}

func TestHoldMigrationLock_HappyPath(t *testing.T) {
	t.Run("an operation under the hold takes no second lock", func(t *testing.T) {
		c := qt.New(t)
		recorder := newNamedLockRecorder()
		m := runSessionMigrator(c, recorder)

		err := m.HoldMigrationLock(c.Context(), "migrate down", m.MigrateUp)

		c.Assert(err, qt.IsNil)
		c.Assert(*recorder.taken, qt.DeepEquals, []string{"ptah_migrate"})
		c.Assert(recorder.trail.statementsOn(0), qt.DeepEquals,
			[]string{"CREATE SCHEMA s1", "SET search_path = s1", "CREATE TABLE t3 (id int)"})
	})

	t.Run("a copy with another lock name takes its own", func(t *testing.T) {
		c := qt.New(t)
		recorder := newNamedLockRecorder()
		m := runSessionMigrator(c, recorder)

		err := m.HoldMigrationLock(c.Context(), "migrate down", m.WithMigrationLockName("billing").MigrateUp)

		c.Assert(err, qt.IsNil)
		c.Assert(*recorder.taken, qt.DeepEquals, []string{"ptah_migrate", "billing"})
	})

	t.Run("an operation after the hold takes the lock again", func(t *testing.T) {
		c := qt.New(t)
		recorder := newNamedLockRecorder()
		m := runSessionMigrator(c, recorder)
		c.Assert(m.HoldMigrationLock(c.Context(), "migrate down", func(context.Context) error { return nil }), qt.IsNil)

		err := m.MigrateUp(c.Context())

		c.Assert(err, qt.IsNil)
		c.Assert(*recorder.taken, qt.DeepEquals, []string{"ptah_migrate", "ptah_migrate"})
	})

	t.Run("a migrator without the lock holds none", func(t *testing.T) {
		c := qt.New(t)
		recorder := newNamedLockRecorder()
		m := runSessionMigrator(c, recorder).WithoutMigrationLock()

		err := m.HoldMigrationLock(c.Context(), "migrate down", m.MigrateUp)

		c.Assert(err, qt.IsNil)
		c.Assert(*recorder.taken, qt.HasLen, 0)
	})

	// A shadow replay runs while the target's lock is held, on a connection of
	// its own and without a lock of its own. The held session is the target's,
	// so the replay's bodies must not run there.
	t.Run("a migrator on another connection runs on its own", func(t *testing.T) {
		c := qt.New(t)
		target := newNamedLockRecorder()
		shadow := newSessionRecorder()
		held := runSessionMigrator(c, target)
		replay := newRunSessionMigrator(c, shadow)

		err := held.HoldMigrationLock(c.Context(), "migrate down", replay.MigrateUp)

		c.Assert(err, qt.IsNil)
		c.Assert(*target.taken, qt.DeepEquals, []string{"ptah_migrate"})
		c.Assert(target.trail.ran, qt.HasLen, 0)
		c.Assert(shadow.trail.statementsOn(shadow.trail.lastSession()), qt.DeepEquals,
			[]string{"CREATE SCHEMA s1", "SET search_path = s1", "CREATE TABLE t3 (id int)"})
	})
}
