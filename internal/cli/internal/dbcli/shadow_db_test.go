package dbcli_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/replaytrust"
)

// shadowDBURL names a database on a server the run does not own. Nothing
// connects to it: a URL that is not a docker:// one is resolved without a
// connection.
const shadowDBURL = "postgres://ptah_user:not-a-secret@shadow.invalid:5432/shadow"

// TestResolveShadowDB_HappyPath pins that no shadow resolution records its
// server as the run's own, however PTAH_DEV_SERVER_DISPOSABLE is set. The
// record is what the replay guard and the replay cleanup read to treat a
// server as one the run may empty, and a shadow database is often a database
// on the target's own server.
func TestResolveShadowDB_HappyPath(t *testing.T) {
	t.Setenv("PTAH_DEV_SERVER_DISPOSABLE", "1")

	t.Run("a declared disposable server is not the shadow's", func(t *testing.T) {
		c := qt.New(t)
		got, release, err := dbcli.ResolveShadowDB(context.Background(), shadowDBURL)
		c.Assert(err, qt.IsNil)
		c.Cleanup(release)
		c.Assert(got, qt.Equals, shadowDBURL)
		c.Assert(devdocker.RunOwned(got), qt.IsFalse)
		c.Assert(replaytrust.Trusted(got), qt.IsFalse)
	})

	// The control: the declaration the variable carries is recorded where a
	// dev database is resolved, so the assertion above reads a record that
	// exists.
	t.Run("a dev database on the declared server is the run's own", func(t *testing.T) {
		c := qt.New(t)
		disposable, err := devdocker.DisposableServerDeclared()
		c.Assert(err, qt.IsNil)
		got, release, err := devdocker.Resolve(context.Background(), shadowDBURL, devdocker.Options{DeclaredDisposable: disposable})
		c.Assert(err, qt.IsNil)
		c.Cleanup(release)
		c.Assert(devdocker.RunOwned(got), qt.IsTrue)
	})

	t.Run("trust is the declaration a shadow database takes", func(t *testing.T) {
		c := qt.New(t)
		got, release, err := dbcli.ResolveShadowDB(replaytrust.With(context.Background(), true), shadowDBURL)
		c.Assert(err, qt.IsNil)
		c.Cleanup(release)
		c.Assert(replaytrust.Trusted(got), qt.IsTrue)
		c.Assert(devdocker.RunOwned(got), qt.IsFalse)
	})
}
