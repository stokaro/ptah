package atlassource_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlassource"
)

// tableCount asks the held connection how many user tables its server holds,
// which is what separates a connection whose state is still there from one
// that is closed or already cleaned.
func tableCount(c *qt.C, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(c.Context(),
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).
		Scan(&count), qt.IsNil)
	return count
}

// A database source hands over its own connection, open, with the schema it
// was read from still behind it.
func TestResolveHolding_DatabaseSourceHoldsItsConnection(t *testing.T) {
	c := qt.New(t)
	url := seedSQLite(t, "CREATE TABLE held_users (id INTEGER PRIMARY KEY)")
	set := classifySingle(t, "--from", url)
	var heldTables int
	var heldState atlassource.State

	err := set.ResolveHolding(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite", DialectFlag: "--dev-url"},
		func(state atlassource.State, conn *dbschema.DatabaseConnection) error {
			heldState = state
			heldTables = tableCount(c, conn)
			return nil
		})

	c.Assert(err, qt.IsNil)
	c.Assert(heldState.Kind, qt.Equals, atlassource.KindDatabase)
	c.Assert(heldState.DB.Tables, qt.HasLen, 1)
	c.Assert(heldTables, qt.Equals, 1)
}

// A migration directory hands over the replay session before the replay's
// cleanup, so the replayed table is still on the dev database during the call
// and gone after it.
func TestResolveHolding_MigrationDirHoldsTheReplaySession(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	set := classifySingle(t, "--from", "file://"+dir)
	var heldTables int

	err := set.ResolveHolding(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite", DialectFlag: "--dev-url", DevURL: devURL,
	}, func(_ atlassource.State, conn *dbschema.DatabaseConnection) error {
		heldTables = tableCount(c, conn)
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(heldTables, qt.Equals, 1)
	assertSQLiteDevEmpty(c, devURL)
}

// A schema file has no server behind it, and the connection is nil.
func TestResolveHolding_LocalFileHoldsNoConnection(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE declared (id INTEGER PRIMARY KEY);"), 0o600), qt.IsNil)
	set := classifySingle(t, "--to", "file://"+path)
	called := false
	var heldConn *dbschema.DatabaseConnection

	err := set.ResolveHolding(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite", DialectFlag: "--dev-url"},
		func(_ atlassource.State, conn *dbschema.DatabaseConnection) error {
			called = true
			heldConn = conn
			return nil
		})

	c.Assert(err, qt.IsNil)
	c.Assert(called, qt.IsTrue)
	c.Assert(heldConn, qt.IsNil)
}

// errHeld is what the hold below fails with.
var errHeld = errors.New("the caller failed")

// The hold's own error comes back as it was returned: the replay wraps what
// its callback returns with the source's name, and a caller's failure is not
// a failure to read the source.
func TestResolveHolding_FailurePath_HoldErrorComesBackUnwrapped(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	set := classifySingle(t, "--from", "file://"+dir)

	err := set.ResolveHolding(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite", DialectFlag: "--dev-url", DevURL: devURL,
	}, func(atlassource.State, *dbschema.DatabaseConnection) error {
		return errHeld
	})

	c.Assert(err, qt.Equals, errHeld)
	assertSQLiteDevEmpty(c, devURL)
}

// errInvalid is what the validator below refuses with.
var errInvalid = errors.New("the schema is refused")

// The validation [atlassource.Set.Resolve] runs still runs, and a refused
// state never reaches the hold.
func TestResolveHolding_FailurePath_ValidationRunsBeforeTheHold(t *testing.T) {
	c := qt.New(t)
	url := seedSQLite(t, "CREATE TABLE held_users (id INTEGER PRIMARY KEY)")
	set := classifySingle(t, "--from", url)
	called := false

	err := set.ResolveHolding(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite", DialectFlag: "--dev-url",
		ValidateInspectedSchema: func(*schemamodel.Database) error { return errInvalid },
	}, func(atlassource.State, *dbschema.DatabaseConnection) error {
		called = true
		return nil
	})

	c.Assert(err, qt.ErrorIs, errInvalid)
	c.Assert(called, qt.IsFalse)
}
