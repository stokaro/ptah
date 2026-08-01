package atlasschema_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasschema"
)

// The escape lint is best-effort and can be defeated (string concatenation
// alone is enough). These tests cover the part that is not best-effort: on the
// ephemeral SQLite dev database Ptah creates for itself, the engine refuses the
// escape whether or not the lint saw it. Every test here runs the payload
// DIRECTLY against the restricted session, bypassing the lint completely, so a
// pass means containment does not depend on pattern matching.

// restrictedEphemeralDev opens the ephemeral SQLite dev database Ptah would
// create for a rehearsal, pins a session, and applies the engine-level
// restrictions. The callback receives a connection whose statements the lint
// never inspected.
func restrictedEphemeralDev(c *qt.C, use func(*dbschema.DatabaseConnection)) {
	c.Helper()
	devURL, cleanup, err := atlasschema.NewEphemeralSQLiteDev()
	c.Assert(err, qt.IsNil)
	c.Cleanup(cleanup)

	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	c.Assert(conn.WithUntrustedSQLSession(ctx, func(session *dbschema.DatabaseConnection) error {
		use(session)
		return nil
	}), qt.IsNil)
}

func TestEphemeralSQLiteDevRefusesAttachAtTheEngine(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	victimPath := filepath.Join(dir, "victim.db")
	seedPlanSQLite(c, victimPath, `CREATE TABLE untouched (id INTEGER PRIMARY KEY);`)

	restrictedEphemeralDev(c, func(session *dbschema.DatabaseConnection) {
		ctx := context.Background()

		// The exact payload from the original escape, run with no lint in
		// front of it.
		_, err := session.ExecContext(ctx, fmt.Sprintf("ATTACH DATABASE %q AS victim", victimPath))

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, "too many attached databases")
	})

	// The victim database never gained the attacker's table.
	c.Assert(sqliteHasTable(c, victimPath, "pwned"), qt.IsFalse)
	c.Assert(sqliteHasTable(c, victimPath, "untouched"), qt.IsTrue)
}

func TestEphemeralSQLiteDevRefusesFilesystemWritesAtTheEngine(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	copyPath := filepath.Join(dir, "exfiltrated.db")

	restrictedEphemeralDev(c, func(session *dbschema.DatabaseConnection) {
		ctx := context.Background()

		// VACUUM INTO writes a database copy to an arbitrary path. SQLite
		// implements it by attaching, so the same restriction covers it.
		_, err := session.ExecContext(ctx, fmt.Sprintf("VACUUM INTO %q", copyPath))
		c.Assert(err, qt.IsNotNil)

		// Loading a native extension is refused by the driver's defaults.
		_, err = session.ExecContext(ctx, `SELECT load_extension('/tmp/evil.so')`)
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, "not authorized")
	})

	_, statErr := os.Stat(copyPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestEphemeralSQLiteDevStillRunsOrdinaryDDL(t *testing.T) {
	c := qt.New(t)

	restrictedEphemeralDev(c, func(session *dbschema.DatabaseConnection) {
		ctx := context.Background()

		// The restriction must not get in the way of the rehearsal itself.
		_, err := session.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)`)
		c.Assert(err, qt.IsNil)
		_, err = session.ExecContext(ctx, `CREATE INDEX idx_users_email ON users (email)`)
		c.Assert(err, qt.IsNil)
		_, err = session.ExecContext(ctx, `INSERT INTO users (email) VALUES ('a@example.com')`)
		c.Assert(err, qt.IsNil)
	})
}

// TestUntrustedSQLSessionRejectsNilCallback pins the guard its sibling
// WithSession already has. The wrapper passes its own closure down, so
// WithSession's nil check never sees the caller's callback and a nil would be
// dereferenced instead of reported.
func TestUntrustedSQLSessionRejectsNilCallback(t *testing.T) {
	c := qt.New(t)
	devURL, cleanup, err := atlasschema.NewEphemeralSQLiteDev()
	c.Assert(err, qt.IsNil)
	c.Cleanup(cleanup)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	err = conn.WithUntrustedSQLSession(ctx, nil)

	c.Assert(err, qt.ErrorMatches, `database session callback is nil`)
}

// TestUntrustedSQLSessionRestrictsBeforeTheCallback pins that the session
// arrives already restricted: the callback never gets a window in which the
// escape would work.
func TestUntrustedSQLSessionRestrictsBeforeTheCallback(t *testing.T) {
	c := qt.New(t)
	devURL, cleanup, err := atlasschema.NewEphemeralSQLiteDev()
	c.Assert(err, qt.IsNil)
	c.Cleanup(cleanup)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	c.Assert(conn.WithUntrustedSQLSession(ctx, func(session *dbschema.DatabaseConnection) error {
		// First statement of the callback, before anything else touches the
		// session.
		_, execErr := session.ExecContext(ctx, `ATTACH DATABASE ':memory:' AS probe`)
		c.Assert(execErr, qt.IsNotNil)
		c.Assert(execErr.Error(), qt.Contains, "too many attached databases")
		return nil
	}), qt.IsNil)
}

// sqliteHasTable reports whether a SQLite database file contains a table.
func sqliteHasTable(c *qt.C, dbPath, table string) bool {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(context.Background(),
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count > 0
}

// seedPlanSQLite creates a SQLite database file with the given DDL.
func seedPlanSQLite(c *qt.C, dbPath, ddl string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for statement := range strings.SplitSeq(ddl, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}
}
