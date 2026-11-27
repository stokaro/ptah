//go:build integration

package integration_test

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// The server looks a name written without a schema up in pg_catalog before
// any schema of the search path, so an unqualified pg_settings is the
// catalog's view, and an update of it sets the parameter of the row it
// changes, for the session. A history that turns standard_conforming_strings
// off that way makes the server read the next statement's backslash as an
// escape, and the SELECT the guard reads as one statement creates a role.
// pgDumpEscapes reaches the same through SET; these reach it through a name.
const (
	unqualifiedCatalogEscape = "UPDATE pg_settings SET setting = 'off' WHERE name = 'standard_conforming_strings';\n" +
		"SELECT 'x\\' AS a, ' ; CREATE ROLE %s; -- ';\n"
	unicodeEscapedCatalogEscape = "UPDATE U&\"\\0070g_catalog\".pg_settings SET setting = 'off' WHERE name = 'standard_conforming_strings';\n" +
		"SELECT 'x\\' AS a, ' ; CREATE ROLE %s; -- ';\n"
)

// TestReplayUnqualifiedCatalogNameE2E_FailurePath replays each escape with no
// trust, and reads the server's roles back: the role the SELECT would create
// does not exist. PostgreSQL 19 refuses standard_conforming_strings off
// itself, so the rows that need a server that runs it take PostgreSQL 16;
// YugabyteDB and PostgreSQL 18 run it too. The default PostgreSQL server has
// rows as well, which a PostgreSQL 19 server would also refuse on its own.
func TestReplayUnqualifiedCatalogNameE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		body    string
		wantErr string
	}{
		{name: "PostgreSQL/unqualified", engine: dbtarget.PostgreSQL, body: unqualifiedCatalogEscape,
			wantErr: `rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings" because`},
		{name: "PostgreSQL/Unicode escape", engine: dbtarget.PostgreSQL, body: unicodeEscapedCatalogEscape,
			wantErr: `rejects protected namespace "pg_catalog" mutation because`},
		{name: "PostgreSQL 16/unqualified", engine: dbtarget.PostgreSQL16, body: unqualifiedCatalogEscape,
			wantErr: `rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings" because`},
		{name: "PostgreSQL 16/Unicode escape", engine: dbtarget.PostgreSQL16, body: unicodeEscapedCatalogEscape,
			wantErr: `rejects protected namespace "pg_catalog" mutation because`},
		{name: "YugabyteDB/unqualified", engine: dbtarget.YugabyteDB, body: unqualifiedCatalogEscape,
			wantErr: `rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings" because`},
		{name: "YugabyteDB/Unicode escape", engine: dbtarget.YugabyteDB, body: unicodeEscapedCatalogEscape,
			wantErr: `rejects protected namespace "pg_catalog" mutation because`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, test.engine)
			role := fmt.Sprintf("ptah_catalog_escape_%d", time.Now().UnixNano())
			c.Cleanup(func() { dropPgDumpRole(c, test.engine, role) })

			got := pgDumpRefused(c, h, dev, fmt.Sprintf(test.body, role))

			c.Assert(got, qt.Contains, test.wantErr)
			c.Assert(pgDumpQuery[int](c, dev, "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = $1", role), qt.Equals, 0)
		})
	}
}

// TestReplayUnqualifiedCatalogPrivilegeE2E_FailurePath replays an update of
// pg_authid, a catalog every database of the server shares, written without
// its schema, and reads the role back: it still may not create a database.
// A superuser's update of pg_authid persists on PostgreSQL; YugabyteDB
// refuses the write itself, so it has no row here.
func TestReplayUnqualifiedCatalogPrivilegeE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	h, dev := newPgDumpHistory(c, dbtarget.PostgreSQL)
	role := fmt.Sprintf("ptah_catalog_role_%d", time.Now().UnixNano())
	compatDriftExec(c, dbtarget.URL(c, dbtarget.PostgreSQL), "CREATE ROLE "+quoteE2EIdent(role)+" NOCREATEDB")
	c.Cleanup(func() { dropPgDumpRole(c, dbtarget.PostgreSQL, role) })

	got := pgDumpRefused(c, h, dev, fmt.Sprintf("UPDATE pg_authid SET rolcreatedb = true WHERE rolname = '%s';\n", role))

	c.Assert(got, qt.Contains, `rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_authid" because`)
	c.Assert(got, qt.Contains, "qualify the name with the schema of the object it means")
	c.Assert(pgDumpQuery[bool](c, dev, "SELECT rolcreatedb FROM pg_catalog.pg_roles WHERE rolname = $1", role), qt.IsFalse)
}

// unqualifiedCatalogUserTables writes to a table named like a catalog in a
// schema of the user's, and to a table of the user's written without a
// schema.
const unqualifiedCatalogUserTables = `CREATE SCHEMA app;
CREATE TABLE app.pg_settings (name text PRIMARY KEY, setting text);
INSERT INTO app.pg_settings (name, setting) VALUES ('standard_conforming_strings', 'on');
UPDATE app.pg_settings SET setting = 'off' WHERE name = 'standard_conforming_strings';
CREATE TABLE notes (id integer PRIMARY KEY, body text);
INSERT INTO notes (id, body) VALUES (1, 'x');
UPDATE notes SET body = 'y';
`

// TestReplayUnqualifiedCatalogNameE2E_HappyPath is the control for the tests
// above: a catalog's name in a schema of the user's, and a table of the
// user's written without a schema, replay with no trust on every engine, and
// the dev database ends without the schema the history created.
func TestReplayUnqualifiedCatalogNameE2E_HappyPath(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			h, dev := newPgDumpHistory(c, engine.engine)
			writePgDumpMigration(c, h, 1, "user_tables", unqualifiedCatalogUserTables, "DROP TABLE notes;\nDROP SCHEMA app CASCADE;\n")

			got := pgDumpRun(c, h, "migrations", "validate", "--dir", h.dir, "--dev-url", dev)

			c.Assert(got, qt.Contains, "migration SQL validated on dev database")
			c.Assert(pgDumpQuery[int](c, dev, "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'app'"), qt.Equals, 0)
		})
	}
}
