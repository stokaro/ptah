package schema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/sqlitevirtual"
)

// `ptah schema diff` settles what its exclude selectors matched before the
// comparison refuses anything of its own, so the operator hears about a
// selector that protects nothing on a run that fails for another reason too.
// The other reason here is a SQLite virtual table on the --from side, which
// the comparison refuses to plan a drop for.

// sqliteFixture creates a SQLite database from statements and returns its
// URL.
func sqliteFixture(c *qt.C, name string, statements ...string) string {
	c.Helper()
	url := "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), name))
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return url
}

// TestSchemaDiffWarnsAboutAnUnmatchedExcludeBeforeAVirtualTable_FailurePath
// warns about the selector on standard error, then fails at the virtual
// table: the warning is the first line there, and the error the second.
func TestSchemaDiffWarnsAboutAnUnmatchedExcludeBeforeAVirtualTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(sqlitevirtual.AllowDropEnvVar)(t)
	from := sqliteFixture(c, "from.db",
		`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
		`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
	)
	to := sqliteFixture(c, "to.db", `CREATE TABLE users (id INTEGER PRIMARY KEY)`)

	stdout, stderr, err := runSchemaStreams("diff", "--from", from, "--to", to, "--exclude", "nosuch_object")

	c.Assert(err, qt.ErrorMatches, `unsupported feature: the database has virtual table "docs" .*`)
	c.Assert(stderr, qt.Matches, `Warning: the --exclude selection matched no objects: "nosuch_object"\.\n`+
		`error: unsupported feature: the database has virtual table "docs" .*\n`)
	c.Assert(stdout, qt.Equals, "")
}
