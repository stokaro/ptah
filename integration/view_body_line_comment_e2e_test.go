//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A schema file may end a view body with a line comment before the semicolon.
// The comment is part of the body Ptah reads, and a migration file writes the
// statement's semicolon after it. On the comment's line the semicolon is part
// of the comment, the next CREATE runs into this one, and PostgreSQL refuses
// the file with `syntax error at or near "CREATE"` (stokaro/ptah#5031). Each
// test writes the file, runs it on PostgreSQL and reads the views back.

// lineCommentBodySchema ends order_lc's body with a line comment. The view
// after it is the statement the comment would swallow.
const lineCommentBodySchema = `CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW order_lc AS SELECT count(*) AS n FROM orders WITH NO DATA -- empty too
;
CREATE MATERIALIZED VIEW order_snapshot AS SELECT max(id) AS last_id FROM orders WITH DATA;
`

// lineCommentBodyViews is what the files create: both views, order_lc without
// rows as its WITH NO DATA asks.
var lineCommentBodyViews = []materializedViewState{
	{Name: "order_lc", Populated: false},
	{Name: "order_snapshot", Populated: true},
}

// materializedViewState is a materialized view of the public schema and
// whether the server holds its rows.
type materializedViewState struct {
	Name      string
	Populated bool
}

// publicMaterializedViews reads the materialized views of the public schema
// from pg_class, sorted by name.
func publicMaterializedViews(c *qt.C, rawURL string) []materializedViewState {
	c.Helper()
	db, err := sql.Open("pgx", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT c.relname, c.relispopulated FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'm' ORDER BY c.relname`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var views []materializedViewState
	for rows.Next() {
		var view materializedViewState
		c.Assert(rows.Scan(&view.Name, &view.Populated), qt.IsNil)
		views = append(views, view)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return views
}

// writeLineCommentBodySchema writes the schema file into a directory of the
// test's own and returns its path.
func writeLineCommentBodySchema(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(lineCommentBodySchema), 0o600), qt.IsNil)
	return path
}

// TestCompatMigrateDiffViewBodyLineCommentE2E diffs the schema into an empty
// migration directory, applies the file to an empty database, and diffs again:
// the directory is in sync, so the comment does not read as a change either.
func TestCompatMigrateDiffViewBodyLineCommentE2E(t *testing.T) {
	c := qt.New(t)
	schema := writeLineCommentBodySchema(c)
	dir := "file://" + filepath.ToSlash(filepath.Join(c.TempDir(), "migrations"))
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_line_comment_dev"), "public")
	target := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_line_comment_target"), "public")

	out, err := runCompatVerb("migrate", "diff", "init", "--dir", dir, "--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runCompatVerb("migrate", "apply", "--dir", dir, "--url", target)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(publicMaterializedViews(c, target), qt.DeepEquals, lineCommentBodyViews)

	out, err = runCompatVerb("migrate", "diff", "again", "--dir", dir, "--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
}

// TestMigrationsGenerateViewBodyLineCommentE2E generates a migration for an
// empty database with `ptah migrations generate` and runs it with `ptah
// migrations up`.
func TestMigrationsGenerateViewBodyLineCommentE2E(t *testing.T) {
	c := qt.New(t)
	schema := writeLineCommentBodySchema(c)
	migrations := filepath.Join(c.TempDir(), "migrations")
	target := postgresScratchDevURL(c, "ptah_line_comment_native")

	out, err := runPtahNativeWithError("migrations", "generate", "--db-url", target, "--schema-file", schema,
		"--migrations-dir", migrations, "--name", "init")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("migrations", "up", "--db-url", target, "--migrations-dir", migrations)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(publicMaterializedViews(c, target), qt.DeepEquals, lineCommentBodyViews)
}
