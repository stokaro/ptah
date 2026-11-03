//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// rowTypeWithComposite declares a table and a composite type with an
// attribute of the table's row type, which PostgreSQL can create only after
// the table, beside a table the next step keeps.
const rowTypeWithComposite = `CREATE TABLE keep (id bigint PRIMARY KEY);
CREATE TABLE t (id bigint PRIMARY KEY, a int);
CREATE TYPE wrap AS (r t, n int);
`

// rowTypeRemoved keeps the one table, so its step removes the table and the
// type that names its row type.
const rowTypeRemoved = `CREATE TABLE keep (id bigint PRIMARY KEY);
`

// publicTablesAndComposites reads the tables and composite types of the public
// schema from pg_class as name:relkind, sorted, leaving out the revision
// tables.
func publicTablesAndComposites(c *qt.C, rawURL string) []string {
	c.Helper()
	db, err := sql.Open("pgx", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT c.relname || ':' || c.relkind::text FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'c') AND c.relname NOT LIKE 'schema_migrations%' ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestMigrationsGenerateCompositeOfATableRowTypeE2E renders the declaration,
// then generates and runs the step that creates a table and a composite type
// naming its row type, the step that removes both, the rollback of the
// removal, and the removal again. Each orders the type after the table on
// creation and before it on removal. With the type first, the creation and
// the rollback stopped at `type "t" does not exist`.
func TestMigrationsGenerateCompositeOfATableRowTypeE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	withType, removed := filepath.Join(dir, "s1.sql"), filepath.Join(dir, "s2.sql")
	c.Assert(os.WriteFile(withType, []byte(rowTypeWithComposite), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(removed, []byte(rowTypeRemoved), 0o600), qt.IsNil)
	target := postgresScratchDevURL(c, "ptah_composite_rowtype")

	rendered, err := runPtahNativeWithError("schema", "render", "--dialect", "postgres", "--schema-file", withType)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", rendered))
	table, typ := strings.Index(rendered, `CREATE TABLE "t"`), strings.Index(rendered, `CREATE TYPE "wrap"`)
	c.Assert(table, qt.Not(qt.Equals), -1, qt.Commentf("%s", rendered))
	c.Assert(typ > table, qt.IsTrue, qt.Commentf("%s", rendered))

	out, err := runPtahNativeWithError("migrations", "generate", "--db-url", target, "--schema-file", withType,
		"--migrations-dir", migrations, "--name", "s1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("migrations", "up", "--db-url", target, "--migrations-dir", migrations)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(publicTablesAndComposites(c, target), qt.DeepEquals, []string{"keep:r", "t:r", "wrap:c"})
	entries, err := os.ReadDir(migrations)
	c.Assert(err, qt.IsNil)
	first := slices.MinFunc(entries, func(a, b os.DirEntry) int { return strings.Compare(a.Name(), b.Name()) }).Name()
	version, _, _ := strings.Cut(first, "_")

	out, err = runPtahNativeWithError("migrations", "generate", "--db-url", target, "--schema-file", removed,
		"--migrations-dir", migrations, "--name", "s2")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("migrations", "up", "--db-url", target, "--migrations-dir", migrations, "--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(publicTablesAndComposites(c, target), qt.DeepEquals, []string{"keep:r"})

	out, err = runPtahNativeWithError("migrations", "down", "--db-url", target, "--migrations-dir", migrations,
		"--target", version, "--confirm")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(publicTablesAndComposites(c, target), qt.DeepEquals, []string{"keep:r", "t:r", "wrap:c"})

	out, err = runPtahNativeWithError("migrations", "up", "--db-url", target, "--migrations-dir", migrations, "--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(publicTablesAndComposites(c, target), qt.DeepEquals, []string{"keep:r"})
}
