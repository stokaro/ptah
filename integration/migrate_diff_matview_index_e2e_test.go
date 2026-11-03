//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestMigrateDiffMaterializedViewIndexE2E_SchemaSelectionKeepsTheIndex
// declares an index on a materialized view and runs `migrate diff --schema
// public` twice. The first run writes the index with the view, and applying
// the directory puts it on the view. The second run finds the directory in
// sync. Scoped by the kept tables alone, the declared index leaves the
// desired side, and the first run writes the view without it.
func TestMigrateDiffMaterializedViewIndexE2E_SchemaSelectionKeepsTheIndex(t *testing.T) {
	c := qt.New(t)
	dev := postgresScratchDevURL(c, "ptah_matview_index_dev")
	target := postgresScratchDevURL(c, "ptah_matview_index_target")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(`CREATE TABLE orders (id integer PRIMARY KEY, amount integer);
CREATE MATERIALIZED VIEW order_amounts AS SELECT amount FROM orders;
CREATE INDEX order_amounts_amount_idx ON order_amounts (amount);
`), 0o600), qt.IsNil)
	dir := c.TempDir()
	diff := func(name string) []string {
		return []string{"migrate", "diff", name, "--dir", "file://" + filepath.ToSlash(dir),
			"--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev, "--schema", "public"}
	}

	first, err := runCompatVerb(diff("init")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	applied, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	second, err := runCompatVerb(diff("second")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "The migration directory is synced with the desired state")
	c.Assert(materializedViewIndexesOf(c, target), qt.Equals, "order_amounts.order_amounts_amount_idx")
}

// materializedViewIndexesOf reads back the indexes on the materialized views
// of the public schema of the database at rawURL, as `view.index`.
func materializedViewIndexesOf(c *qt.C, rawURL string) string {
	c.Helper()
	db, err := sql.Open("pgx", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var indexes string
	c.Assert(db.QueryRowContext(c.Context(), `
		SELECT coalesce(string_agg(t.relname || '.' || i.relname, ' ' ORDER BY t.relname, i.relname), '')
		FROM pg_index x
		JOIN pg_class i ON i.oid = x.indexrelid
		JOIN pg_class t ON t.oid = x.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'public' AND t.relkind = 'm'`).Scan(&indexes), qt.IsNil)
	return indexes
}
