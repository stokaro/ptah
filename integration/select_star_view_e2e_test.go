//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// PostgreSQL expands a `*` when it creates a view and stores the column list,
// so a declaration written `SELECT *` never matched what the server reports,
// and every plan replaced the view again; a materialized view was dropped and
// created, with its rows (stokaro/ptah#3633).

// writeSelectStarSchema writes sql to a schema file of its own.
func writeSelectStarSchema(c *qt.C, sql string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql+"\n"), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyConvergesOnASelectStarViewLive applies each schema to an empty
// database and plans it again: the second plan is empty.
func TestSchemaApplyConvergesOnASelectStarViewLive(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "a view selecting every column with a filter",
			sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE VIEW big AS SELECT * FROM orders WHERE total > 100;`,
		},
		{
			name: "a star beside another item and an alias star",
			sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE VIEW big AS SELECT *, 1 AS one FROM orders;
CREATE VIEW recent AS SELECT o.* FROM orders o ORDER BY id;`,
		},
		{
			name: "a materialized view",
			sql: `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE MATERIALIZED VIEW big_mv AS SELECT * FROM orders WHERE total > 100;`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			schema := writeSelectStarSchema(c, test.sql)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyReplacesASelectStarViewWhenTheTableGainsAColumnLive pins the
// star as the desired columns: a view created before its table gained a column
// lacks it, so the plan replaces the view, and the one after is empty.
func TestSchemaApplyReplacesASelectStarViewWhenTheTableGainsAColumnLive(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", writeSelectStarSchema(c,
		`CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE VIEW big AS SELECT * FROM orders WHERE total > 100;`), "--auto-approve")
	grown := writeSelectStarSchema(c, `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL, discount integer NOT NULL DEFAULT 0);
CREATE VIEW big AS SELECT * FROM orders WHERE total > 100;`)

	plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", grown, "--dry-run")
	c.Assert(plan, qt.Contains, `CREATE OR REPLACE VIEW "big"`)
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", grown, "--auto-approve")

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", grown, "--dry-run")
	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestMigrateDiffConvergesOnASelectStarViewLive is the Atlas-compatible path:
// a migration directory that creates a `SELECT *` view, diffed against the same
// SQL, writes no migration.
func TestMigrateDiffConvergesOnASelectStarViewLive(t *testing.T) {
	c := qt.New(t)
	dev, _ := scratchReplayDatabase(c)
	sql := `CREATE TABLE orders (id bigint PRIMARY KEY, total integer NOT NULL);
CREATE VIEW big AS SELECT * FROM orders WHERE total > 100;`
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.sql"), []byte(sql+"\n"), 0o600), qt.IsNil)
	_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", "file://"+writeSelectStarSchema(c, sql), "--dev-url", dev)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
}
