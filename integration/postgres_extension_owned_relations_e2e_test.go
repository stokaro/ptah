//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyLeavesAnExtensionsRelationsToTheExtensionE2E applies a source
// that declares the extension and none of what it owns. Each owned relation
// stays out of the comparison, with what hangs off it, so the run has nothing to
// do. Read as a user's object, each is planned as a drop the server refuses with
// 2BP01 while the extension stands, on every run, and no apply can get past it.
// A relation the extension does not own is still the source's to remove.
func TestSchemaApplyLeavesAnExtensionsRelationsToTheExtensionE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	t.Cleanup(cancel)
	binaries := buildDiffSkipBinaries(c, ctx, e2eRepoRoot(t))

	const (
		table  = "CREATE TABLE t (id integer PRIMARY KEY);\n"
		hstore = "CREATE EXTENSION hstore;\n"
		owned  = "CREATE TABLE owned (id integer PRIMARY KEY, code text UNIQUE, n integer CHECK (n > 0));\n" +
			"CREATE INDEX owned_n ON owned (n);\n" +
			"ALTER TABLE owned ENABLE ROW LEVEL SECURITY;\n" +
			"CREATE POLICY owned_all ON owned USING (true);\n" +
			"CREATE FUNCTION owned_touch() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';\n" +
			"CREATE TRIGGER owned_touch BEFORE INSERT ON owned FOR EACH ROW EXECUTE FUNCTION owned_touch();\n" +
			"ALTER EXTENSION hstore ADD FUNCTION owned_touch();\n" +
			"ALTER EXTENSION hstore ADD TABLE owned;\n"
	)
	for _, row := range []struct {
		name, current, declared string
	}{
		{name: "a view the extension creates", current: "CREATE EXTENSION pg_stat_statements;\n", declared: "CREATE EXTENSION pg_stat_statements;\n"},
		{name: "a table with its children", current: hstore + owned, declared: hstore},
		{name: "a view", current: hstore + "CREATE VIEW owned AS SELECT 1 AS one;\nALTER EXTENSION hstore ADD VIEW owned;\n", declared: hstore},
		{name: "a materialized view with its index", current: hstore + "CREATE MATERIALIZED VIEW owned AS SELECT 1 AS one;\nCREATE INDEX owned_one ON owned (one);\n" +
			"ALTER EXTENSION hstore ADD MATERIALIZED VIEW owned;\n", declared: hstore},
		{name: "a sequence", current: hstore + "CREATE SEQUENCE owned;\nALTER EXTENSION hstore ADD SEQUENCE owned;\n", declared: hstore},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(row.declared+table), 0o600), qt.IsNil)
			url := databaseBuiltFrom(c, table+row.current)

			stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
				"--db-url", url, "--schema-file", "schema.sql", "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}

	t.Run("a view the extension does not own", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(hstore+table), 0o600), qt.IsNil)
		url := databaseBuiltFrom(c, table+hstore+"CREATE VIEW mine AS SELECT 1 AS one;\n")

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
			"--db-url", url, "--schema-file", "schema.sql", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Contains, "DROP VIEW \"mine\";\n")
	})

	// The extension's script grants this one, so the read reports it even though
	// it does not report the view.
	t.Run("a grant a source declares on a view the extension creates", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		desired := "CREATE EXTENSION pg_stat_statements;\n" + table + "GRANT SELECT ON pg_stat_statements TO PUBLIC;\n"
		c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(desired), 0o600), qt.IsNil)
		url := databaseBuiltFrom(c, table+"CREATE EXTENSION pg_stat_statements;\n")

		stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, "schema", "apply",
			"--db-url", url, "--schema-file", "schema.sql", "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	})
}
