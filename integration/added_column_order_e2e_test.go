//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// addedColumnOrderEngine is one engine a table gains columns on: a database of
// the test's own, a clean one to replay the migration directory on, the query
// answering the table's columns in position, and the migration the community
// binary writes for the change.
type addedColumnOrderEngine struct {
	name    string
	target  func(c *qt.C) string
	columns string
	// wantChange is the file CE v1.3.0 writes for the change, measured with
	// `migrate diff` against each engine. On SQLite CE quotes an identifier
	// with backticks where Ptah writes double quotes, a spelling divergence
	// stokaro/ptah#1235 records; the statements and their order are CE's.
	wantChange string
}

var addedColumnOrderEngines = []addedColumnOrderEngine{
	{
		name: "postgresql",
		target: func(c *qt.C) string {
			target, _ := scratchReplayDatabase(c)
			return target
		},
		columns: "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't'",
		wantChange: `-- Modify "t" table
ALTER TABLE "public"."t" ADD COLUMN "z" integer NULL, ADD COLUMN "b" integer NULL, ADD COLUMN "m" integer NULL;
`,
	},
	{
		name: "mysql",
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "order")
			return url
		},
		columns: "SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 't'",
		wantChange: "-- Modify \"t\" table\n" +
			"ALTER TABLE `t` ADD COLUMN `z` int NULL, ADD COLUMN `b` int NULL, ADD COLUMN `m` int NULL;\n",
	},
	{
		name: "mariadb",
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "order")
			return url
		},
		columns: "SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 't'",
		wantChange: "-- Modify \"t\" table\n" +
			"ALTER TABLE `t` ADD COLUMN `z` int NULL, ADD COLUMN `b` int NULL, ADD COLUMN `m` int NULL;\n",
	},
	{
		name:    "sqlite",
		target:  sqliteScratchURL,
		columns: "SELECT group_concat(name, ',') FROM (SELECT name FROM pragma_table_info('t') ORDER BY cid)",
		wantChange: `-- Add column "z" to table: "t"
ALTER TABLE "t" ADD COLUMN "z" int NULL;
-- Add column "b" to table: "t"
ALTER TABLE "t" ADD COLUMN "b" int NULL;
-- Add column "m" to table: "t"
ALTER TABLE "t" ADD COLUMN "m" int NULL;
`,
	},
}

const (
	// addedColumnOrderInitial is the table before it gains its columns.
	addedColumnOrderInitial = "CREATE TABLE t (a int);\n"
	// addedColumnOrderDesired declares the new columns out of name order, so
	// a plan in name order adds them as b, m, z.
	addedColumnOrderDesired = "CREATE TABLE t (a int, z int, b int, m int);\n"
)

// TestMigrateDiffAddsColumnsInTheDeclaredOrderE2E runs the compatibility
// `migrate diff` for a table that gains three columns declared out of name
// order, pins the file to the one the community binary writes, applies it, and
// reads the columns back in position: the table holds them as a table created
// from the declaration does.
func TestMigrateDiffAddsColumnsInTheDeclaredOrderE2E(t *testing.T) {
	for _, test := range addedColumnOrderEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			dir := filepath.Join(work, "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "20200101000000_init.sql"), []byte(addedColumnOrderInitial), 0o600), qt.IsNil)
			schema := filepath.Join(work, "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(addedColumnOrderDesired), 0o600), qt.IsNil)
			dirURL, schemaURL := "file://"+filepath.ToSlash(dir), "file://"+filepath.ToSlash(schema)
			hashed, err := runCompatVerb("migrate", "hash", "--dir", dirURL)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))
			dev, target := test.target(c), test.target(c)

			diffed, err := runCompatVerb("migrate", "diff", "change", "--dir", dirURL, "--to", schemaURL, "--dev-url", dev)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", diffed))
			c.Assert(filesMatching(c, dir, "*_change.sql"), qt.Equals, test.wantChange)
			applied, err := runCompatVerb("migrate", "apply", "--dir", dirURL, "--url", target)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(heldColumns(c, target, test.columns), qt.Equals, "a,z,b,m")
		})
	}
}

// TestSchemaApplyAddsColumnsInTheDeclaredOrderE2E applies the same change with
// the native `schema apply` and reads the columns back in position, then plans
// once more: the plan is empty.
func TestSchemaApplyAddsColumnsInTheDeclaredOrderE2E(t *testing.T) {
	for _, test := range addedColumnOrderEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.target(c)
			created := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", addedColumnOrderInitial), "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			file := writeDeclaredSchemaFile(c, "schema.sql", addedColumnOrderDesired)

			added := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(added.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", added.Stdout, added.Stderr))
			c.Assert(heldColumns(c, target, test.columns), qt.Equals, "a,z,b,m")
			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestMigrationsDownRestoresRemovedColumnsWhereTheTableHeldThemE2E writes the
// migration that drops three columns, held out of name order, with the native
// `migrations generate`, runs it up and down, and reads the columns back in
// position after each run. The down file adds the columns back in the order the
// table held them, so the table holds them where it did before the migration.
func TestMigrationsDownRestoresRemovedColumnsWhereTheTableHeldThemE2E(t *testing.T) {
	for _, test := range addedColumnOrderEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.target(c)
			created := runPtahIn(c, "schema", "apply", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", addedColumnOrderDesired), "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			dir := filepath.Join(c.TempDir(), "migrations")
			generated := runPtahIn(c, "migrations", "generate", "--db-url", target,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", addedColumnOrderInitial), "--migrations-dir", dir, "--name", "drop")
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(heldColumns(c, target, test.columns), qt.Equals, "a")

			down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			c.Assert(heldColumns(c, target, test.columns), qt.Equals, "a,z,b,m")
		})
	}
}
