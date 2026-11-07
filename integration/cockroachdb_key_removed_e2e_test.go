//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// cockroachKeyRemovedRead prints the primary key of public.parent as the server does.
const cockroachKeyRemovedRead = "SELECT conname || ': ' || pg_get_constraintdef(oid) FROM pg_constraint " +
	"WHERE conrelid = 'public.parent'::regclass AND contype = 'p'"

// TestSchemaApplyCockroachDBKeyRemovedE2E_HappyPath removes a primary key on CockroachDB
// 26.3.5, where every table holds one. A DROP CONSTRAINT alone stopped at the server.
// Each row reads the key back and counts the rows, and a second run finds nothing to do.
func TestSchemaApplyCockroachDBKeyRemovedE2E_HappyPath(t *testing.T) {
	const rows = "INSERT INTO parent VALUES (1, 10), (2, 20);\n"
	tests := []struct{ name, built, desired string }{
		{
			name:    "the key leaves",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n",
		},
		{
			name:    "its column leaves after it",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\n" + rows,
			desired: "CREATE TABLE parent (code INT8);\n",
		},
		{
			name:    "a key of another name",
			built:   "CREATE TABLE parent (id INT8, code INT8, CONSTRAINT custom PRIMARY KEY (id));\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n",
		},
		{
			name: "a foreign key takes a UNIQUE over the columns",
			built: "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\n" +
				"CREATE TABLE child (id INT8 PRIMARY KEY, pid INT8 REFERENCES parent (id));\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL UNIQUE, code INT8);\n" +
				"CREATE TABLE child (id INT8 PRIMARY KEY, pid INT8 REFERENCES parent (id));\n",
		},
		{
			name:    "a table without a key, control",
			built:   "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), test.built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, cockroachKeyRemovedRead), qt.Equals, "parent_pkey: PRIMARY KEY (rowid ASC)")
			c.Assert(readInt(c, target, "SELECT count(*) FROM parent"), qt.Equals, 2)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestMigrationsCockroachDBKeyRemovedE2E_TheRollbackRestoresTheKey rolls the removal back.
// The server takes the key off the hidden column and drops the column with it.
func TestMigrationsCockroachDBKeyRemovedE2E_TheRollbackRestoresTheKey(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\nINSERT INTO parent VALUES (1, 10);\n")
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n"), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "key")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(columnKeyString(c, target, cockroachKeyRemovedRead), qt.Equals, "parent_pkey: PRIMARY KEY (rowid ASC)")

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, cockroachKeyRemovedRead), qt.Equals, "parent_pkey: PRIMARY KEY (id ASC)")
	c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'parent' AND column_name = 'rowid'"), qt.Equals, 0)
}

// The plan moves the key of parent to a hidden column in a raw statement, and removes
// a view and a table after it.
const (
	cockroachKeyAndDropsBuilt = "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\n" +
		"INSERT INTO parent VALUES (1, 10), (2, 20);\n" +
		"CREATE TABLE archive (id INT8 PRIMARY KEY);\n" +
		"CREATE VIEW v AS SELECT id FROM archive;\n"
	cockroachKeyAndDropsDesired = "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n"
)

// TestNativePlanningVerbsCockroachDBE2E_AKeyLeavingForRowIDLeavesTheOtherDropsWithoutAGuard
// holds every native verb that shows or writes a plan to the drops the same plan gets
// when the key stays. ADD COLUMN removes nothing a later drop names.
func TestNativePlanningVerbsCockroachDBE2E_AKeyLeavingForRowIDLeavesTheOtherDropsWithoutAGuard(t *testing.T) {
	for _, test := range nativePlanningVerbs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), cockroachKeyAndDropsBuilt)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(cockroachKeyAndDropsDesired), 0o600), qt.IsNil)
			dir := c.TempDir()

			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, test.args(target, schema)...)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			planned := run.Stdout + plannedFiles(c, dir)
			c.Assert(planned, qt.Contains, `ADD COLUMN "rowid"`)
			c.Assert(planned, qt.Contains, `DROP VIEW "v";`)
			c.Assert(planned, qt.Contains, `DROP TABLE "archive";`)
			c.Assert(planned, qt.Not(qt.Contains), "IF EXISTS")
		})
	}
}

// TestCompatSchemaDiffCockroachDBE2E_AKeyLeavingForRowIDLeavesTheOtherDropsWithoutAGuard
// is the row above on the compatibility surface, which plans in the same style.
func TestCompatSchemaDiffCockroachDBE2E_AKeyLeavingForRowIDLeavesTheOtherDropsWithoutAGuard(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), cockroachKeyAndDropsBuilt)
	desired, _ := cockroachVisibilityDatabase(c, c.Context(), cockroachKeyAndDropsDesired)

	out, err := runCompatVerb("schema", "diff", "--from", target, "--to", desired)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `ADD COLUMN "rowid"`)
	c.Assert(out, qt.Contains, `DROP VIEW "v";`)
	c.Assert(out, qt.Contains, `DROP TABLE "archive";`)
	c.Assert(out, qt.Not(qt.Contains), "IF EXISTS")
}

// TestSchemaApplyCockroachDBKeyRemovedE2E_RunsWithTheOtherDrops runs that plan on the
// server. Nothing ahead of a drop took its object, or the drop without a guard would
// stop the plan, and a second run finds nothing to do.
func TestSchemaApplyCockroachDBKeyRemovedE2E_RunsWithTheOtherDrops(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), cockroachKeyAndDropsBuilt)

	run := keyedApply(c, target, cockroachKeyAndDropsDesired)

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(columnKeyString(c, target, cockroachKeyRemovedRead), qt.Equals, "parent_pkey: PRIMARY KEY (rowid ASC)")
	c.Assert(readInt(c, target, "SELECT count(*) FROM parent"), qt.Equals, 2)
	c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('archive', 'v')"), qt.Equals, 0)
	again := keyedApply(c, target, cockroachKeyAndDropsDesired)
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}

// TestMigrationsCockroachDBKeyRemovedE2E_AFileStopsAtATableThatIsGone is what a guard
// behind that raw statement would hide. The table is gone by the time the file runs,
// and `migrations up` stops at its drop. With IF EXISTS on that drop the file does
// nothing there and records itself applied over a database it was not planned
// against.
func TestMigrationsCockroachDBKeyRemovedE2E_AFileStopsAtATableThatIsGone(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), cockroachKeyAndDropsBuilt)
	dir := exactGeneratedFile(c, target, cockroachKeyAndDropsDesired)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "DROP VIEW v")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "DROP TABLE archive")
	c.Assert(err, qt.IsNil)

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")

	c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(up.Stderr, qt.Contains, `does not exist`)
}
