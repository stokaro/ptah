//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// exactApplyDrop is a PostgreSQL database and a schema file that removes one
// of its objects. The run reads the public schema only, so the reporting
// schema holds whatever the declaration does not.
type exactApplyDrop struct {
	name string
	// built is the SQL the database is built from, before the dependent.
	built string
	// desired is the schema file `schema apply` converges public to.
	desired string
	// remains counts what is left of the removed object.
	remains string
}

// The removals the rows below make: a column, a table, and a domain, which
// the PostgreSQL type owner plans.
var (
	exactDropColumn = exactApplyDrop{
		name:    "a column",
		built:   "CREATE TABLE t (id bigint PRIMARY KEY, a int, b int);\nINSERT INTO t VALUES (1, 2, 3);\n",
		desired: "CREATE TABLE t (id bigint PRIMARY KEY, a int);\n",
		remains: "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.t'::regclass AND attname = 'b' AND NOT attisdropped",
	}
	exactDropTable = exactApplyDrop{
		name:    "a table",
		built:   "CREATE TABLE keep (id bigint PRIMARY KEY);\nCREATE TABLE gone (id bigint PRIMARY KEY, b int);\nINSERT INTO gone VALUES (1, 3);\n",
		desired: "CREATE TABLE keep (id bigint PRIMARY KEY);\n",
		remains: "SELECT count(*) FROM pg_class WHERE relname = 'gone'",
	}
	exactDropDomain = exactApplyDrop{
		name:    "a domain",
		built:   "CREATE TABLE keep (id bigint PRIMARY KEY);\nCREATE DOMAIN amount AS numeric(12, 2);\n",
		desired: "CREATE TABLE keep (id bigint PRIMARY KEY);\n",
		remains: "SELECT count(*) FROM pg_type WHERE typname = 'amount'",
	}
)

// exactDropsOfRelatedTables remove a table with a table that depends on it, which
// the plan drops first. Dropped the other way around, a partitioned parent takes
// its partitions, so the drop of one stops at `table "p1" does not exist`, and
// a parent of plain inheritance stops at the child that holds it. Sorted by name,
// each parent would go first.
var exactDropsOfRelatedTables = []exactApplyDrop{
	{
		name: "a partitioned table and its partition",
		built: "CREATE TABLE keep (id bigint PRIMARY KEY);\n" +
			"CREATE TABLE p (id bigint, d date, PRIMARY KEY (id, d)) PARTITION BY RANGE (d);\n" +
			"CREATE TABLE p1 PARTITION OF p FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');\n",
		desired: "CREATE TABLE keep (id bigint PRIMARY KEY);\n",
		remains: "SELECT count(*) FROM pg_class WHERE relname IN ('p', 'p1')",
	},
	{
		name: "a table and a table that inherits it",
		built: "CREATE TABLE keep (id bigint PRIMARY KEY);\n" +
			"CREATE TABLE a (id bigint PRIMARY KEY);\nCREATE TABLE b (x int) INHERITS (a);\n",
		desired: "CREATE TABLE keep (id bigint PRIMARY KEY);\n",
		remains: "SELECT count(*) FROM pg_class WHERE relname IN ('a', 'b')",
	},
}

// runExactApply builds a scratch PostgreSQL database from built and runs the
// native binary's `schema apply` of desired over its public schema. It returns
// the database's URL and the run.
func runExactApply(c *qt.C, built, desired string) (string, clirun.Result) {
	c.Helper()
	target := databaseBuiltFrom(c, built)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema,
		"--schemas", "public", "--auto-approve")
	return target, run
}

// readInt runs a query that returns one integer against the database at url.
func readInt(c *qt.C, url, query string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var value int
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("%s", query))
	return value
}

// TestSchemaApplyExactDropsE2E_HappyPath is the control for the failure path
// below: with nothing outside the declaration reading it, the object goes, so
// a refusal there comes from the reader and not from a drop the plan cannot
// make.
func TestSchemaApplyExactDropsE2E_HappyPath(t *testing.T) {
	for _, test := range append([]exactApplyDrop{exactDropColumn, exactDropTable, exactDropDomain}, exactDropsOfRelatedTables...) {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, test.built, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(readInt(c, target, test.remains), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyExactDropsE2E_FailurePath gives each removal a dependent in
// a schema the run leaves out, holding a value. Native `schema apply` plans
// in the exact statement style, so the drop carries no CASCADE: PostgreSQL
// refuses it, names the dependent, and the plan's one transaction leaves the
// database as it was. Planned in the annotated style, the drop carries
// CASCADE, the apply succeeds, and the dependent and its value are gone.
func TestSchemaApplyExactDropsE2E_FailurePath(t *testing.T) {
	tests := []struct {
		drop exactApplyDrop
		// dependent is the reader nobody declares, in the reporting schema.
		dependent string
		refusal   string
		// value reads the dependent's value back.
		value string
	}{
		{
			drop:      exactDropColumn,
			dependent: "CREATE SCHEMA reporting;\nCREATE VIEW reporting.v AS SELECT id, b FROM public.t;\n",
			refusal:   "cannot drop column b of table t because other objects depend on it",
			value:     "SELECT b FROM reporting.v WHERE id = 1",
		},
		{
			drop:      exactDropTable,
			dependent: "CREATE SCHEMA reporting;\nCREATE VIEW reporting.v AS SELECT id, b FROM public.gone;\n",
			refusal:   "cannot drop table gone because other objects depend on it",
			value:     "SELECT b FROM reporting.v WHERE id = 1",
		},
		{
			drop: exactDropDomain,
			dependent: "CREATE SCHEMA reporting;\nCREATE TABLE reporting.ledger (id bigint PRIMARY KEY, total public.amount);\n" +
				"INSERT INTO reporting.ledger VALUES (1, 3);\n",
			refusal: "cannot drop type amount because other objects depend on it",
			value:   "SELECT total::int FROM reporting.ledger WHERE id = 1",
		},
	}
	for _, test := range tests {
		t.Run(test.drop.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, test.drop.built+test.dependent, test.drop.desired)

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, test.refusal)
			c.Assert(readInt(c, target, test.value), qt.Equals, 3)
		})
	}
}

// exactGeneratedFile runs the native binary's `migrations generate` of desired
// over the public schema of the database at target, and returns the migration
// directory.
func exactGeneratedFile(c *qt.C, target, desired string) string {
	c.Helper()
	dir := c.TempDir()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema,
		"--schemas", "public", "--migrations-dir", dir, "--name", "exact")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	return dir
}

// TestMigrationsGenerateExactDropsE2E_AnUndeclaredReaderStopsTheFile is the
// failure path above through a migration file: the file carries no CASCADE, so
// `migrations up` stops at the server's refusal and the dependent keeps its
// value. Generated in the annotated style, the file applies and takes the
// dependent with it.
func TestMigrationsGenerateExactDropsE2E_AnUndeclaredReaderStopsTheFile(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, exactDropColumn.built+
		"CREATE SCHEMA reporting;\nCREATE VIEW reporting.v AS SELECT id, b FROM public.t;\n")
	dir := exactGeneratedFile(c, target, exactDropColumn.desired)

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")

	c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(up.Stderr, qt.Contains, "cannot drop column b of table t because other objects depend on it")
	c.Assert(readInt(c, target, "SELECT b FROM reporting.v WHERE id = 1"), qt.Equals, 3)
}

// TestMigrationsGenerateExactDropsE2E_AFileStopsOnAPartlyAppliedDatabase is
// what the decision trades a guard for. The file adds a column and drops a
// table; the table is gone by the time it runs. `migrations up` stops at the
// drop, and the file's one transaction takes the added column back with it, so
// nothing of the file stays applied. Generated in the annotated style, IF EXISTS
// makes the drop do nothing and the file records itself applied over a database
// it was not planned against.
func TestMigrationsGenerateExactDropsE2E_AFileStopsOnAPartlyAppliedDatabase(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, "CREATE TABLE keep (id bigint PRIMARY KEY);\nCREATE TABLE gone (id bigint PRIMARY KEY);\n")
	dir := exactGeneratedFile(c, target, "CREATE TABLE keep (id bigint PRIMARY KEY, added int);\n")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "DROP TABLE gone")
	c.Assert(err, qt.IsNil)

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")

	c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(up.Stderr, qt.Contains, `table "gone" does not exist`)
	c.Assert(readInt(c, target,
		"SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.keep'::regclass AND attname = 'added' AND NOT attisdropped",
	), qt.Equals, 0)
}

// filesMatching is the content of the files under dir that pattern matches.
func filesMatching(c *qt.C, dir, pattern string) string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	c.Assert(err, qt.IsNil)
	var written strings.Builder
	for _, match := range matches {
		content, err := os.ReadFile(match) // #nosec G304 -- a path under the test's own temporary directory
		c.Assert(err, qt.IsNil)
		written.Write(content)
	}
	return written.String()
}

// plannedFiles is the content of what a verb that reports its plan in a file
// wrote under dir: a saved plan, and a migration pair.
func plannedFiles(c *qt.C, dir string) string {
	c.Helper()
	return filesMatching(c, dir, "*.json") + filesMatching(c, dir, "migrations/*.sql")
}

// nativePlanningVerbs are the native verbs that show or write a plan, each asking
// through its own planning call.
var nativePlanningVerbs = []struct {
	name string
	args func(target, schema string) []string
}{
	{name: "schema apply --dry-run", args: func(target, schema string) []string {
		return []string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--dry-run"}
	}},
	{name: "schema plan", args: func(target, schema string) []string {
		return []string{"schema", "plan", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--output", "plan.json"}
	}},
	{name: "schema diff", args: func(target, schema string) []string {
		return []string{"schema", "diff", "--from", target, "--to", schema, "--schemas", "public"}
	}},
	{name: "schema compare", args: func(target, schema string) []string {
		return []string{"schema", "compare", "--db-url", target, "--schema-file", schema, "--schemas", "public"}
	}},
	{name: "migrations plan", args: func(target, schema string) []string {
		return []string{"migrations", "plan", "--db-url", target, "--schema-file", schema, "--schemas", "public"}
	}},
	{name: "migrations generate", args: func(target, schema string) []string {
		return []string{"migrations", "generate", "--db-url", target, "--schema-file", schema, "--schemas", "public",
			"--migrations-dir", "migrations", "--name", "exact"}
	}},
}

// TestNativePlanningVerbsE2E_WriteTheExactStatementStyle holds every native
// verb that shows or writes a plan to the style `schema apply` runs, on
// PostgreSQL, so a preview never shows a statement the run does not send. Each
// row reddens when that verb's planning call is planned in the annotated style.
func TestNativePlanningVerbsE2E_WriteTheExactStatementStyle(t *testing.T) {
	for _, test := range nativePlanningVerbs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, exactDropColumn.built+"CREATE TABLE gone (id bigint PRIMARY KEY);\n")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(exactDropColumn.desired), 0o600), qt.IsNil)
			dir := c.TempDir()

			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, test.args(target, schema)...)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			planned := run.Stdout + plannedFiles(c, dir)
			c.Assert(planned, qt.Contains, "DROP COLUMN")
			c.Assert(planned, qt.Contains, "DROP TABLE")
			c.Assert(planned, qt.Not(qt.Contains), "CASCADE")
			c.Assert(planned, qt.Not(qt.Contains), "IF EXISTS")
		})
	}
}

// The enum e loses the value y, which PostgreSQL can only do by rebuilding the type,
// and an index, a check constraint and a view leave in the same plan. The row of t
// shows the rebuild keeps the data.
const (
	enumRebuildBuilt = "CREATE TYPE e AS ENUM ('x', 'y');\n" +
		"CREATE TABLE t (id int PRIMARY KEY, a int, s e, CONSTRAINT t_a_check CHECK (a > 0));\n" +
		"CREATE INDEX i ON t (a);\n" +
		"CREATE VIEW v AS SELECT id, a FROM t;\n" +
		"INSERT INTO t VALUES (1, 2, 'x');\n"
	enumRebuildDesired = "CREATE TYPE e AS ENUM ('x');\n" +
		"CREATE TABLE t (id int PRIMARY KEY, a int, s e);\n"
)

// TestNativePlanningVerbsE2E_AnEnumRebuildLeavesTheOtherDropsWithoutAGuard holds
// every verb to the drops the same plan gets without the rebuild. The rebuild is
// one raw statement ahead of them, and it removes only the type it renamed.
func TestNativePlanningVerbsE2E_AnEnumRebuildLeavesTheOtherDropsWithoutAGuard(t *testing.T) {
	for _, test := range nativePlanningVerbs {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, enumRebuildBuilt)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(enumRebuildDesired), 0o600), qt.IsNil)
			dir := c.TempDir()

			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, test.args(target, schema)...)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			planned := run.Stdout + plannedFiles(c, dir)
			c.Assert(planned, qt.Contains, `ALTER TYPE "e" RENAME TO "e__ptah_old";`)
			c.Assert(planned, qt.Contains, `DROP VIEW "v";`)
			c.Assert(planned, qt.Contains, `DROP INDEX "i";`)
			c.Assert(planned, qt.Contains, `ALTER TABLE "t" DROP CONSTRAINT "t_a_check";`)
			c.Assert(planned, qt.Not(qt.Contains), "IF EXISTS")
		})
	}
}

// TestSchemaApplyExactDropsE2E_AnEnumRebuildRunsWithTheOtherDrops runs that plan on
// the server, and a second plan finding nothing proves it converged. Nothing ahead
// of a drop took its object, or the drop without a guard would stop the plan.
func TestSchemaApplyExactDropsE2E_AnEnumRebuildRunsWithTheOtherDrops(t *testing.T) {
	c := qt.New(t)

	target, run := runExactApply(c, enumRebuildBuilt, enumRebuildDesired)

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	for query, want := range map[string]int{
		"SELECT count(*) FROM pg_enum WHERE enumlabel = 'y'":             0,
		"SELECT count(*) FROM pg_type WHERE typname = 'e__ptah_old'":     0,
		"SELECT count(*) FROM pg_class WHERE relname IN ('i', 'v')":      0,
		"SELECT count(*) FROM pg_constraint WHERE conname = 't_a_check'": 0,
		"SELECT count(*) FROM t WHERE id = 1 AND s = 'x'":                1,
	} {
		c.Assert(readInt(c, target, query), qt.Equals, want, qt.Commentf("%s", query))
	}
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(enumRebuildDesired), 0o600), qt.IsNil)
	again := runPtahIn(c, "schema", "diff", "--from", target, "--to", schema, "--schemas", "public")
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Not(qt.Contains), "ALTER")
	c.Assert(again.Stdout, qt.Not(qt.Contains), "DROP")
}

// TestMigrationsGenerateExactDropsE2E_AFileWithAnEnumRebuildStopsOnAPartlyAppliedDatabase
// is what a guard behind a rebuild would hide. The index is gone by the time the
// file runs. `migrations up` stops at its drop, and the file's one transaction takes
// the rebuild back, so the enum keeps the value. With IF EXISTS on that drop the file
// does nothing there and records itself applied over a database it was not planned
// against.
func TestMigrationsGenerateExactDropsE2E_AFileWithAnEnumRebuildStopsOnAPartlyAppliedDatabase(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, enumRebuildBuilt)
	dir := exactGeneratedFile(c, target, enumRebuildDesired)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "DROP INDEX i")
	c.Assert(err, qt.IsNil)

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")

	c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(up.Stderr, qt.Contains, `index "i" does not exist`)
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_enum WHERE enumlabel = 'y'"), qt.Equals, 1)
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_type WHERE typname = 'e__ptah_old'"), qt.Equals, 0)
}

// enumValueAddedBuilt and enumValueAddedDesired add the value y and the index i, so
// a rollback planned as one change rebuilds the enum and then drops the index.
const (
	enumValueAddedBuilt   = "CREATE TYPE e AS ENUM ('x');\nCREATE TABLE t (id int PRIMARY KEY, a int, s e);\n"
	enumValueAddedDesired = "CREATE TYPE e AS ENUM ('x', 'y');\nCREATE TABLE t (id int PRIMARY KEY, a int, s e);\nCREATE INDEX i ON t (a);\n"
)

// derivedRollbackDir is a hashed migration directory whose second file adds the
// value y and the index i, applied to a scratch database. A rollback to its first
// file derived from the schema difference rebuilds the enum and then drops the index.
// It returns the directory, the database and a dev database.
func derivedRollbackDir(c *qt.C) (dir, target, dev string) {
	c.Helper()
	dir = c.TempDir()
	for name, body := range map[string]string{
		"20200101000000_init.sql": enumValueAddedBuilt,
		"20200102000000_add.sql":  "ALTER TYPE e ADD VALUE 'y';\nCREATE INDEX i ON t (a);\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	target, _ = scratchReplayDatabase(c)
	dev, _ = scratchReplayDatabase(c)
	out, err = runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir, target, dev
}

// TestMigrationsDownE2E_ADerivedRollbackThatRebuildsAnEnumLeavesItsDropWithoutAGuard
// plans in the style a generated rollback carries, through the comparison a derived
// rollback runs instead.
func TestMigrationsDownE2E_ADerivedRollbackThatRebuildsAnEnumLeavesItsDropWithoutAGuard(t *testing.T) {
	c := qt.New(t)
	dir, target, dev := derivedRollbackDir(c)

	run := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--dir-format", "atlas",
		"--revision-format", "atlas", "--shadow-db", dev, "--target", "20200101000000", "--plan", "--dry-run", "--confirm")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, `ALTER TYPE "e" RENAME TO "e__ptah_old"`)
	c.Assert(run.Stdout, qt.Contains, `DROP INDEX "i"`)
	c.Assert(run.Stdout, qt.Not(qt.Contains), "IF EXISTS")
}

// TestSchemaApplyExactDropsE2E_ARoutineLeavesAheadOfTheCockroachDBTableItsBodyReads
// removes a table and a function whose quoted body reads it. CockroachDB records
// what such a body reads, so the table's drop without CASCADE stops at `cannot
// drop table t because other objects depend on it` unless the function goes
// first. A second run finding nothing proves both are gone.
func TestSchemaApplyExactDropsE2E_ARoutineLeavesAheadOfTheCockroachDBTableItsBodyReads(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), "CREATE TABLE keep (id INT8 PRIMARY KEY);\n"+
		"CREATE TABLE t (id INT8 PRIMARY KEY);\n"+
		"CREATE FUNCTION count_t() RETURNS INT8 LANGUAGE SQL AS $$ SELECT count(*) FROM t $$;\n")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE keep (id INT8 PRIMARY KEY);\n"), 0o600), qt.IsNil)

	run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}
