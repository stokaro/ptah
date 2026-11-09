//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
)

// PostgreSQL 18 keeps a NOT NULL as a named constraint, RENAME COLUMN leaves the name
// as it was, and a name stays with its holder until a statement frees it. Each row is
// two databases whose difference gives a statement a name another column still holds
// at that point, which the server answers with `constraint "..." for relation "t"
// already exists`. The expected names are read from the database the script moves
// toward, not restated here.
var heldNotNullNames = []struct {
	name, left, right string
}{
	{
		name:  "a renamed column",
		left:  `CREATE TABLE t (id int PRIMARY KEY, full_name text NOT NULL);`,
		right: `CREATE TABLE t (id int PRIMARY KEY, full_name text NOT NULL); ALTER TABLE t RENAME COLUMN full_name TO display_name;`,
	},
	{
		name: "a renamed column a view reads, which keeps the statements apart",
		left: `CREATE TABLE t (id int PRIMARY KEY, full_name text NOT NULL); CREATE VIEW v AS SELECT id, full_name FROM t;`,
		right: `CREATE TABLE t (id int PRIMARY KEY, full_name text NOT NULL); CREATE VIEW v AS SELECT id, full_name FROM t;
			ALTER TABLE t RENAME COLUMN full_name TO display_name;`,
	},
	{
		name: "a column that takes the name of one the plan removes",
		left: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL, a_new text NOT NULL);`,
		right: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL, a_new text NOT NULL);
			ALTER TABLE t DROP COLUMN a; ALTER TABLE t RENAME COLUMN a_new TO a;`,
	},
	{
		name: "two columns that trade names",
		left: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL, b text NOT NULL);`,
		right: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL, b text NOT NULL);
			ALTER TABLE t RENAME COLUMN a TO c; ALTER TABLE t RENAME COLUMN b TO a; ALTER TABLE t RENAME COLUMN c TO b;`,
	},
	{
		name: "a renamed column and a new one under its old name",
		left: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);`,
		right: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);
			ALTER TABLE t RENAME COLUMN a TO b; ALTER TABLE t ADD COLUMN a text NOT NULL;`,
	},
}

// tableTNotNullNames reads the NOT NULL constraints of public.t by column, in column
// name order.
func tableTNotNullNames(c *qt.C, db *sql.DB) string {
	c.Helper()
	var names string
	err := db.QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(a.attname || '=' || k.conname, ',' ORDER BY a.attname), '')
		FROM pg_constraint k
		JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = k.conkey[1]
		WHERE k.contype = 'n' AND k.conrelid = 'public.t'::regclass`).Scan(&names)
	c.Assert(err, qt.IsNil)
	return names
}

// assertScriptRunsToSynced runs what run prints for the row on the left database. The
// right database's names arrive with it, and the next plan reads synced.
func assertScriptRunsToSynced(c *qt.C, run func(args ...string) (string, error), left, right string) {
	c.Helper()
	leftURL := newEmptyReadScopeDatabase(c, "held_left")
	rightURL := newEmptyReadScopeDatabase(c, "held_right")
	execPostgresFamilySQL(c, c.Context(), leftURL, left)
	execPostgresFamilySQL(c, c.Context(), rightURL, right)

	script, err := run("schema", "diff", "--from", leftURL, "--to", rightURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", script))
	execPostgresFamilySQL(c, c.Context(), leftURL, script)

	c.Assert(tableTNotNullNames(c, openReadScopeDatabase(c, leftURL)), qt.Equals, tableTNotNullNames(c, openReadScopeDatabase(c, rightURL)))
	next, err := run("schema", "diff", "--from", leftURL, "--to", rightURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", next))
	c.Assert(next, qt.Contains, "Schemas are synced")
}

// TestSchemaDiffFreesAHeldNotNullNameE2E holds the native plan to every row.
func TestSchemaDiffFreesAHeldNotNullNameE2E(t *testing.T) {
	for _, test := range heldNotNullNames {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assertScriptRunsToSynced(c, runPtahNativeOutcome, test.left, test.right)
		})
	}
}

// TestCompatSchemaDiffFreesAHeldNotNullNameE2E holds ptah-compat to the rows where its
// shape differs: one statement that needs no rename, and two statements kept apart. The
// rest reach the same pass over the same statements.
func TestCompatSchemaDiffFreesAHeldNotNullNameE2E(t *testing.T) {
	for _, test := range heldNotNullNames[:2] {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assertScriptRunsToSynced(c, runCompatVerb, test.left, test.right)
		})
	}
}

// TestCompatStrictSchemaDiffFreesAHeldNotNullNameE2E is the same plan under the strict
// policy, which leaves a statement that runs as it is.
func TestCompatStrictSchemaDiffFreesAHeldNotNullNameE2E(t *testing.T) {
	c := qt.New(t)
	runStrict := func(args ...string) (string, error) {
		return runCompatVerbWithPolicy(atlascompatpolicy.StrictCE(), args...)
	}
	assertScriptRunsToSynced(c, runStrict, heldNotNullNames[1].left, heldNotNullNames[1].right)
}

// heldNotNullNameFiles is a schema and the same schema with a column renamed, its NOT
// NULL declared under the name the rename leaves it. The second row keeps the two
// statements apart on the surface that would merge them, and in the third the rollback
// has to give a kept column its name back ahead of the column it restores.
var heldNotNullNameFiles = []struct {
	name, before, after string
	// wantAfter is the names the second file declares, read back from the database.
	wantAfter string
	// wantRename says the plan moves a name, where one statement cannot free it.
	wantRename bool
}{
	{
		name:      "a renamed column",
		before:    `CREATE TABLE t (id int PRIMARY KEY, full_name text NOT NULL);`,
		after:     `CREATE TABLE t (id int PRIMARY KEY, display_name text CONSTRAINT t_full_name_not_null NOT NULL);`,
		wantAfter: "display_name=t_full_name_not_null,id=t_id_not_null",
	},
	{
		name:   "a renamed column a view reads",
		before: `CREATE TABLE t (id int PRIMARY KEY, full_name text NOT NULL); CREATE VIEW v AS SELECT id, full_name AS n FROM t;`,
		after: `CREATE TABLE t (id int PRIMARY KEY, display_name text CONSTRAINT t_full_name_not_null NOT NULL);
			CREATE VIEW v AS SELECT id, display_name AS n FROM t;`,
		wantAfter:  "display_name=t_full_name_not_null,id=t_id_not_null",
		wantRename: true,
	},
	{
		name:       "a column that takes the name of one the plan removes",
		before:     `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL, a_new text NOT NULL);`,
		after:      `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT t_a_new_not_null NOT NULL);`,
		wantAfter:  "a=t_a_new_not_null,id=t_id_not_null",
		wantRename: true,
	},
}

// TestCompatMigrateDiffFreesAHeldNotNullNameBothWaysE2E runs the files `migrate diff`
// writes forward, back and forward. PostgreSQL runs the drops of one statement first,
// so a plan that merges the drop with the addition needs no rename, and writing one
// would only move the file further from what the community binary writes.
func TestCompatMigrateDiffFreesAHeldNotNullNameBothWaysE2E(t *testing.T) {
	for _, test := range heldNotNullNameFiles {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := diffStatementStyleSteps(c, exactSteps(test.before, test.after))
			pairs := readMigrationPairs(c, dir)
			c.Assert(pairs, qt.HasLen, 2)
			target := statementStyleTarget(c)
			execOn := func(script string) {
				_, err := target.ExecContext(c.Context(), script)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", script))
			}

			execOn(pairs[0].Up)
			before := tableTNotNullNames(c, target)
			execOn(pairs[1].Up)
			after := tableTNotNullNames(c, target)
			execOn(pairs[1].Down)
			restored := tableTNotNullNames(c, target)

			c.Assert(after, qt.Equals, test.wantAfter)
			c.Assert(restored, qt.Equals, before)
			c.Assert(strings.Contains(pairs[1].Up, "RENAME CONSTRAINT"), qt.Equals, test.wantRename)
			c.Assert(strings.Contains(pairs[1].Down, "RENAME CONSTRAINT"), qt.Equals, test.wantRename)
		})
	}
}

// TestMigrationsFreeAHeldNotNullNameBothWaysE2E generates each file over a replay of
// the directory, applies them, and rolls the last one back. Native statements stay
// apart, so the rename is in both files.
func TestMigrationsFreeAHeldNotNullNameBothWaysE2E(t *testing.T) {
	c := qt.New(t)
	test := heldNotNullNameFiles[0]
	target := newEmptyReadScopeDatabase(c, "held_target")
	dev := newEmptyReadScopeDatabase(c, "held_dev")
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	generate := func(name, schema string) string {
		file := filepath.Join(work, name+".sql")
		c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
		return runPtahNative(c, "migrations", "generate", "--replay", "--dev-url", dev,
			"--schema-file", file, "--migrations-dir", dir, "--name", name)
	}
	generate("before", test.before)
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	base, _, _ := strings.Cut(filepath.Base(files[0]), "_")
	db := openReadScopeDatabase(c, target)

	runPtahNative(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--to-version", base)
	before := tableTNotNullNames(c, db)
	generate("after", test.after)
	runPtahNative(c, "migrations", "up", "--allow-destructive", "--db-url", target, "--migrations-dir", dir)
	after := tableTNotNullNames(c, db)
	again := generate("again", test.after)
	runPtahNative(c, "migrations", "down", "--confirm", "--db-url", target, "--migrations-dir", dir, "--target", base)

	c.Assert(after, qt.Equals, test.wantAfter)
	c.Assert(again, qt.Contains, "no migration files generated")
	c.Assert(tableTNotNullNames(c, db), qt.Equals, before)
}
