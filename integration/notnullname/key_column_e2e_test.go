//go:build integration

package notnullname_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestSchemaDiffKeepsTheNotNullNameOfAKeyColumn plans a primary-key column whose NOT NULL
// holds a name the server would not give it, and a column left to the server beside it.
// Each script runs on the left database, the names read back match the right database,
// and the next plan is synced.
func TestSchemaDiffKeepsTheNotNullNameOfAKeyColumn(t *testing.T) {
	tests := []struct {
		name, left, right string
	}{
		{
			name:  "a renamed key column",
			left:  `CREATE TABLE t (id int PRIMARY KEY, v text)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, v text); ALTER TABLE t RENAME COLUMN id TO pk`,
		},
		{
			name:  "a new table whose key column was renamed",
			left:  `SELECT 1`,
			right: `CREATE TABLE t (id int PRIMARY KEY, v text); ALTER TABLE t RENAME COLUMN id TO pk`,
		},
		{
			name:  "a new table whose key column chose a name",
			left:  `SELECT 1`,
			right: `CREATE TABLE t (id int CONSTRAINT nn NOT NULL PRIMARY KEY, v text)`,
		},
		{
			name:  "an added key column that chose a name, on a table with no key",
			left:  `CREATE TABLE t (v text)`,
			right: `CREATE TABLE t (v text, id int CONSTRAINT nn NOT NULL PRIMARY KEY)`,
		},
		{
			name:  "a key column that changes its name only",
			left:  `CREATE TABLE t (id int PRIMARY KEY, v text)`,
			right: `CREATE TABLE t (id int CONSTRAINT nn NOT NULL PRIMARY KEY, v text)`,
		},
		{
			name:  "a composite key with a renamed column",
			left:  `CREATE TABLE t (a int, b int, PRIMARY KEY (a, b))`,
			right: `CREATE TABLE t (a int, b int, PRIMARY KEY (a, b)); ALTER TABLE t RENAME COLUMN b TO c`,
		},
	}
	for _, surface := range surfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				left := newDatabase(c, dbtarget.PostgreSQL, test.left)
				right := newDatabase(c, dbtarget.PostgreSQL, test.right)

				execScript(c, left, plan(c, surface.run, left, right))

				c.Assert(notNullNames(c, left), qt.Equals, notNullNames(c, right))
				assertSynced(c, surface.run, left, right)
			})
		}
	}
}

// TestSchemaDiffLeavesTheServerToNameAKeyColumn is the control: the name the server gives
// a key column stays out of the statement, as it is on every table nobody renamed.
func TestSchemaDiffLeavesTheServerToNameAKeyColumn(t *testing.T) {
	const right = `CREATE TABLE t (id int PRIMARY KEY, v text)`
	tests := []struct {
		name string
		run  func(args ...string) (string, error)
		want string
	}{
		{
			name: "ptah",
			run:  runPtah,
			want: `CREATE TABLE "t" (
  "id" integer PRIMARY KEY NOT NULL,
  "v" text
);
`,
		},
		{
			name: "ptah-compat",
			run:  runCompat,
			want: `-- Create "t" table
CREATE TABLE "t" (
  "id" integer NOT NULL,
  "v" text NULL,
  PRIMARY KEY ("id")
);
`,
		},
		{
			name: "ptah-compat strict",
			run:  runStrictCompat,
			want: `-- Create "t" table
CREATE TABLE "t" (
  "id" integer NOT NULL,
  "v" text NULL,
  PRIMARY KEY ("id")
);
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			left := newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`)

			c.Assert(plan(c, test.run, left, newDatabase(c, dbtarget.PostgreSQL, right)), qt.Equals, test.want)
		})
	}
}

// TestDatabaseReadWritesTheNotNullNameOfAKeyColumn replays what `ptah db read` prints into
// an empty database, which holds the same names afterward.
func TestDatabaseReadWritesTheNotNullNameOfAKeyColumn(t *testing.T) {
	tests := []struct {
		name, schema, want string
	}{
		{
			name:   "a renamed key column",
			schema: `CREATE TABLE t (id int PRIMARY KEY, v text); ALTER TABLE t RENAME COLUMN id TO pk`,
			want:   `"pk" integer PRIMARY KEY CONSTRAINT "t_id_not_null" NOT NULL`,
		},
		{
			name:   "a key column that chose a name",
			schema: `CREATE TABLE t (id int CONSTRAINT nn NOT NULL PRIMARY KEY, v text)`,
			want:   `"id" integer PRIMARY KEY CONSTRAINT "nn" NOT NULL`,
		},
		{
			name:   "a key column left to the server",
			schema: `CREATE TABLE t (id int PRIMARY KEY, v text)`,
			want:   `"id" integer PRIMARY KEY NOT NULL`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := newDatabase(c, dbtarget.PostgreSQL, test.schema)
			replica := newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`)

			output, err := runPtah("db", "read", "--db-url", source)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))

			c.Assert(output, qt.Contains, test.want)
			execScript(c, replica, tableStatement(c, output))
			c.Assert(notNullNames(c, replica), qt.Equals, notNullNames(c, source))
		})
	}
}

// tableStatement is the CREATE TABLE statement of t in what `ptah db read` prints.
func tableStatement(c *qt.C, output string) string {
	c.Helper()
	_, statement, found := strings.Cut(output, `CREATE TABLE "t"`)
	c.Assert(found, qt.IsTrue, qt.Commentf("%s", output))
	body, _, found := strings.Cut(statement, ";")
	c.Assert(found, qt.IsTrue, qt.Commentf("%s", output))
	return `CREATE TABLE "t"` + body + ";"
}

// TestRenamedKeyColumnFilesRunBothWays runs both command surfaces forward,
// back and forward. The rollback restores the old column and its constraint
// name after dropping the column that holds the new primary key.
func TestRenamedKeyColumnFilesRunBothWays(t *testing.T) {
	const before = `CREATE TABLE t (id int PRIMARY KEY, v text)`
	const after = `CREATE TABLE t (pk int CONSTRAINT t_id_not_null NOT NULL PRIMARY KEY, v text)`
	want := namesBothWays{
		before:   "id=t_id_not_null",
		after:    "pk=t_id_not_null",
		restored: "id=t_id_not_null",
		again:    "pk=t_id_not_null",
	}
	t.Run("ptah", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(nativeFilesBothWays(c, before, after), qt.Equals, want)
	})
	t.Run("ptah with a dependent foreign key", func(t *testing.T) {
		c := qt.New(t)
		const childBefore = `; CREATE TABLE child (parent_id int CONSTRAINT child_parent_fk REFERENCES t(id))`
		const childAfter = `; CREATE TABLE child (parent_id int CONSTRAINT child_parent_fk REFERENCES t(pk))`
		c.Assert(nativeFilesBothWays(c, before+childBefore, after+childAfter), qt.Equals, want)
	})
	t.Run("ptah-compat", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(compatFilesBothWays(c, runCompat, before, after), qt.Equals, want)
	})
	t.Run("ptah-compat strict", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(compatFilesBothWays(c, runStrictCompat, before, after), qt.Equals, want)
	})
}

// TestRenamedKeyColumnFileRunsForwardOnPtah generates and applies the native file, whose
// next generation has nothing to write. A key column added to a table without a key keeps
// its name there as well.
func TestRenamedKeyColumnFileRunsForwardOnPtah(t *testing.T) {
	tests := []struct {
		name, before, after, want string
	}{
		{
			name:   "a renamed key column",
			before: `CREATE TABLE t (id int PRIMARY KEY, v text)`,
			after:  `CREATE TABLE t (pk int CONSTRAINT t_id_not_null NOT NULL PRIMARY KEY, v text)`,
			want:   "pk=t_id_not_null",
		},
		{
			name:   "a key column that chose a name, added to a table without a key",
			before: `CREATE TABLE t (v text)`,
			after:  `CREATE TABLE t (v text, id int CONSTRAINT nn NOT NULL PRIMARY KEY)`,
			want:   "id=nn",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			names := nativeFilesForward(c, test.before, test.after)

			c.Assert(names, qt.Equals, test.want)
		})
	}
}

// nativeFilesForward has `ptah migrations generate` write a file for before and then for
// after over a replay of the directory, applies them, holds the directory synced with
// after, and answers the names the database reads back.
func nativeFilesForward(c *qt.C, before, after string) string {
	c.Helper()
	dev := newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`)
	target := newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`)
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	ptah := func(args ...string) string {
		output, err := runPtah(args...)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
		return output
	}
	generate := func(name, schema string) string {
		file := filepath.Join(work, name+".sql")
		c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
		return ptah("migrations", "generate", "--replay", "--dev-url", dev,
			"--schema-file", file, "--migrations-dir", dir, "--name", name)
	}
	generate("before", before)
	generate("after", after)
	c.Assert(generate("again", after), qt.Contains, "no migration files generated")
	ptah("migrations", "up", "--allow-destructive", "--db-url", target, "--migrations-dir", dir)
	return notNullNames(c, target)
}
