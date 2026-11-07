//go:build integration

package notnullname_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestSchemaDiffNamesTheNotNullItAdds_HappyPath makes a column NOT NULL under the name
// the right database holds. Each script runs on the left database, the names read back
// match the right database, and the next plan is synced.
func TestSchemaDiffNamesTheNotNullItAdds_HappyPath(t *testing.T) {
	tests := []struct {
		name, left, right string
		wantStatement     string
	}{
		{
			name:          "a name somebody chose",
			left:          `CREATE TABLE t (id int PRIMARY KEY, a text)`,
			right:         `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL)`,
			wantStatement: `ALTER TABLE "t" ADD CONSTRAINT "nn" NOT NULL "a"`,
		},
		{
			name:          "the name the server gives, which leaves the statement alone",
			left:          `CREATE TABLE t (id int PRIMARY KEY, a text)`,
			right:         `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL)`,
			wantStatement: `ALTER COLUMN "a" SET NOT NULL`,
		},
		{
			name: "a name the server numbered, because a kept column holds the plain one",
			left: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);
				ALTER TABLE t RENAME COLUMN a TO b; ALTER TABLE t ADD COLUMN a text`,
			right: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);
				ALTER TABLE t RENAME COLUMN a TO b; ALTER TABLE t ADD COLUMN a text; ALTER TABLE t ALTER COLUMN a SET NOT NULL`,
			wantStatement: `ADD CONSTRAINT "t_a_not_null1" NOT NULL "a"`,
		},
		{
			name: "a chosen name, while a kept column holds the name the server would give",
			left: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);
				ALTER TABLE t RENAME COLUMN a TO b; ALTER TABLE t ADD COLUMN a text`,
			right: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);
				ALTER TABLE t RENAME COLUMN a TO b; ALTER TABLE t ADD COLUMN a text CONSTRAINT nn NOT NULL`,
			wantStatement: `ADD CONSTRAINT "nn" NOT NULL "a"`,
		},
		{
			name:          "the name of a column the plan removes",
			left:          `CREATE TABLE t (id int PRIMARY KEY, a text, b text NOT NULL)`,
			right:         `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT t_b_not_null NOT NULL)`,
			wantStatement: `ADD CONSTRAINT "t_b_not_null" NOT NULL "a"`,
		},
		{
			name:          "a column that gains a primary key",
			left:          `CREATE TABLE t (id int, v text)`,
			right:         `CREATE TABLE t (id int CONSTRAINT nn NOT NULL PRIMARY KEY, v text)`,
			wantStatement: `ADD CONSTRAINT "nn" NOT NULL "id"`,
		},
		{
			name:          "a column whose type changes too",
			left:          `CREATE TABLE t (id int PRIMARY KEY, a int)`,
			right:         `CREATE TABLE t (id int PRIMARY KEY, a bigint CONSTRAINT nn NOT NULL)`,
			wantStatement: `ADD CONSTRAINT "nn" NOT NULL "a"`,
		},
	}
	for _, surface := range surfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				left := newDatabase(c, dbtarget.PostgreSQL, test.left)
				right := newDatabase(c, dbtarget.PostgreSQL, test.right)

				script := plan(c, surface.run, left, right)
				execScript(c, left, script)

				c.Assert(script, qt.Contains, test.wantStatement)
				c.Assert(notNullNames(c, left), qt.Equals, notNullNames(c, right))
				assertSynced(c, surface.run, left, right)
			})
		}
	}
}

// TestSchemaDiffFillsARowBeforeItNamesTheNotNull fills the NULL rows from the column's
// declared default, ahead of the constraint. Strict mode writes no fill, and
// TestSchemaDiffNamedNotNull_FailurePath holds it.
func TestSchemaDiffFillsARowBeforeItNamesTheNotNull(t *testing.T) {
	for _, surface := range surfaces[:2] {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			left := newDatabase(c, dbtarget.PostgreSQL, `CREATE TABLE t (id int PRIMARY KEY, a text DEFAULT 'x'); INSERT INTO t VALUES (1, NULL)`)
			right := newDatabase(c, dbtarget.PostgreSQL, `CREATE TABLE t (id int PRIMARY KEY, a text DEFAULT 'x' CONSTRAINT nn NOT NULL)`)

			execScript(c, left, plan(c, surface.run, left, right))

			c.Assert(notNullNames(c, left), qt.Equals, notNullNames(c, right))
			c.Assert(queryString(c, left, `SELECT a FROM t`), qt.Equals, "x")
			assertSynced(c, surface.run, left, right)
		})
	}
}

// TestSchemaDiffNamedNotNull_FailurePath is a NULL row nothing fills. The server refuses
// the constraint, as it refuses SET NOT NULL, and the names stay.
func TestSchemaDiffNamedNotNull_FailurePath(t *testing.T) {
	tests := []struct {
		name, left, right string
		surface           int
	}{
		{
			name:    "no default on ptah",
			left:    `CREATE TABLE t (id int PRIMARY KEY, a text); INSERT INTO t VALUES (1, NULL)`,
			right:   `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL)`,
			surface: 0,
		},
		{
			name:    "no default on ptah-compat",
			left:    `CREATE TABLE t (id int PRIMARY KEY, a text); INSERT INTO t VALUES (1, NULL)`,
			right:   `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL)`,
			surface: 1,
		},
		{
			name:    "a default strict mode does not fill from",
			left:    `CREATE TABLE t (id int PRIMARY KEY, a text DEFAULT 'x'); INSERT INTO t VALUES (1, NULL)`,
			right:   `CREATE TABLE t (id int PRIMARY KEY, a text DEFAULT 'x' CONSTRAINT nn NOT NULL)`,
			surface: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			surface := surfaces[test.surface]
			left := newDatabase(c, dbtarget.PostgreSQL, test.left)
			right := newDatabase(c, dbtarget.PostgreSQL, test.right)

			err := tryScript(c, left, plan(c, surface.run, left, right))

			c.Assert(err, qt.ErrorMatches, `.*column "a" of relation "t" contains null values.*`)
			c.Assert(notNullNames(c, left), qt.Equals, "id=t_id_not_null")
		})
	}
}

// TestNamedNotNullFilesRunBothWays runs what each surface writes forward, back and
// forward. The rollback takes the constraint away by its column, so it reads no name.
func TestNamedNotNullFilesRunBothWays(t *testing.T) {
	const before = `CREATE TABLE t (id int PRIMARY KEY, a text)`
	const after = `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL)`
	want := namesBothWays{
		before:   "id=t_id_not_null",
		after:    "a=nn,id=t_id_not_null",
		restored: "id=t_id_not_null",
		again:    "a=nn,id=t_id_not_null",
	}
	t.Run("ptah", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(nativeFilesBothWays(c, before, after), qt.Equals, want)
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

// TestSchemaFileAddsANamedNotNull reads the statement a plan writes from a schema file,
// offline, and applies what it reads. The name reaches the catalog, and a second apply
// has nothing to do.
func TestSchemaFileAddsANamedNotNull(t *testing.T) {
	c := qt.New(t)
	target := newDatabase(c, dbtarget.PostgreSQL, `SELECT 1`)
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(`CREATE TABLE t (id int PRIMARY KEY, a text);
ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;
`), 0o600), qt.IsNil)
	apply := func() string {
		output, err := runPtah("schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")
		c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
		return output
	}

	apply()
	names := notNullNames(c, target)
	again := apply()

	c.Assert(names, qt.Equals, "a=nn,id=t_id_not_null")
	c.Assert(again, qt.Not(qt.Contains), "ALTER TABLE")
}
