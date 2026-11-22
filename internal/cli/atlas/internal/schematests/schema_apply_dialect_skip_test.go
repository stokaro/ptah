package schematests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// The desired schemas of [otherDialectSkipProject]: the database as it is, with
// a column more, with an index, and without the table old.
const (
	keepAndOld      = "CREATE TABLE keep (id INTEGER PRIMARY KEY, a TEXT);\nCREATE TABLE old (id INTEGER PRIMARY KEY);\n"
	keepWithAColumn = "CREATE TABLE keep (id INTEGER PRIMARY KEY, a TEXT, c TEXT);\nCREATE TABLE old (id INTEGER PRIMARY KEY);\n"
	keepWithAnIndex = keepAndOld + "CREATE INDEX keep_a ON keep(a);\n"
	keepOnly        = "CREATE TABLE keep (id INTEGER PRIMARY KEY, a TEXT);\n"
)

// postgresDropTableSkip is a diff block labeled for PostgreSQL that leaves out
// every DROP TABLE, as an env writes it.
const postgresDropTableSkip = "  diff \"postgres\" {\n    skip {\n      drop_table = true\n    }\n  }\n"

// otherDialectSkipProject is a SQLite database holding keep(id, a) and old(id),
// a desired schema, and an atlas.hcl that writes top before its env and env in
// it.
func otherDialectSkipProject(c *qt.C, top, env, desired string) string {
	c.Helper()
	dir := c.TempDir()
	c.Chdir(dir)
	dbPath := filepath.Join(dir, "app.db")
	atlastest.SqliteExec(c, dbPath, "CREATE TABLE keep (id INTEGER PRIMARY KEY, a TEXT); CREATE TABLE old (id INTEGER PRIMARY KEY);")
	c.Assert(os.WriteFile("schema.sql", []byte(desired), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(top+`env "local" {
  url = "sqlite://`+filepath.ToSlash(dbPath)+`"
  src = "file://schema.sql"
`+env+`}
`), 0o600), qt.IsNil)
	return dbPath
}

// A diff block labeled for another dialect that changes nothing in a SQLite
// plan is reported, and the plan is applied. The community binary v1.4.0
// exits 0 on each project and plans the same changes, measured with `schema
// apply --dry-run`: "Schema is synced" on the first, the DROP TABLE of old
// where an env block decides the value, the ADD COLUMN, and a plain CREATE
// INDEX, since SQLite has no concurrent index build.
func TestSchemaApplyReportsADiffBlockLabeledForAnotherDialectThatChangesNothing(t *testing.T) {
	tests := []struct {
		name        string
		top         string
		env         string
		desired     string
		wantOld     int
		wantColumns int
		wantIndexes int
	}{
		{name: "a plan with no change", env: postgresDropTableSkip, desired: keepAndOld, wantOld: 1, wantColumns: 2},
		{
			name:    "the env's unlabeled block decides the value",
			top:     "diff \"postgres\" {\n  skip {\n    drop_table = true\n  }\n}\n",
			env:     "  diff {\n    skip {\n      drop_table = false\n    }\n  }\n",
			desired: keepOnly, wantColumns: 2,
		},
		{
			name:    "the env's block for the plan's dialect decides the value",
			top:     "diff \"postgres\" {\n  skip {\n    drop_table = true\n  }\n}\n",
			env:     "  diff \"sqlite\" {\n    skip {\n      drop_table = false\n    }\n  }\n",
			desired: keepOnly, wantColumns: 2,
		},
		{name: "a plan with no change of the kind", env: postgresDropTableSkip, desired: keepWithAColumn, wantOld: 1, wantColumns: 3},
		{
			name:    "a concurrent_index block for a dialect with no concurrent build",
			env:     "  diff \"postgres\" {\n    concurrent_index {\n      create = true\n    }\n  }\n",
			desired: keepWithAnIndex, wantOld: 1, wantColumns: 2, wantIndexes: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			atlastest.AllowSchemaApplyWithoutDevURL(t)
			c := qt.New(t)
			dbPath := otherDialectSkipProject(c, test.top, test.env, test.desired)

			_, stderr, err := atlastest.RunCompatStreams(c, "schema", "apply", "--env", "local", "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(stderr, qt.Equals, `the diff "postgres" block of the project file is not applied to this plan for "sqlite": `+
				"Ptah applies it only to a plan for postgres, and it changes nothing in this one\n")
			c.Assert(atlastest.SqliteTableCount(c, dbPath, "old"), qt.Equals, test.wantOld)
			c.Assert(atlastest.SqliteQueryInt(c, dbPath, "SELECT count(*) FROM pragma_table_info('keep')"), qt.Equals, test.wantColumns)
			c.Assert(atlastest.SqliteIndexCount(c, dbPath, "keep_a"), qt.Equals, test.wantIndexes)
		})
	}
}

// A diff block labeled for another dialect is refused, before anything is
// applied, where it decides a change the plan holds. The community binary
// v1.4.0 ignores the label and leaves the DROP TABLE out, measured on SQLite:
// "Schema is synced" and exit 0. Ptah applies the block only to PostgreSQL, so
// planning the drop would drop the table the block asks to keep.
func TestSchemaApplyRefusesADiffBlockLabeledForAnotherDialectThatChangesThePlan(t *testing.T) {
	atlastest.AllowSchemaApplyWithoutDevURL(t)
	c := qt.New(t)
	dbPath := otherDialectSkipProject(c, "", postgresDropTableSkip, keepOnly)

	stdout, _, err := atlastest.RunCompatStreams(c, "schema", "apply", "--env", "local", "--auto-approve")

	c.Assert(err, qt.ErrorIs, atlasschema.ErrDiffPolicyForAnotherDialect)
	c.Assert(err, qt.ErrorMatches, `(?s).*the diff "postgres" block of the project file sets drop_table = true in its skip block, `+
		`which Ptah applies only to a plan for postgres\. This plan is for "sqlite" and holds a change that value decides: drop_table of "old"\..*`)
	c.Assert(stdout, qt.Not(qt.Contains), "DROP")
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "old"), qt.Equals, 1)
}
