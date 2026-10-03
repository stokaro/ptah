package schematests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// compositeMainHCL declares one table in SQLite's main schema, in Atlas HCL.
const compositeMainHCL = `schema "main" {}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

// compositeSchemaProject writes an HCL part and a SQL part, both labeled with
// SQLite's main schema, and an atlas.hcl whose env "local" takes their
// composition as its desired state. It returns the project directory.
func compositeSchemaProject(c *qt.C) string {
	c.Helper()
	dir := c.TB.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "users.hcl"), []byte(compositeMainHCL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "posts.sql"),
		[]byte("CREATE TABLE posts (id int PRIMARY KEY, user_id int REFERENCES users (id));\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`data "composite_schema" "app" {
  schema "main" {
    url = "file://users.hcl"
  }
  schema "main" {
    url = "file://posts.sql"
  }
}

env "local" {
  url = "`+sqliteURLFromPath(filepath.Join(dir, "app.db"))+`"
  dev = "sqlite://dev?mode=memory"
  src = data.composite_schema.app.url
}
`), 0o600), qt.IsNil)
	return dir
}

// compositeProjectFlags selects the project's env.
func compositeProjectFlags(dir string) []string {
	return []string{"--config", "file://" + filepath.ToSlash(filepath.Join(dir, "atlas.hcl")), "--env", "local"}
}

// TestSchemaApply_AppliesACompositeDesiredState applies the composition with no
// --to: the env's desired state is the composition, which no file:// URL can
// spell. The target is read back, because an apply that planned nothing also
// succeeds.
func TestSchemaApply_AppliesACompositeDesiredState(t *testing.T) {
	c := qt.New(t)
	dir := compositeSchemaProject(c)

	out, err := atlastest.RunCompatOutput(append([]string{"schema", "apply"},
		append(compositeProjectFlags(dir), "--auto-approve")...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	inspected, err := atlastest.RunCompatOutput("schema", "inspect",
		"--url", sqliteURLFromPath(filepath.Join(dir, "app.db")))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", inspected))
	c.Assert(inspected, qt.Contains, `table "users"`)
	c.Assert(inspected, qt.Contains, `table "posts"`)
}

// TestSchemaDiff_ReadsACompositeDesiredState diffs an empty database against
// the composition, again with no --to.
func TestSchemaDiff_ReadsACompositeDesiredState(t *testing.T) {
	c := qt.New(t)
	dir := compositeSchemaProject(c)
	empty := filepath.Join(c.TB.TempDir(), "empty.db")
	c.Assert(os.WriteFile(empty, nil, 0o600), qt.IsNil)

	out, err := atlastest.RunCompatOutput(append([]string{"schema", "diff"},
		append(compositeProjectFlags(dir), "--from", sqliteURLFromPath(empty))...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	// The HCL part names its schema and the SQL part does not, and the diff
	// keeps each spelling.
	c.Assert(out, qt.Contains, `CREATE TABLE "main"."users"`)
	c.Assert(out, qt.Contains, `CREATE TABLE "posts"`)
}

// TestSchemaInspect_ReadsACompositeDesiredState covers both inspection arms:
// the one that resolves the source, and the one that renders it on the dev
// database. A kind either arm does not know is "an unresolved inspection
// source".
func TestSchemaInspect_ReadsACompositeDesiredState(t *testing.T) {
	c := qt.New(t)
	dir := compositeSchemaProject(c)

	out, err := atlastest.RunCompatOutput(append([]string{"schema", "inspect"},
		append(compositeProjectFlags(dir), "--url", "env://src", "--format", "{{ sql . }}")...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(out, qt.Contains, `CREATE TABLE "posts"`)
}

// TestCompositeDesiredState_RefusedWhereOnlyLocalFilesAreRead names the
// construct where a verb reads local schema files only, rather than refusing
// the composition's internal marker as an unknown scheme.
func TestCompositeDesiredState_RefusedWhereOnlyLocalFilesAreRead(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name: "schema apply with a saved plan",
			args: []string{"schema", "apply", "--plan", "file://plan.json", "--auto-approve"},
			wantErr: `schema apply --plan does not support atlas\.hcl data\.composite_schema desired state yet; ` +
				`pass --to explicitly`,
		},
		{
			name:    "schema plan",
			args:    []string{"schema", "plan"},
			wantErr: `.*does not support atlas\.hcl data\.composite_schema desired state yet; pass --to explicitly`,
		},
		{
			name:    "schema test",
			args:    []string{"schema", "test"},
			wantErr: `atlas schema test does not support atlas\.hcl data\.composite_schema desired state yet; pass --url explicitly`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compositeSchemaProject(c)

			out, err := atlastest.RunCompatOutput(append(test.args, compositeProjectFlags(dir)...)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("%s", out))
		})
	}
}
