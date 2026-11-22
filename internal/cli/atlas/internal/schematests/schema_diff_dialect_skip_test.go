package schematests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// dialectSkipDiffProject is a schema diff from a SQLite schema holding
// old_users to one holding nothing, with an atlas.hcl env carrying policy.
func dialectSkipDiffProject(c *qt.C, policy string) string {
	c.Helper()
	dir := c.TempDir()
	c.Chdir(dir)
	from := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(from, []byte("CREATE TABLE old_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("to.sql", []byte("CREATE TABLE kept (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  dev = "sqlite://`+filepath.ToSlash(filepath.Join(dir, "dev.db"))+`"
  src = "file://to.sql"
`+policy+`}
`), 0o600), qt.IsNil)
	return "file://" + filepath.ToSlash(from)
}

// A skip in a diff block labeled with the dev database's dialect leaves the
// drop out of schema diff, as the unlabeled block does.
func TestSchemaDiffUsesALabeledAtlasProjectDiffSkip(t *testing.T) {
	c := qt.New(t)
	from := dialectSkipDiffProject(c, "  diff \"sqlite\" {\n    skip {\n      drop_table = true\n    }\n  }\n")

	stdout, stderr, err := atlastest.RunCompatStreams(c, "schema", "diff", "--env", "local", "--from", from)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Equals, "-- SKIP: DROP TABLE of old_users omitted by diff policy (skip: drop_table)\n"+
		"-- Create \"kept\" table\nCREATE TABLE \"kept\" (\"id\" integer NULL, PRIMARY KEY (\"id\"));\n")
	c.Assert(stderr, qt.Equals, "")
}

// A skip labeled with another dialect is refused rather than ignored.
func TestSchemaDiffRefusesADiffSkipLabeledForAnotherDialect(t *testing.T) {
	c := qt.New(t)
	from := dialectSkipDiffProject(c, "  diff \"postgres\" {\n    skip {\n      drop_table = true\n    }\n  }\n")

	stdout, _, err := atlastest.RunCompatStreams(c, "schema", "diff", "--env", "local", "--from", from)

	c.Assert(err, qt.ErrorIs, atlasschema.ErrDiffPolicyForAnotherDialect)
	c.Assert(stdout, qt.Equals, "")
}
