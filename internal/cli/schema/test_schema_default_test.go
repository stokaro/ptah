package schema_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// writeUnqualifiedSchemaFixture makes the working directory a fixture holding
// the same unqualified `users` table as schema.sql and as a Go annotation
// directory, models, plus a case directory, tests, that reads it.
func writeUnqualifiedSchemaFixture(c *qt.C) {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n"), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(root, "models"), 0o700), qt.IsNil)
	writeUsersModel(c, filepath.Join(root, "models"))
	c.Assert(os.Mkdir(filepath.Join(root, "tests"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "tests", "users.yaml"), []byte(
		"cases:\n"+
			"  - name: users is provisioned\n"+
			"    steps:\n"+
			"      - assert:\n"+
			"          query: SELECT COUNT(*) FROM users\n"+
			"          scalar: \"0\"\n"), 0o600), qt.IsNil)
	c.Chdir(root)
}

// unqualifiedSources are the authored desired-schema sources the fixture
// holds, each spelled as the flags that select it. Each leaves the table
// unqualified, so the throwaway database creates it in its default schema.
var unqualifiedSources = []struct {
	name string
	args []string
}{
	{name: "schema file", args: []string{"--schema-file", "schema.sql"}},
	{name: "schema file through root dir", args: []string{"--root-dir", "schema.sql"}},
	{name: "Go annotations", args: []string{"--root-dir", "models"}},
}

// TestSchemaTestSchemaSelectionUnqualifiedObject_HappyPath pins that a
// selection naming the throwaway database's default schema keeps an object the
// desired schema leaves unqualified. SQLite creates `users` in `main`, so
// `--schema main` names the schema the table lives in, on the ephemeral
// default and on an explicit SQLite URL alike.
func TestSchemaTestSchemaSelectionUnqualifiedObject_HappyPath(t *testing.T) {
	devURLs := []struct {
		name string
		args []string
	}{
		{name: "ephemeral default"},
		{name: "explicit SQLite URL", args: []string{"--db-url", "sqlite://dev.db"}},
	}

	for _, source := range unqualifiedSources {
		for _, devURL := range devURLs {
			t.Run(source.name+", "+devURL.name, func(t *testing.T) {
				c := qt.New(t)
				writeUnqualifiedSchemaFixture(c)

				out, err := runSchemaTestCommand(slices.Concat([]string{"--dir", "tests", "--schema", "main"}, source.args, devURL.args)...)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				c.Assert(out, qt.Contains, `PASS  case "users is provisioned"`)
			})
		}
	}
}

// TestSchemaTestSchemaSelectionUnqualifiedObject_FailurePath is the inverse
// control: the default schema owns the unqualified object, and only it, so a
// selection naming another schema still keeps nothing and is refused. A default
// that matched every selection would pass here.
func TestSchemaTestSchemaSelectionUnqualifiedObject_FailurePath(t *testing.T) {
	for _, source := range unqualifiedSources {
		t.Run(source.name, func(t *testing.T) {
			c := qt.New(t)
			writeUnqualifiedSchemaFixture(c)

			out, err := runSchemaTestCommand(slices.Concat([]string{"--dir", "tests", "--schema", "other"}, source.args)...)

			c.Assert(err, qt.ErrorMatches, `--schema other selects no tables out of the desired schema`)
			c.Assert(out, qt.Not(qt.Contains), "PASS")
		})
	}
}

// TestSchemaTestSchemaSelectionURLScope_FailurePath pins that the throwaway
// database's URL decides which schema owns an unqualified object, before any
// connection is opened. A PostgreSQL search_path naming one schema puts the
// object there, so `--schema public` keeps nothing; a MySQL URL's database is
// its schema, so `--schema other` keeps nothing; and a search_path listing two
// schemas leaves the owner to whichever exists first, which only the server
// knows, so the selection is refused. Nothing listens on port 1, so a run that
// got past the selection would fail on the connection instead.
func TestSchemaTestSchemaSelectionURLScope_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		devURL  string
		schema  string
		wantErr string
	}{
		{
			name:    "PostgreSQL search_path names another schema",
			devURL:  "postgres://ptah@127.0.0.1:1/dev?search_path=other",
			schema:  "public",
			wantErr: `--schema public selects no tables out of the desired schema`,
		},
		{
			name:    "MySQL database names another schema",
			devURL:  "mysql://ptah@127.0.0.1:1/dev",
			schema:  "other",
			wantErr: `--schema other selects no tables out of the desired schema`,
		},
		{
			name:   "PostgreSQL search_path lists two schemas",
			devURL: "postgres://ptah@127.0.0.1:1/dev?search_path=other,public",
			schema: "other",
			wantErr: `--schema other: --db-url sets search_path "other,public", which lists more than one schema,` +
				` so the selection cannot tell which of them owns an object the desired schema leaves unqualified;` +
				` name one schema in search_path`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			writeUnqualifiedSchemaFixture(c)

			out, err := runSchemaTestCommand("--dir", "tests", "--schema-file", "schema.sql", "--schema", test.schema, "--db-url", test.devURL)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(out, qt.Not(qt.Contains), "PASS")
		})
	}
}
