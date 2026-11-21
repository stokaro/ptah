package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
)

// writeSchemaFile writes an SQL schema file into dir and returns its path.
func writeSchemaFile(c *qt.C, dir, name, sql string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
	return path
}

// TestCompatSchemaValidate_ReadsEverySchemaFile repeats --schema-file: the
// second file's foreign key names a table only the first declares, so the
// schema is valid only when both are read. Joined into one --schema-file
// value, the two paths are one path that does not exist.
func TestCompatSchemaValidate_ReadsEverySchemaFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	users := writeSchemaFile(c, dir, "users.sql", "CREATE TABLE users (id integer PRIMARY KEY);\n")
	orders := writeSchemaFile(c, dir, "orders.sql",
		"CREATE TABLE orders (id integer PRIMARY KEY, user_id integer, FOREIGN KEY (user_id) REFERENCES users(id));\n")

	both := runTree(atlas.NewCompatCommand("atlas"), "schema", "validate", "--dialect", "postgres",
		"--schema-file", users, "--schema-file", orders)
	alone := runTree(atlas.NewCompatCommand("atlas"), "schema", "validate", "--dialect", "postgres", "--schema-file", orders)

	c.Assert(both, qt.Equals, compatRun{})
	c.Assert(alone.code, qt.Equals, 1)
	c.Assert(alone.stdout, qt.Contains, `references unknown table "users"`)
}

// TestCompatSchemaValidate_ChecksEveryDialect repeats --dialect with the
// dialect that refuses the schema first: a column-level REFERENCES clause is
// refused for MySQL and accepted for PostgreSQL. Joined, the two names are one
// dialect nobody accepts; with only the last read, the run would pass.
func TestCompatSchemaValidate_ChecksEveryDialect(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	schema := writeSchemaFile(c, dir, "schema.sql",
		"CREATE TABLE users (id integer PRIMARY KEY);\nCREATE TABLE orders (id integer PRIMARY KEY, user_id integer REFERENCES users(id));\n")

	both := runTree(atlas.NewCompatCommand("atlas"), "schema", "validate", "--schema-file", schema,
		"--dialect", "mysql", "--dialect", "postgres")
	postgres := runTree(atlas.NewCompatCommand("atlas"), "schema", "validate", "--schema-file", schema, "--dialect", "postgres")

	c.Assert(both.code, qt.Equals, 1)
	c.Assert(both.stdout, qt.Matches, `mysql: source: [^\n]*a column-level REFERENCES clause[^\n]*\n`)
	c.Assert(postgres, qt.Equals, compatRun{})
}

// TestCompatRepeatedFlags_HelpNamesHowValuesAreForwarded reads the flag type
// each repeatable flag prints, which follows the native flag it forwards to: a
// value per occurrence (stringArray) where the native flag repeats, and a
// comma-separated list (strings) where it takes one joined value.
func TestCompatRepeatedFlags_HelpNamesHowValuesAreForwarded(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "schema validate forwards each file", args: []string{"help", "schema", "validate"},
			want: "      --schema-file stringArray "},
		{name: "schema validate forwards each dialect", args: []string{"help", "schema", "validate"},
			want: "      --dialect stringArray "},
		{name: "migrate checkpoint joins its schemas", args: []string{"help", "migrate", "checkpoint"},
			want: "  -s, --schema strings "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
			c.Assert(got.stdout, qt.Contains, tt.want)
		})
	}
}
