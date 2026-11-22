//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A PostgreSQL schema name may begin or end with a space, so `a` and ` a` are
// two schemas. Each test below runs against a database that holds `a.t`, and a
// run that names ` a` has to reach ` a` and leave `a` as it is. The catalog is
// read back to say which schema the server changed.

// spacedSchemaSQL declares both schemas, each with a table t, as SQL.
const spacedSchemaSQL = `CREATE SCHEMA a;
CREATE TABLE a.t (id integer);
CREATE SCHEMA " a";
CREATE TABLE " a".t (id integer NOT NULL, note text);
`

// spacedSchemaHCL is spacedSchemaSQL as an Atlas HCL document.
const spacedSchemaHCL = `schema "a" {}
schema " a" {}
table "t" {
  schema = schema.a
  column "id" {
    type = integer
    null = true
  }
}
table "t" {
  schema = schema[" a"]
  column "id" {
    type = integer
  }
  column "note" {
    type = text
    null = true
  }
}
`

// spacedSchemaIssueHCL is the document stokaro/ptah#6487 reports: one schema,
// ` a`, and a table in it.
const spacedSchemaIssueHCL = `schema " a" {}
table "t" {
  schema = schema[" a"]
  column "id" {
    type = int
  }
}
`

// spacedSchemaApplied is what spacedSchemaTables reads once a desired schema
// declaring both tables is applied to a database holding `a.t`: ` a`.t is
// created, and `a`.t keeps its one nullable column.
const spacedSchemaApplied = "[ a].t(id NO,note YES), [a].t(id YES)"

// newSpacedSchemaDatabase creates a database, dropped when the test ends,
// holding the table `a.t`, and returns its URL.
func newSpacedSchemaDatabase(c *qt.C, role string) string {
	c.Helper()
	database := newEmptyReadScopeDatabase(c, role)
	execPostgresFamilySQL(c, c.Context(), database, "CREATE SCHEMA a")
	execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE a.t (id integer)")
	return database
}

// spacedSchemaTables lists the tables of the schemas `a` and ` a` as
// [schema].table(column nullability, ...), in name order. The brackets show the
// space.
func spacedSchemaTables(c *qt.C, database string) string {
	c.Helper()
	var tables string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg('[' || t.table_schema || '].' || t.table_name || '(' || (
			SELECT string_agg(column_name || ' ' || is_nullable, ',' ORDER BY ordinal_position)
			FROM information_schema.columns k
			WHERE k.table_schema = t.table_schema AND k.table_name = t.table_name) || ')', ', '
			ORDER BY t.table_schema, t.table_name), '')
		FROM information_schema.tables t
		WHERE t.table_schema IN ('a', ' a') AND t.table_type = 'BASE TABLE'`).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables
}

// writeSpacedSchemaFile writes body to a file named name and returns its path.
func writeSpacedSchemaFile(c *qt.C, name, body string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// TestSpacedSchemaNameApplyE2E applies a desired schema that declares `a.t`
// and ` a`.t to a database holding `a.t`, on the native and the compatibility
// surface. The catalog then holds both tables, `a.t` unchanged, and a second
// run plans nothing, because the read pairs each table with its own schema.
func TestSpacedSchemaNameApplyE2E(t *testing.T) {
	tests := []struct {
		name    string
		program clirun.Target
		// args is the command line, given the database, a dev database and
		// the directory holding the desired schema files.
		args   func(database, dev, dir string) []string
		synced string
	}{
		{
			name:    "ptah schema apply",
			program: clirun.Ptah,
			args: func(database, _, dir string) []string {
				return []string{"schema", "apply", "--db-url", database,
					"--schema-file", filepath.Join(dir, "schema.sql"), "--auto-approve"}
			},
			synced: "Schema is synced, no changes to be made.",
		},
		{
			name:    "ptah-compat schema apply",
			program: clirun.Compat,
			args: func(database, dev, dir string) []string {
				return []string{"schema", "apply", "-u", database,
					"--to", "file://" + filepath.ToSlash(filepath.Join(dir, "schema.hcl")), "--dev-url", dev, "--auto-approve"}
			},
			synced: "Schema is synced, no changes to be made",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := newSpacedSchemaDatabase(c, "spaced_apply")
			dev := newEmptyReadScopeDatabase(c, "spaced_apply_dev")
			dir := filepath.Dir(writeSpacedSchemaFile(c, "schema.sql", spacedSchemaSQL))
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.hcl"), []byte(spacedSchemaHCL), 0o600), qt.IsNil)

			applied := clirun.Run(c, test.program, clirun.Options{Dir: c.TempDir()}, test.args(database, dev, dir)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(spacedSchemaTables(c, database), qt.Equals, spacedSchemaApplied)

			again := clirun.Run(c, test.program, clirun.Options{Dir: c.TempDir()}, test.args(database, dev, dir)...)

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, test.synced)
			c.Assert(spacedSchemaTables(c, database), qt.Equals, spacedSchemaApplied)
		})
	}
}

// TestSpacedSchemaNameInspectE2E inspects, on the compatibility surface, a
// database holding `a.t_plain` and ` a`.t_space, and the document the issue
// reports. Each run describes the schema it names, and only that one:
// PTAH_ATLAS_STRICT_COMPAT=1 reads `-s ' a'` as Atlas CE v1.4.0 does, as the
// schema ` a`, and the default surface trims the value, as its flag grammar
// says, and describes `a`.
func TestSpacedSchemaNameInspectE2E(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		// args is the command line, given the database, a dev database and
		// the path of the issue's document.
		args    func(database, dev, document string) []string
		present []string
		absent  []string
	}{
		{
			name: "the whole database, as SQL",
			args: func(database, _, _ string) []string {
				return []string{"schema", "inspect", "-u", database, "--format", "{{ sql . }}"}
			},
			present: []string{
				`CREATE SCHEMA IF NOT EXISTS " a";`,
				`CREATE TABLE " a"."t_space" ("id" integer NULL);`,
				`CREATE SCHEMA IF NOT EXISTS "a";`,
				`CREATE TABLE "a"."t_plain" ("id" integer NULL);`,
			},
		},
		{
			name: "the whole database, as HCL",
			args: func(database, _, _ string) []string {
				return []string{"schema", "inspect", "-u", database}
			},
			present: []string{"schema \" a\" {\n}", "schema = schema[\" a\"]", "schema \"a\" {\n}", "schema = schema.a"},
		},
		{
			name: "the issue's document, through the dev database",
			args: func(_, dev, document string) []string {
				return []string{"schema", "inspect", "-u", "file://" + filepath.ToSlash(document), "--dev-url", dev, "--format", "{{ sql . }}"}
			},
			present: []string{`CREATE SCHEMA IF NOT EXISTS " a";`, `CREATE TABLE " a"."t" ("id" integer NOT NULL);`},
			absent:  []string{`"a"`},
		},
		{
			name: "strict mode, -s ' a'",
			env:  []string{"PTAH_ATLAS_STRICT_COMPAT=1"},
			args: func(database, _, _ string) []string {
				return []string{"schema", "inspect", "-u", database, "-s", " a", "--format", "{{ sql . }}"}
			},
			present: []string{`CREATE TABLE " a"."t_space" ("id" integer NULL);`},
			absent:  []string{"t_plain"},
		},
		{
			name: "the default surface, -s ' a'",
			args: func(database, _, _ string) []string {
				return []string{"schema", "inspect", "-u", database, "-s", " a", "--format", "{{ sql . }}"}
			},
			present: []string{`CREATE TABLE "a"."t_plain" ("id" integer NULL);`},
			absent:  []string{"t_space"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := newEmptyReadScopeDatabase(c, "spaced_inspect")
			for _, statement := range []string{
				"CREATE SCHEMA a", `CREATE SCHEMA " a"`,
				"CREATE TABLE a.t_plain (id integer)", `CREATE TABLE " a".t_space (id integer)`,
			} {
				execPostgresFamilySQL(c, c.Context(), database, statement)
			}
			dev := newEmptyReadScopeDatabase(c, "spaced_inspect_dev")
			document := writeSpacedSchemaFile(c, "schema.hcl", spacedSchemaIssueHCL)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), test.env...)},
				test.args(database, dev, document)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			for _, part := range test.present {
				c.Assert(got.Stdout, qt.Contains, part)
			}
			for _, part := range test.absent {
				c.Assert(got.Stdout, qt.Not(qt.Contains), part)
			}
		})
	}
}
