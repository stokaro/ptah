//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
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

// TestSpacedSchemaNameGrantE2E applies a desired schema granting USAGE on
// ` a` to a database holding `a` and ` a`. The grant reaches ` a` and no
// privilege reaches `a`. Trimmed, the plan wrote `GRANT USAGE ON SCHEMA "a"`,
// a privilege on a schema the desired schema does not name.
func TestSpacedSchemaNameGrantE2E(t *testing.T) {
	c := qt.New(t)
	role := fmt.Sprintf("ptah_spaced_grant_%d", time.Now().UnixNano())
	admin := openReadScopeDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL))
	_, err := admin.ExecContext(c.Context(), "CREATE ROLE "+role)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		// Cleanups run last first, so the database holding the grant, created
		// below, is dropped before the role.
		_, err := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	database := newSpacedSchemaDatabase(c, "spaced_grant")
	execPostgresFamilySQL(c, c.Context(), database, `CREATE SCHEMA " a"`)
	schemaFile := writeSpacedSchemaFile(c, "schema.sql",
		"CREATE SCHEMA a;\nCREATE TABLE a.t (id integer);\nCREATE SCHEMA \" a\";\nGRANT USAGE ON SCHEMA \" a\" TO "+role+";\n")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"schema", "apply", "--db-url", database, "--schema-file", schemaFile, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	var privileges string
	err = openReadScopeDatabase(c, database).QueryRowContext(c.Context(),
		"SELECT '[a]=' || has_schema_privilege($1, 'a', 'USAGE') || ' [ a]=' || has_schema_privilege($1, ' a', 'USAGE')",
		role).Scan(&privileges)
	c.Assert(err, qt.IsNil)
	c.Assert(privileges, qt.Equals, "[a]=false [ a]=true")
}

// TestSpacedSchemaNamePlacementE2E applies a desired schema that writes a table
// without a schema, over a URL that pins no search_path, to a database whose
// own search_path is `"a "`. The session resolves to `a `, so the table is
// created there, and `a` stays empty. Trimmed, the plan wrote
// `CREATE TABLE "a"."placed"`.
func TestSpacedSchemaNamePlacementE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "spaced_placement")
	execPostgresFamilySQL(c, c.Context(), database, "CREATE SCHEMA a")
	execPostgresFamilySQL(c, c.Context(), database, `CREATE SCHEMA "a "`)
	var name string
	c.Assert(openReadScopeDatabase(c, database).QueryRowContext(c.Context(), "SELECT current_database()").Scan(&name), qt.IsNil)
	// Identifier syntax: a string literal would be one list element, quotes
	// included.
	execPostgresFamilySQL(c, c.Context(), database, fmt.Sprintf(`ALTER DATABASE %q SET search_path = "a "`, name))
	schemaFile := writeSpacedSchemaFile(c, "schema.sql", "CREATE TABLE placed (id integer);\n")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"schema", "apply", "--db-url", database, "--schema-file", schemaFile, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	var placed string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(),
		"SELECT string_agg('[' || table_schema || '].' || table_name, ', ') FROM information_schema.tables WHERE table_name = 'placed'",
	).Scan(&placed)
	c.Assert(err, qt.IsNil)
	c.Assert(placed, qt.Equals, "[a ].placed")
}

// TestSpacedSchemaNameRemovalE2E applies a desired schema that declares `a.t`
// and the schema ` a` to a database holding a row in `a.t` and the table
// ` a`.t. The removal drops ` a`.t, and `a.t` keeps its row. Trimmed, the plan
// wrote `DROP TABLE "a"."t"` and destroyed the declared table.
func TestSpacedSchemaNameRemovalE2E(t *testing.T) {
	c := qt.New(t)
	database := newSpacedSchemaDatabase(c, "spaced_removal")
	execPostgresFamilySQL(c, c.Context(), database, "INSERT INTO a.t VALUES (1)")
	execPostgresFamilySQL(c, c.Context(), database, `CREATE SCHEMA " a"`)
	execPostgresFamilySQL(c, c.Context(), database, `CREATE TABLE " a".t (id integer)`)
	schemaFile := writeSpacedSchemaFile(c, "schema.sql", "CREATE SCHEMA a;\nCREATE TABLE a.t (id integer);\nCREATE SCHEMA \" a\";\n")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"schema", "apply", "--db-url", database, "--schema-file", schemaFile, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(spacedSchemaTables(c, database), qt.Equals, "[a].t(id YES)")
	var rows int
	c.Assert(openReadScopeDatabase(c, database).QueryRowContext(c.Context(), "SELECT count(*) FROM a.t").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}
