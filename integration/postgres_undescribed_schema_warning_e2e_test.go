//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A URL that pins no search_path reads every schema of the database, so a
// schema the desired schema declares nothing in is compared as absent and its
// tables are planned for removal. Each comparison verb says so on standard
// error, one line per schema, unless the operator named the scope with
// search_path or --schemas. The database holds public.users and the two tables
// of crm, and the desired schema declares public.users alone.

// undescribedDeclared is the desired schema every test compares with.
const undescribedDeclared = "CREATE TABLE public.users (id integer PRIMARY KEY);\n"

const (
	// undescribedWarning is the line every verb prints for crm.
	undescribedWarning = `warning: the desired schema declares nothing in schema "crm", so its 2 tables are planned for removal; ` +
		"to keep them, pin search_path in the URL or pass --schemas\n"
	// undescribedAnyWarning is the part of the warning every schema's line
	// carries.
	undescribedAnyWarning = "declares nothing in schema"
	undescribedDropFirst  = `DROP TABLE IF EXISTS "crm"."customers" CASCADE;`
	undescribedDropSecond = `DROP TABLE IF EXISTS "crm"."orders" CASCADE;`
)

// undescribedRun is one verb run against the fixture database.
type undescribedRun struct {
	name string
	// args is the command line, given the database URL, which pins no
	// search_path, the desired schema file and an empty migration directory.
	args func(database, schema, migrations string) []string
	// stdout is a part of standard output that shows the verb compared.
	stdout   string
	exitCode int
}

// TestComparisonVerbsWarnAboutAnUndescribedSchemaE2E runs each verb with no
// scope named. Each plans the removal of both crm tables and warns once about
// crm on standard error.
func TestComparisonVerbsWarnAboutAnUndescribedSchemaE2E(t *testing.T) {
	c := qt.New(t)
	database := newUndescribedSchemaDatabase(c)
	tests := []undescribedRun{
		{
			name: "schema compare",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "compare", "--db-url", database, "--schema-file", schema}
			},
			stdout: undescribedDropFirst,
		},
		{
			name: "migrations plan",
			args: func(database, schema, _ string) []string {
				return []string{"migrations", "plan", "--db-url", database, "--schema-file", schema}
			},
			stdout: undescribedDropFirst,
		},
		{
			name: "schema drift as JSON",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "drift", "--db-url", database, "--schema-file", schema, "--format", "json"}
			},
			stdout:   `"tables_removed": [`,
			exitCode: 1,
		},
		{
			name: "schema apply --dry-run",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run"}
			},
			stdout: undescribedDropFirst,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			run := runPtahIn(c, test.args(database, writeUndescribedSchema(c), c.TempDir())...)

			c.Assert(run.ExitCode, qt.Equals, test.exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.stdout)
			c.Assert(run.Stdout, qt.Not(qt.Contains), undescribedAnyWarning)
			c.Assert(run.Stderr, qt.Contains, undescribedWarning)
			c.Assert(strings.Count(run.Stderr, undescribedAnyWarning), qt.Equals, 1)
		})
	}
}

// TestMigrationsGenerateWarnsAboutTheDropsItWritesE2E is the case the warning
// exists for: `migrations generate` writes both drops into a file.
func TestMigrationsGenerateWarnsAboutTheDropsItWritesE2E(t *testing.T) {
	c := qt.New(t)
	database := newUndescribedSchemaDatabase(c)
	migrations := c.TempDir()

	generated := runPtahIn(c, "migrations", "generate", "--db-url", database, "--schema-file", writeUndescribedSchema(c), "--migrations-dir", migrations)

	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	c.Assert(generated.Stderr, qt.Contains, undescribedWarning)
	c.Assert(strings.Count(generated.Stderr, undescribedAnyWarning), qt.Equals, 1)
	up := readOneUpMigration(c, migrations)
	c.Assert(up, qt.Contains, undescribedDropFirst)
	c.Assert(up, qt.Contains, undescribedDropSecond)
}

// TestComparisonVerbsDoNotWarnWhenTheScopeIsNamedE2E names the scope each way
// an operator can. No line warns, while each verb still compares: a scope of
// public finds the database synced, and a selection naming crm removes its
// tables because the operator asked for crm. A plan that skips table drops,
// with `diff.skip: [drop_table]` in ptah.yaml, drops no table and warns about
// none either.
func TestComparisonVerbsDoNotWarnWhenTheScopeIsNamedE2E(t *testing.T) {
	c := qt.New(t)
	database := newUndescribedSchemaDatabase(c)
	skipDrops := filepath.Join(c.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(skipDrops, []byte("diff:\n  skip: [drop_table]\n"), 0o600), qt.IsNil)
	tests := []undescribedRun{
		{
			name: "schema compare, search_path=public",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "compare", "--db-url", withURLParameter(database, "search_path", "public"), "--schema-file", schema}
			},
			stdout: readScopeSynced,
		},
		{
			name: "migrations generate, search_path=public",
			args: func(database, schema, migrations string) []string {
				return []string{"migrations", "generate", "--db-url", withURLParameter(database, "search_path", "public"),
					"--schema-file", schema, "--migrations-dir", migrations}
			},
			stdout: readScopeNoChange,
		},
		{
			name: "schema apply --dry-run, search_path=public",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "apply", "--db-url", withURLParameter(database, "search_path", "public"),
					"--schema-file", schema, "--dry-run"}
			},
			stdout: "Schema is synced, no changes to be made.\n",
		},
		{
			name: "schema compare --schemas public",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "compare", "--db-url", database, "--schema-file", schema, "--schemas", "public"}
			},
			stdout: readScopeSynced,
		},
		{
			name: "migrations generate --schemas public",
			args: func(database, schema, migrations string) []string {
				return []string{"migrations", "generate", "--db-url", database, "--schema-file", schema,
					"--migrations-dir", migrations, "--schemas", "public"}
			},
			stdout: readScopeNoChange,
		},
		{
			name: "schema drift --schemas public",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "drift", "--db-url", database, "--schema-file", schema, "--schemas", "public"}
			},
			stdout: readScopeNoDrift,
		},
		{
			name: "schema compare --schemas public,crm",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "compare", "--db-url", database, "--schema-file", schema, "--schemas", "public,crm"}
			},
			stdout: undescribedDropFirst,
		},
		{
			name: "migrations generate, table drops skipped",
			args: func(database, schema, migrations string) []string {
				return []string{"migrations", "generate", "--config", skipDrops, "--db-url", database,
					"--schema-file", schema, "--migrations-dir", migrations}
			},
			stdout: readScopeNoChange,
		},
		{
			name: "schema apply --dry-run, table drops skipped",
			args: func(database, schema, _ string) []string {
				return []string{"schema", "apply", "--config", skipDrops, "--db-url", database, "--schema-file", schema, "--dry-run"}
			},
			stdout: "Schema is synced, no changes to be made.\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			run := runPtahIn(c, test.args(database, writeUndescribedSchema(c), c.TempDir())...)

			c.Assert(run.ExitCode, qt.Equals, test.exitCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.stdout)
			c.Assert(run.Stderr, qt.Not(qt.Contains), undescribedAnyWarning)
		})
	}
}

// newUndescribedSchemaDatabase creates a database, dropped when the test ends,
// holding public.users, crm.customers and crm.orders, and returns its URL,
// which pins no search_path.
func newUndescribedSchemaDatabase(c *qt.C) string {
	c.Helper()
	database := newEmptyReadScopeDatabase(c, "undescribed")
	for _, statement := range []string{
		"CREATE SCHEMA crm",
		"CREATE TABLE public.users (id integer PRIMARY KEY)",
		"CREATE TABLE crm.customers (id integer PRIMARY KEY)",
		"CREATE TABLE crm.orders (id integer PRIMARY KEY)",
	} {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	return database
}

// writeUndescribedSchema writes the desired schema to a file and returns its
// path.
func writeUndescribedSchema(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(undescribedDeclared), 0o600), qt.IsNil)
	return path
}
