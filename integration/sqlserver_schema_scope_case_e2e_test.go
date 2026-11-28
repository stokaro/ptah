//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A SQL Server database under a case-insensitive collation holds one schema
// for the names Sales and sales. A selection matches the names it meets as
// they are written, so `schema apply --schemas sales` for a document that
// declares sales.t, against a database holding Sales.t, keeps one spelling and
// leaves out the other, and the plan creates the table the database holds,
// which fails with 2714 (stokaro/ptah#6786); spelled the other way round it
// drops the table. Each test reads the database back after the apply.

// sqlServerScopeHistory is what the held schema holds: Sales.t and Sales.u,
// one row each. The schema is created in a batch of its own.
var sqlServerScopeHistory = []string{
	"CREATE SCHEMA Sales",
	"CREATE TABLE Sales.t (id int NOT NULL PRIMARY KEY); CREATE TABLE Sales.u (id int NOT NULL PRIMARY KEY);" +
		"INSERT INTO Sales.t (id) VALUES (1); INSERT INTO Sales.u (id) VALUES (1);",
}

// sqlServerScopeSchema declares the schema in lower case with t and u.
const sqlServerScopeSchema = "CREATE SCHEMA sales;\n" +
	"CREATE TABLE sales.t (id int NOT NULL PRIMARY KEY);\n" +
	"CREATE TABLE sales.u (id int NOT NULL PRIMARY KEY);\n"

// sqlServerScopeRun is a database under one collation holding the schema
// Sales, and a schema file declaring it as sales.
type sqlServerScopeRun struct {
	url    string
	schema string
}

// newSQLServerScopeRun creates the database under collation and builds it.
func newSQLServerScopeRun(c *qt.C, collation string) sqlServerScopeRun {
	c.Helper()
	run := sqlServerScopeRun{url: sqlServerCollatedDatabase(c, collation), schema: filepath.Join(c.TempDir(), "schema.sql")}
	conn := connectDevDialect(c, run.url)
	for _, batch := range sqlServerScopeHistory {
		_, err := conn.ExecContext(c.Context(), batch)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", batch))
	}
	c.Assert(os.WriteFile(run.schema, []byte(sqlServerScopeSchema), 0o600), qt.IsNil)
	return run
}

// apply runs `ptah schema apply` with selectors.
func (r sqlServerScopeRun) apply(c *qt.C, selectors ...string) clirun.Result {
	c.Helper()
	return r.applyAt(c, r.url, selectors...)
}

// applyAt runs `ptah schema apply` against dbURL with selectors.
func (r sqlServerScopeRun) applyAt(c *qt.C, dbURL string, selectors ...string) clirun.Result {
	c.Helper()
	args := append([]string{"schema", "apply", "--db-url", dbURL, "--schema-file", r.schema, "--auto-approve"}, selectors...)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}, args...)
}

// inSchema is the run's URL with its schema parameter set to schema.
func (r sqlServerScopeRun) inSchema(c *qt.C, schema string) string {
	c.Helper()
	parsed, err := url.Parse(r.url)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("schema", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// tables is each table of the schemas Sales and sales, with its row count,
// in byte order.
func (r sqlServerScopeRun) tables(c *qt.C) string {
	c.Helper()
	return readCaseOnlyFact(c, r.url, `SELECT ISNULL(STRING_AGG(CONVERT(nvarchar(max), s.name + N'.' + t.name + N':' +
			CONVERT(nvarchar(20), p.rows)), N',') WITHIN GROUP (ORDER BY s.name COLLATE Latin1_General_BIN, t.name), N'')
		FROM sys.tables AS t
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		JOIN sys.partitions AS p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
		WHERE s.name COLLATE Latin1_General_BIN IN (N'Sales', N'sales')`)
}

// Under a case-insensitive collation a selection spelled either way keeps
// both sides of the schema: the apply plans nothing, and the tables keep their
// rows.
func TestSchemaApplySQLServerCaseInsensitiveScopeE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		selectors []string
	}{
		{name: "--schemas as the document spells it", selectors: []string{"--schemas", "sales"}},
		{name: "--schemas as the database holds it", selectors: []string{"--schemas", "Sales"}},
		{name: "--include a table as the database holds it", selectors: []string{"--include", "Sales.t"}},
		{name: "--exclude a table as the database holds it", selectors: []string{"--exclude", "Sales.u"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newSQLServerScopeRun(c, "Latin1_General_100_CI_AS")

			applied := run.apply(c, test.selectors...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(run.tables(c), qt.Equals, "Sales.t:1,Sales.u:1")
		})
	}
}

// A URL whose schema parameter names the schema as the database holds it reads
// that schema, and a --schemas name spelled as the document spells it is that
// schema too.
func TestSchemaApplySQLServerCaseInsensitiveScopeE2E_URLSchema(t *testing.T) {
	c := qt.New(t)
	run := newSQLServerScopeRun(c, "Latin1_General_100_CI_AS")

	applied := run.applyAt(c, run.inSchema(c, "Sales"), "--schemas", "sales")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(run.tables(c), qt.Equals, "Sales.t:1,Sales.u:1")
}

// The compatibility surface adapts the same selection: `--schema` spelled as
// the document spells it keeps both sides.
func TestSchemaApplySQLServerCaseInsensitiveScopeE2E_Compat(t *testing.T) {
	c := qt.New(t)
	run := newSQLServerScopeRun(c, "Latin1_General_100_CI_AS")
	dev := sqlServerCollatedDatabase(c, "Latin1_General_100_CI_AS")

	applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"schema", "apply", "--url", run.url, "--to", "file://"+filepath.ToSlash(run.schema), "--schema", "sales",
		"--dev-url", dev, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made")
	c.Assert(run.tables(c), qt.Equals, "Sales.t:1,Sales.u:1")
}

// A selector spelled as the document spells the schema matches nothing once
// both sides are spelled as the database holds it, and the apply refuses it
// rather than run without the protection it asked for.
func TestSchemaApplySQLServerCaseInsensitiveScopeE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		selector string
	}{
		{name: "a table", selector: "sales.u"},
		{name: "the schema", selector: "sales"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newSQLServerScopeRun(c, "Latin1_General_100_CI_AS")

			applied := run.apply(c, "--exclude", test.selector)

			c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, fmt.Sprintf("the --exclude selection matched no objects: %q", test.selector))
			c.Assert(run.tables(c), qt.Equals, "Sales.t:1,Sales.u:1")
		})
	}
}

// The control: under a case-sensitive collation sales is another schema, so
// `--schemas sales` creates it beside Sales, which keeps its rows.
func TestSchemaApplySQLServerCaseSensitiveScopeE2E_KeepsTwoSchemas(t *testing.T) {
	c := qt.New(t)
	run := newSQLServerScopeRun(c, "Latin1_General_100_CS_AS")

	applied := run.apply(c, "--schemas", "sales")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(run.tables(c), qt.Equals, "Sales.t:1,Sales.u:1,sales.t:0,sales.u:0")
}

// sqlServerAccount creates a login and a db_owner user for it in the database
// at dbURL whose default schema is schema, and returns dbURL connecting as it.
// The login is dropped when the test ends, after the database.
func sqlServerAccount(c *qt.C, dbURL, schema string) string {
	c.Helper()
	admin := connectDevDialect(c, dbtarget.URL(c, dbtarget.SQLServer))
	login := fmt.Sprintf("ptah_scope_%d", time.Now().UnixNano()%1_000_000_000_000)
	password := fmt.Sprintf("Sc0pe-%d-Aa!", time.Now().UnixNano())
	_, err := admin.ExecContext(c.Context(), fmt.Sprintf("CREATE LOGIN [%s] WITH PASSWORD = N'%s', CHECK_POLICY = OFF", login, password))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), fmt.Sprintf("DROP LOGIN [%s]", login))
		c.Check(dropErr, qt.IsNil)
	})
	conn := connectDevDialect(c, dbURL)
	for _, batch := range []string{
		"CREATE SCHEMA " + schema,
		fmt.Sprintf("CREATE USER [%s] FOR LOGIN [%s] WITH DEFAULT_SCHEMA = [%s]", login, login, schema),
		fmt.Sprintf("ALTER ROLE db_owner ADD MEMBER [%s]", login),
	} {
		_, err := conn.ExecContext(c.Context(), batch)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", batch))
	}
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(login, password)
	return parsed.String()
}

// routineBodies is the schema, the name and the returned value of every scalar
// function the database holds, in byte order.
func routineBodies(c *qt.C, dbURL string) string {
	c.Helper()
	return readCaseOnlyFact(c, dbURL, `SELECT ISNULL(STRING_AGG(CONVERT(nvarchar(max), SCHEMA_NAME(o.schema_id) + N'.' + o.name + N':' +
			CASE WHEN m.definition LIKE N'%RETURN 5%' THEN N'5' WHEN m.definition LIKE N'%RETURN 2%' THEN N'2' ELSE N'1' END), N',')
		WITHIN GROUP (ORDER BY SCHEMA_NAME(o.schema_id) COLLATE Latin1_General_BIN), N'')
		FROM sys.objects AS o JOIN sys.sql_modules AS m ON m.object_id = o.object_id
		WHERE o.type = 'FN'`)
}

// For an account whose default schema is not dbo, a routine the schema
// declares without a schema is the one in that default schema, where the
// server creates a name written without one, and the plan replaces that
// routine by its qualified name. dbo holds a routine of the same name, which
// the plan does not name and the apply leaves as it is.
func TestSchemaApplySQLServerRoutineReplacementForAnAccountOutsideDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerCollatedDatabase(c, "Latin1_General_100_CI_AS")
	account := sqlServerAccount(c, dbURL, "app")
	conn := connectDevDialect(c, dbURL)
	for _, batch := range []string{
		"CREATE FUNCTION dbo.f() RETURNS int AS BEGIN RETURN 1 END",
		"CREATE FUNCTION app.f() RETURNS int AS BEGIN RETURN 2 END",
	} {
		_, err := conn.ExecContext(c.Context(), batch)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", batch))
	}
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE FUNCTION f() RETURNS int AS BEGIN RETURN 5 END;\nGO\n"), 0o600), qt.IsNil)
	apply := func() clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
			"schema", "apply", "--db-url", account, "--schema-file", schema, "--auto-approve")
	}

	applied := apply()
	again := apply()

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "DROP FUNCTION [app].[f];")
	c.Assert(applied.Stdout, qt.Contains, "CREATE OR ALTER FUNCTION [app].[f]()")
	c.Assert(applied.Stdout, qt.Not(qt.Matches), `(?s).*FUNCTION (IF EXISTS )?\[f\].*`)
	c.Assert(routineBodies(c, dbURL), qt.Equals, "app.f:5,dbo.f:1")
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// `migrations plan`, `migrations generate` and `schema drift` select the same
// way: a selector spelled as the database holds the schema meets the schema
// the file declares in another case, and the comparison finds nothing to
// change.
var sqlServerMigrationSelections = []struct {
	name      string
	selectors []string
}{
	{name: "--exclude a table as the database holds it", selectors: []string{"--exclude", "Sales.u"}},
	{name: "--include a table as the database holds it", selectors: []string{"--include", "Sales.t"}},
}

func TestMigrationsPlanSQLServerCaseInsensitiveSelectionE2E(t *testing.T) {
	for _, test := range sqlServerMigrationSelections {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newSQLServerScopeRun(c, "Latin1_General_100_CI_AS")

			planned := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				append([]string{"migrations", "plan", "--db-url", run.url, "--schema-file", run.schema}, test.selectors...)...)

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Contains, "Safety: no executable migration statements")
		})
	}
}

func TestMigrationsGenerateSQLServerCaseInsensitiveSelectionE2E(t *testing.T) {
	for _, test := range sqlServerMigrationSelections {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newSQLServerScopeRun(c, "Latin1_General_100_CI_AS")
			dir := c.TempDir()

			generated := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				append([]string{"migrations", "generate", "--db-url", run.url, "--schema-file", run.schema, "--migrations-dir", dir},
					test.selectors...)...)

			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			c.Assert(generated.Stdout, qt.Contains, "no migration files generated")
			entries, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
}

// `ptah-compat migrate diff --schema` replays the directory on the dev
// database and selects from the replay as it holds the names: a directory
// that creates Sales.t and a file declaring sales.t are synced.
func TestMigrateDiffSQLServerCaseInsensitiveScopeE2E(t *testing.T) {
	c := qt.New(t)
	dev := sqlServerCollatedDatabase(c, "Latin1_General_100_CI_AS")
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"),
		[]byte("CREATE SCHEMA [Sales];\nGO\nCREATE TABLE [Sales].[t] ([id] int NOT NULL PRIMARY KEY);\nGO\n"), 0o600), qt.IsNil)
	document := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(document, []byte("CREATE SCHEMA sales;\nCREATE TABLE sales.t (id int NOT NULL PRIMARY KEY);\n"), 0o600), qt.IsNil)
	compat := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}, args...)
	}
	hashed := compat("migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))

	diffed := compat("migrate", "diff", "v2", "--dir", "file://"+filepath.ToSlash(dir),
		"--to", "file://"+filepath.ToSlash(document), "--schema", "Sales", "--dev-url", dev)

	c.Assert(diffed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed.Stdout, diffed.Stderr))
	c.Assert(diffed.Stdout, qt.Contains, "The migration directory is synced with the desired state, no changes to be made")
	written, err := filepath.Glob(filepath.Join(dir, "*_v2.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

// `schema compare` selects the same way, and reports no difference.
func TestSchemaCompareSQLServerCaseInsensitiveSelectionE2E(t *testing.T) {
	tests := []struct {
		name      string
		selectors []string
	}{
		{name: "--schemas as the document spells it", selectors: []string{"--schemas", "sales"}},
		{name: "--exclude a table as the database holds it", selectors: []string{"--exclude", "Sales.u"}},
		{name: "--include a table as the database holds it", selectors: []string{"--include", "Sales.t"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newSQLServerScopeRun(c, "Latin1_General_100_CI_AS")

			compared := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				append([]string{"schema", "compare", "--db-url", run.url, "--schema-file", run.schema}, test.selectors...)...)

			c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
			c.Assert(compared.Stdout, qt.Contains, "No schema differences detected.")
		})
	}
}

// `schema drift` selects the same way, and finds no drift.
func TestSchemaDriftSQLServerCaseInsensitiveSelectionE2E(t *testing.T) {
	for _, test := range sqlServerMigrationSelections {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newSQLServerScopeRun(c, "Latin1_General_100_CI_AS")

			drift := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				append([]string{"schema", "drift", "--db-url", run.url, "--schema-file", run.schema}, test.selectors...)...)

			c.Assert(drift.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", drift.Stdout, drift.Stderr))
			c.Assert(drift.Stdout, qt.Equals, "No schema drift detected.\n")
		})
	}
}
