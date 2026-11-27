//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A server started with lower_case_table_names=1 holds one database for the
// names Sales and sales. A selection that keeps one spelling and leaves out the
// other makes the plan drop what the document declares: `schema apply
// --schemas sales` for a document declaring the database as Sales keeps the
// held database, leaves the declared one out, and plans DROP DATABASE
// (stokaro/ptah#6785). Each test reads the database back after the apply.

// foldedScopeServers are the servers that fold names, one per release line a
// cell certifies, each reached through a URL naming no database.
var foldedScopeServers = []struct {
	name   string
	server dbtarget.Engine
}{
	{name: "MySQL 8.4", server: dbtarget.MySQL84FoldedServer},
	{name: "MySQL 26.10", server: dbtarget.MySQLFoldedServer},
	{name: "MariaDB 11.8", server: dbtarget.MariaDB118FoldedServer},
	{name: "MariaDB 12.3", server: dbtarget.MariaDBFoldedServer},
}

// exactScopeServers keep names apart by case, and are the control.
var exactScopeServers = []struct {
	name   string
	server dbtarget.Engine
}{
	{name: "MySQL", server: dbtarget.MySQLDevServer},
	{name: "MariaDB", server: dbtarget.MariaDBDevServer},
}

// scopeHistory is what the held database holds: t and u, one row each.
const scopeHistory = "CREATE TABLE t (id INT NOT NULL PRIMARY KEY);\nCREATE TABLE u (id INT NOT NULL PRIMARY KEY);\n" +
	"INSERT INTO t (id) VALUES (1);\nINSERT INTO u (id) VALUES (1);\n"

// scopeSchema declares the database %[1]s with t and u.
const scopeSchema = "CREATE DATABASE `%[1]s`;\n" +
	"CREATE TABLE `%[1]s`.`t` (`id` int NOT NULL, PRIMARY KEY (`id`));\n" +
	"CREATE TABLE `%[1]s`.`u` (`id` int NOT NULL, PRIMARY KEY (`id`));\n"

// scopeHCL declares the database %[1]s with t and u in Atlas HCL, which
// ptah-compat reads without a dev database.
const scopeHCL = `schema "%[1]s" {}
table "t" {
  schema = schema.%[1]s
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
table "u" {
  schema = schema.%[1]s
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

// scopeTables declares t, and u in upper case, for a URL naming the database.
const scopeTables = "CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`));\n" +
	"CREATE TABLE `U` (`id` int NOT NULL, PRIMARY KEY (`id`));\n"

// scopeRun is a database the server holds under a lower-case name and a
// schema file that declares it in upper case.
type scopeRun struct {
	scratch  mysqlScratch
	held     string
	heldURL  string
	declared string
	schema   string
}

// newScopeRun builds the held database on server, after asserting that the
// server's lower_case_table_names is folding.
func newScopeRun(c *qt.C, server dbtarget.Engine, folding int) scopeRun {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, server)
	var setting int
	c.Assert(scratch.admin.QueryRowContext(c.Context(), "SELECT @@lower_case_table_names").Scan(&setting), qt.IsNil)
	c.Assert(setting, qt.Equals, folding)
	held, heldURL := scratch.builtAs(c, "scope", scopeHistory)
	run := scopeRun{
		scratch: scratch, held: held, heldURL: heldURL, declared: strings.ToUpper(held),
		schema: filepath.Join(c.TempDir(), "schema.sql"),
	}
	c.Assert(os.WriteFile(run.schema, fmt.Appendf(nil, scopeSchema, run.declared), 0o600), qt.IsNil)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), scratch.admin, run.declared) })
	return run
}

// args is selectors with <db> standing for the held name and <DB> for the
// declared one.
func (r scopeRun) args(selectors ...string) []string {
	spelled := make([]string, len(selectors))
	for i, selector := range selectors {
		spelled[i] = strings.NewReplacer("<db>", r.held, "<DB>", r.declared).Replace(selector)
	}
	return spelled
}

// apply runs `ptah schema apply` for the whole server with selectors.
func (r scopeRun) apply(c *qt.C, selectors ...string) clirun.Result {
	c.Helper()
	args := append([]string{"schema", "apply", "--db-url", r.scratch.adminURL, "--schema-file", r.schema, "--auto-approve"},
		r.args(selectors...)...)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}, args...)
}

// file writes body to a new file and returns its path.
func (r scopeRun) file(c *qt.C, name, body string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// rows is each table the database holds, with its row count, in name order,
// and empty for a database the server does not hold.
func (r scopeRun) rows(c *qt.C, database string) []string {
	c.Helper()
	tables, err := r.scratch.admin.QueryContext(c.Context(), `
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE CAST(TABLE_SCHEMA AS BINARY) = CAST(? AS BINARY) ORDER BY TABLE_NAME`, database)
	c.Assert(err, qt.IsNil)
	var names []string
	for tables.Next() {
		var name string
		c.Assert(tables.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(tables.Err(), qt.IsNil)
	c.Assert(tables.Close(), qt.IsNil)
	held := make([]string, 0, len(names))
	for _, name := range names {
		var count int
		// #nosec G202 -- the database and the table are names this test created.
		c.Assert(r.scratch.admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+database+"`.`"+name+"`").Scan(&count), qt.IsNil)
		held = append(held, fmt.Sprintf("%s:%d", name, count))
	}
	return held
}

// databasesOf is which of the run's two names the server holds a database
// under, in byte order.
func (r scopeRun) databasesOf(c *qt.C) []string {
	c.Helper()
	held, err := r.scratch.admin.QueryContext(c.Context(), `
		SELECT SCHEMA_NAME FROM information_schema.SCHEMATA
		WHERE CAST(SCHEMA_NAME AS BINARY) IN (CAST(? AS BINARY), CAST(? AS BINARY))
		ORDER BY CAST(SCHEMA_NAME AS BINARY)`, r.held, r.declared)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(held.Close(), qt.IsNil) }()
	var names []string
	for held.Next() {
		var name string
		c.Assert(held.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(held.Err(), qt.IsNil)
	return names
}

// On a server that folds names, a selection spelled either way keeps both
// sides of the database: the apply plans nothing, and t and u keep their rows.
//
// An --exclude alone leaves every other database on the server in scope, and
// the file declares none of them, so an exclusion is run inside --schemas: the
// servers are shared with other tests.
func TestSchemaApplyMySQLFamilyFoldedScopeE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		selectors []string
	}{
		{name: "--schemas as the server holds it", selectors: []string{"--schemas", "<db>"}},
		{name: "--schemas as the document spells it", selectors: []string{"--schemas", "<DB>"}},
		{name: "--schemas in both spellings", selectors: []string{"--schemas", "<db>,<DB>"}},
		{name: "--include a table as the server holds it", selectors: []string{"--include", "<db>.t"}},
		{name: "--exclude a table as the server holds it", selectors: []string{"--schemas", "<db>", "--exclude", "<db>.u"}},
	}
	for _, server := range foldedScopeServers {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newScopeRun(c, server.server, 1)

				applied := run.apply(c, test.selectors...)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
				c.Assert(run.rows(c, run.held), qt.DeepEquals, []string{"t:1", "u:1"})
			})
		}
	}
}

// A selector spelled as the document spells the database matches nothing once
// both sides are spelled as the server holds the name, and the apply refuses
// it rather than run without the protection it asked for.
func TestSchemaApplyMySQLFamilyFoldedScopeE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		want     string
	}{
		{name: "a table", selector: "<DB>.u", want: `the --exclude selection matched no objects: "%s.u"`},
		{name: "the database", selector: "<DB>", want: `the --exclude selection matched no objects: "%s"`},
	}
	for _, server := range foldedScopeServers {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newScopeRun(c, server.server, 1)

				applied := run.apply(c, "--schemas", "<db>", "--exclude", test.selector)

				c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stderr, qt.Contains, fmt.Sprintf(test.want, run.declared))
				c.Assert(run.rows(c, run.held), qt.DeepEquals, []string{"t:1", "u:1"})
			})
		}
	}
}

// The control: on a server that keeps names apart by case, the declared name
// is another database. `--schemas` spelled as the document spells it creates
// that database, and the held one keeps its rows.
func TestSchemaApplyMySQLFamilyExactScopeE2E_KeepsTwoDatabases(t *testing.T) {
	for _, server := range exactScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 0)

			applied := run.apply(c, "--schemas", "<DB>")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(run.rows(c, run.held), qt.DeepEquals, []string{"t:1", "u:1"})
			c.Assert(run.rows(c, run.declared), qt.DeepEquals, []string{"t:0", "u:0"})
		})
	}
}

// On a server that keeps names apart by case, a selection of both spellings
// takes two databases: the document declares one and not the other, so the
// plan creates the declared database and drops the held one. The comparison
// records that the server keeps the names apart, and the planner runs the
// pair it refuses on a server that folds them.
func TestSchemaApplyMySQLFamilyExactScopeE2E_PlansTheNamesakePair(t *testing.T) {
	for _, server := range exactScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 0)

			applied := run.apply(c, "--schemas", "<db>,<DB>")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "DROP DATABASE `"+run.held+"`;")
			c.Assert(run.databasesOf(c), qt.DeepEquals, []string{run.declared})
			c.Assert(run.rows(c, run.declared), qt.DeepEquals, []string{"t:0", "u:0"})
		})
	}
}

// ptah-compat adapts the same selection: `--schema` spelled as the server
// holds the database keeps both sides of it.
func TestSchemaApplyMySQLFamilyFoldedScopeE2E_Compat(t *testing.T) {
	for _, server := range foldedScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 1)
			document := run.file(c, "schema.hcl", fmt.Sprintf(scopeHCL, run.declared))

			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"schema", "apply", "--url", run.scratch.adminURL, "--to", "file://"+filepath.ToSlash(document),
				"--schema", run.held, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made")
			c.Assert(run.rows(c, run.held), qt.DeepEquals, []string{"t:1", "u:1"})
		})
	}
}

// `schema diff` from the server to the file selects the same way, and finds
// nothing to change.
func TestSchemaDiffMySQLFamilyFoldedScopeE2E(t *testing.T) {
	for _, server := range foldedScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 1)

			diffed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"schema", "diff", "--from", run.scratch.adminURL, "--to", run.schema, "--schemas", run.held)

			c.Assert(diffed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed.Stdout, diffed.Stderr))
			c.Assert(diffed.Stdout, qt.Contains, "No changes found in the visible scope.")
			c.Assert(diffed.Stdout, qt.Not(qt.Contains), "DROP")
		})
	}
}

// For a URL naming the database, a table selector spelled as the server holds
// the table meets the table the file declares in another case: `migrations
// plan` and `migrations generate` find nothing to change.
func TestMigrationsPlanMySQLFamilyFoldedSelectionE2E_HappyPath(t *testing.T) {
	for _, server := range foldedScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 1)

			planned := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"migrations", "plan", "--db-url", run.heldURL, "--schema-file", run.file(c, "tables.sql", scopeTables),
				"--exclude", "u")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Contains, "Safety: no executable migration statements")
		})
	}
}

func TestMigrationsGenerateMySQLFamilyFoldedSelectionE2E(t *testing.T) {
	for _, server := range foldedScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 1)
			dir := c.TempDir()

			generated := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"migrations", "generate", "--db-url", run.heldURL, "--schema-file", run.file(c, "tables.sql", scopeTables),
				"--exclude", "u", "--migrations-dir", dir)

			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			c.Assert(generated.Stdout, qt.Contains, "no migration files generated")
			entries, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
}

// A table selector spelled as the file spells the table matches nothing once
// both sides are spelled as the server holds it, and an --include that selects
// nothing is refused.
func TestMigrationsPlanMySQLFamilyFoldedSelectionE2E_FailurePath(t *testing.T) {
	for _, server := range foldedScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 1)

			planned := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"migrations", "plan", "--db-url", run.heldURL, "--schema-file", run.file(c, "tables.sql", scopeTables),
				"--include", "U")

			c.Assert(planned.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stderr, qt.Contains, `the --include selection matched no objects: "U"`)
			c.Assert(planned.Stdout, qt.Not(qt.Contains), "CREATE TABLE")
		})
	}
}
