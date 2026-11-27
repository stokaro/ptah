//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A comparison of a whole MySQL or MariaDB server drops a database the schema
// file does not declare with DROP DATABASE, which drops every table in it. A
// selection that leaves a table of that database out of the comparison must
// not let the plan drop the database, or the table and its rows go with it
// (stokaro/ptah#6797). The plan drops what the selection kept, one table at a
// time, and keeps the database. Each test reads the database back.
//
// Every run names its databases with --schemas, because a whole-server
// comparison drops every database the file does not declare, and the servers
// are shared with other tests.

// keepServers are the servers a whole-server comparison runs on: every
// MySQL-family release line a cell certifies, folding names, and the lines
// that keep names apart by case.
var keepServers = slices.Concat(foldedScopeServers, exactScopeServers)

// keepRun is a database the schema file does not declare, holding t and u with
// one row each, beside one the file declares.
type keepRun struct {
	scope  scopeRun
	app    string
	schema string
	hcl    string
}

// newKeepRun builds the two databases on server.
func newKeepRun(c *qt.C, server dbtarget.Engine) keepRun {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, server)
	held, heldURL := scratch.builtAs(c, "keep", scopeHistory)
	app, _ := scratch.builtAs(c, "keep_app", "CREATE TABLE a (id INT NOT NULL PRIMARY KEY);\n")
	run := keepRun{scope: scopeRun{scratch: scratch, held: held, heldURL: heldURL}, app: app}
	run.schema = run.scope.file(c, "schema.sql", fmt.Sprintf("CREATE TABLE `%s`.`a` (`id` int NOT NULL, PRIMARY KEY (`id`));\n", app))
	run.hcl = run.scope.file(c, "schema.hcl", fmt.Sprintf(`schema %[1]q {}
table "a" {
  schema = schema.%[1]s
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`, app))
	return run
}

// args is the run's --schemas scope, then selectors with <db> standing for
// the database the file does not declare.
func (r keepRun) args(flag string, selectors ...string) []string {
	args := []string{flag, r.scope.held + "," + r.app}
	for _, selector := range selectors {
		args = append(args, strings.ReplaceAll(selector, "<db>", r.scope.held))
	}
	return args
}

func (r keepRun) run(c *qt.C, target clirun.Target, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, target, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}, args...)
}

// A selection that leaves t out keeps the database: the apply drops u, and t
// keeps its row.
func TestSchemaApplyMySQLFamilyKeepsADatabaseASelectionLeavesPartOfE2E(t *testing.T) {
	tests := []struct {
		name      string
		selectors []string
	}{
		{name: "--exclude the table", selectors: []string{"--exclude", "<db>.t"}},
		{name: "--include the other table", selectors: []string{"--include", "<db>.u"}},
	}
	for _, server := range keepServers {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newKeepRun(c, server.server)

				applied := run.run(c, clirun.Ptah, append([]string{
					"schema", "apply", "--db-url", run.scope.scratch.adminURL, "--schema-file", run.schema, "--auto-approve",
				}, run.args("--schemas", test.selectors...)...)...)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(run.scope.rows(c, run.scope.held), qt.DeepEquals, []string{"t:1"})
			})
		}
	}
}

func TestCompatSchemaApplyMySQLFamilyKeepsADatabaseAnExclusionLeavesPartOfE2E(t *testing.T) {
	for _, server := range keepServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newKeepRun(c, server.server)

			applied := run.run(c, clirun.Compat, append([]string{
				"schema", "apply", "-u", run.scope.scratch.adminURL, "--to", "file://" + filepath.ToSlash(run.hcl), "--auto-approve",
			}, run.args("--schema", "--exclude", "<db>.t")...)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(run.scope.rows(c, run.scope.held), qt.DeepEquals, []string{"t:1"})
		})
	}
}

// The diff of the same comparison drops u and not the database.
// `ptah-compat schema diff` takes a --dev-url, which on a URL naming no
// database is a dev server it empties; the servers here are shared, so it is
// measured by hand.
func TestSchemaDiffMySQLFamilyKeepsADatabaseAnExclusionLeavesPartOfE2E(t *testing.T) {
	for _, server := range keepServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newKeepRun(c, server.server)

			diffed := run.run(c, clirun.Ptah, append([]string{
				"schema", "diff", "--from", run.scope.scratch.adminURL, "--to", "file://" + filepath.ToSlash(run.hcl),
			}, run.args("--schemas", "--exclude", "<db>.t")...)...)

			c.Assert(diffed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed.Stdout, diffed.Stderr))
			c.Assert(diffed.Stdout, qt.Contains, fmt.Sprintf("DROP TABLE `%s`.`u`;", run.scope.held))
			c.Assert(diffed.Stdout, qt.Not(qt.Contains), "DROP DATABASE")
			c.Assert(run.scope.rows(c, run.scope.held), qt.DeepEquals, []string{"t:1", "u:1"})
		})
	}
}

// On a server that keeps names apart by case, a whole-server read asked for
// the database the file declares and the one the server holds in another case
// reads both. MariaDB answers `SCHEMA_NAME IN ('DB', 'db')` with no row while
// it holds db, so a read that asked it that way left db out: `--exclude
// <db>.u` matched nothing, and the apply refused. Read whole, db is kept for
// the u the exclusion leaves out, t is dropped, and the declared database is
// created.
func TestSchemaApplyMySQLFamilyExactServerReadsADatabaseDeclaredInAnotherCaseE2E(t *testing.T) {
	for _, server := range exactScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, server.server)
			held, _ := scratch.builtAs(c, "case", scopeHistory)
			declared := strings.ToUpper(held)
			run := scopeRun{scratch: scratch, held: held, declared: declared}
			schema := run.file(c, "schema.sql", fmt.Sprintf(scopeSchema, declared))
			c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), scratch.admin, declared) })

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"schema", "apply", "--db-url", scratch.adminURL, "--schema-file", schema, "--auto-approve",
				"--schemas", declared+","+held, "--exclude", held+".u")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(run.rows(c, held), qt.DeepEquals, []string{"u:1"})
			c.Assert(run.rows(c, declared), qt.DeepEquals, []string{"t:0", "u:0"})
		})
	}
}
