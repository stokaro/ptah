//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
)

// A single-database URL used to accept a list naming two databases, then apply
// only the first database's plan. Refusing that scope preserves both databases
// and their rows, and writes neither a saved plan nor migration files (#6558).
func TestNativeMySQLSchemasOutsideURLRefuseWithoutChangesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			first, address := scratch.builtAs(c, "selection_first", "CREATE TABLE items (id INT PRIMARY KEY);\nINSERT INTO items VALUES (7);\n")
			second, _ := scratch.builtAs(c, "selection_second", "CREATE TABLE items (id INT PRIMARY KEY);\nINSERT INTO items VALUES (11);\n")
			file := writeDeclaredSchemaFile(c, "replacement.sql", "CREATE TABLE replacement (id INT PRIMARY KEY);\n")
			artifacts := c.TempDir()
			opts := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute}
			selection := first + "," + second

			for _, args := range [][]string{
				{"schema", "apply", "--db-url", address, "--schema-file", file, "--schemas", selection, "--auto-approve"},
				{"schema", "plan", "--db-url", address, "--schema-file", file, "--schemas", selection, "--output", filepath.Join(artifacts, "plan.json")},
				{"migrations", "generate", "--db-url", address, "--schema-file", file, "--schemas", selection, "--migrations-dir", artifacts},
			} {
				run := clirun.Run(c, clirun.Ptah, opts, args...)

				c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("%s\nstdout:\n%s\nstderr:\n%s", args, run.Stdout, run.Stderr))
				c.Assert(run.Stderr, qt.Contains, fmt.Sprintf("schema selection names database %q, but the URL is limited to database %q", second, first))
				c.Assert(run.Stderr, qt.Contains, "remove the database from the URL")
				c.Assert(scratch.tablesOf(c, first), qt.Equals, "items")
				c.Assert(scratch.tablesOf(c, second), qt.Equals, "items")
				c.Assert(mysqlSchemaSelectionRow(c, scratch, first), qt.Equals, 7)
				c.Assert(mysqlSchemaSelectionRow(c, scratch, second), qt.Equals, 11)
				files, err := os.ReadDir(artifacts)
				c.Assert(err, qt.IsNil)
				c.Assert(files, qt.HasLen, 0)
			}
		})
	}
}

// Matching a single-database URL still applies and converges. Removing its
// database component permits the same two-name list and reads both databases.
func TestNativeMySQLSchemasMatchingURLAndWholeServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			first, address := scratch.builtAs(c, "selection_first", "CREATE TABLE items (id INT PRIMARY KEY);\nINSERT INTO items VALUES (7);\n")
			second, _ := scratch.builtAs(c, "selection_second", "CREATE TABLE items (id INT PRIMARY KEY);\nINSERT INTO items VALUES (11);\n")
			file := writeDeclaredSchemaFile(c, "alter.sql", "CREATE TABLE items (id INT PRIMARY KEY, note INT);\n")
			opts := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute}

			applied := clirun.Run(c, clirun.Ptah, opts, "schema", "apply", "--db-url", address,
				"--schema-file", file, "--schemas", first, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "ADD COLUMN")
			c.Assert(mysqlSchemaSelectionRow(c, scratch, first), qt.Equals, 7)
			c.Assert(mysqlSchemaSelectionRow(c, scratch, second), qt.Equals, 11)
			c.Assert(scratch.tablesOf(c, second), qt.Equals, "items")
			settled := clirun.Run(c, clirun.Ptah, opts, "schema", "apply", "--db-url", address,
				"--schema-file", file, "--schemas", first, "--dry-run")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			server, err := atlasurl.WithDatabaseName(scratch.adminURL, "")
			c.Assert(err, qt.IsNil)
			inspected := clirun.Run(c, clirun.Ptah, opts, "schema", "inspect", "--db-url", server,
				"--schemas", first+","+second, "--format", "sql")
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", inspected.Stdout, inspected.Stderr))
			c.Assert(inspected.Stdout, qt.Contains, "CREATE TABLE `"+first+"`.`items`")
			c.Assert(inspected.Stdout, qt.Contains, "CREATE TABLE `"+second+"`.`items`")
		})
	}
}

// mysqlSchemaSelectionRow reads the preserved row through the admin session,
// independently of the CLI's selected read or comparison.
func mysqlSchemaSelectionRow(c *qt.C, scratch mysqlScratch, database string) int {
	c.Helper()
	var id int
	// #nosec G202 -- the identifier is a database this test created and quotes.
	err := scratch.admin.QueryRowContext(c.Context(), "SELECT id FROM `"+strings.ReplaceAll(database, "`", "``")+"`.items").Scan(&id)
	c.Assert(err, qt.IsNil)
	return id
}
