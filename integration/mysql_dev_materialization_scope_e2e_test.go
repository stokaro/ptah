//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A document a command creates on a dev database stays in the dev database. On
// MySQL and MariaDB a schema is a database, so a document naming its tables
// with the target's name -- an HCL document has to, a SQL file may -- created
// them in the target database when it was materialized on a dev database of
// the same server. Each test reads the target back afterwards.

// devMaterializationDocuments are the documents each test materializes, both
// naming the target database: a SQL file and an HCL document.
func devMaterializationDocuments(c *qt.C, target string) map[string]string {
	c.Helper()
	dir := c.TempDir()
	documents := map[string]string{
		"schema.sql": fmt.Sprintf("CREATE TABLE %s.t9 (id int PRIMARY KEY);\n", target),
		"schema.hcl": fmt.Sprintf("schema %[1]q {}\ntable \"t9\" {\n  schema = schema.%[1]s\n  column \"id\" {\n"+
			"    type = int\n    null = false\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n", target),
	}
	paths := make(map[string]string, len(documents))
	for name, document := range documents {
		path := filepath.Join(dir, name)
		c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
		paths[name] = "file://" + path
	}
	return paths
}

// devMaterializationRuns are the commands that create a --from document on the
// dev database: the native diff and the Atlas-compatible one.
var devMaterializationRuns = []struct {
	name    string
	program clirun.Target
}{
	{name: "ptah schema diff", program: clirun.Ptah},
	{name: "ptah-compat schema diff", program: clirun.Compat},
}

// TestSchemaDiffMaterializesAFileOnTheDevDatabaseAloneE2E diffs a document
// declaring t9 in the target to the target, which holds t1, with a dev
// database of the same server. The plan creates t1 and drops t9, the target
// holds t1 alone afterwards, and the dev database is left empty.
func TestSchemaDiffMaterializesAFileOnTheDevDatabaseAloneE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, run := range devMaterializationRuns {
			for _, document := range []string{"schema.sql", "schema.hcl"} {
				t.Run(engine.name+"/"+run.name+"/"+document, func(t *testing.T) {
					c := qt.New(t)
					scratch := newMySQLFamilyScratch(c, engine.admin)
					target, targetURL := scratch.builtFrom(c, "CREATE TABLE t1 (id int PRIMARY KEY);")
					dev, devURL := scratch.database(c, "dev")
					from := devMaterializationDocuments(c, target)[document]

					diff := clirun.Run(c, run.program, clirun.Options{Dir: c.TempDir()},
						"schema", "diff", "--from", from, "--to", targetURL, "--dev-url", devURL)

					c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
					c.Assert(diff.Stdout, qt.Contains, "CREATE TABLE `t1`")
					c.Assert(diff.Stdout, qt.Contains, "DROP TABLE")
					c.Assert(diff.Stdout, qt.Contains, "`t9`")
					c.Assert(scratch.tablesOf(c, target), qt.Equals, "t1")
					c.Assert(scratch.tablesOf(c, dev), qt.Equals, "")
				})
			}
		}
	}
}

// TestCompatSchemaInspectMaterializesAFileOnTheDevDatabaseAloneE2E inspects the
// same documents through the dev database: the output describes t9, and the
// database the document names is not touched.
func TestCompatSchemaInspectMaterializesAFileOnTheDevDatabaseAloneE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, document := range []string{"schema.sql", "schema.hcl"} {
			t.Run(engine.name+"/"+document, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.admin)
				target, _ := scratch.builtFrom(c, "CREATE TABLE t1 (id int PRIMARY KEY);")
				dev, devURL := scratch.database(c, "dev")
				url := devMaterializationDocuments(c, target)[document]

				inspect := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
					"schema", "inspect", "--url", url, "--dev-url", devURL, "--format", "{{ sql . }}")

				c.Assert(inspect.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", inspect.Stdout, inspect.Stderr))
				c.Assert(inspect.Stdout, qt.Contains, "CREATE TABLE `t9`")
				c.Assert(scratch.tablesOf(c, target), qt.Equals, "t1")
				c.Assert(scratch.tablesOf(c, dev), qt.Equals, "")
			})
		}
	}
}

// TestSchemaDiffRefusesAFileNamingTwoDatabasesE2E diffs a SQL file naming the
// target and a second database. There is no one database to create it in on
// the dev database, so the diff is refused before anything runs, and neither
// database the file names gains a table.
func TestSchemaDiffRefusesAFileNamingTwoDatabasesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			target, targetURL := scratch.builtFrom(c, "CREATE TABLE t1 (id int PRIMARY KEY);")
			other, _ := scratch.database(c, "other")
			_, devURL := scratch.database(c, "dev")
			file := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(file, []byte(fmt.Sprintf(
				"CREATE TABLE %s.t9 (id int PRIMARY KEY);\nCREATE TABLE %s.t8 (id int PRIMARY KEY);\n", target, other)), 0o600), qt.IsNil)

			diff := runPtahIn(c, "schema", "diff", "--from", "file://"+file, "--to", targetURL, "--dev-url", devURL)

			c.Assert(diff.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", diff.Stdout))
			c.Assert(diff.Stderr, qt.Contains, "materialize schema on dev database: dev database simulation refused: statement 1 names schema")
			c.Assert(scratch.tablesOf(c, target), qt.Equals, "t1")
			c.Assert(scratch.tablesOf(c, other), qt.Equals, "")
		})
	}
}
