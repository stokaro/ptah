//go:build integration

package excludedschema_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

var savedPlanConsumers = []struct {
	name, urlFlag string
	target        clirun.Target
}{
	{name: "native", urlFlag: "--db-url", target: clirun.Ptah},
	{name: "compat JSON", urlFlag: "--url", target: clirun.Compat},
}

// planBeforeExcludedTable writes a plan while its excluded table is absent.
// A schema plan permits an unmatched exclusion with a warning.
func planBeforeExcludedTable(c *qt.C, engine dbtarget.Engine, desired string) (*sql.DB, string, string, string) {
	c.Helper()
	admin, database, server := excludedTableDatabase(c, engine)
	_, err := admin.ExecContext(c.Context(), "DROP TABLE `"+database+"`.t")
	c.Assert(err, qt.IsNil)
	root := c.TempDir()
	source := filepath.Join(root, "desired.sql")
	c.Assert(os.WriteFile(source, []byte(strings.ReplaceAll(desired, "<database>", database)), 0o600), qt.IsNil)
	plan := filepath.Join(root, "reviewed.plan.json")
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
		"schema", "plan", "--db-url", server, "--schema-file", source, "--schemas", database,
		"--exclude", database+".t", "--output", plan)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	return admin, database, server, plan
}

func createExcludedTableAfterPlanning(c *qt.C, admin *sql.DB, database string) {
	c.Helper()
	_, err := admin.ExecContext(c.Context(), "CREATE TABLE `"+database+"`.t (id INT NOT NULL PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(c.Context(), "INSERT INTO `"+database+"`.t VALUES (42)")
	c.Assert(err, qt.IsNil)
}

// The filtered fingerprint cannot see a newly created excluded table. The
// saved database drop must still be refused before either table is changed.
func TestMySQLFamilySavedPlanRefusesNewExcludedTableE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, consumer := range savedPlanConsumers {
				t.Run(consumer.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, server, plan := planBeforeExcludedTable(c, engine.admin, "-- No database is declared.\n")
					contents, err := os.ReadFile(plan)
					c.Assert(err, qt.IsNil)
					c.Assert(string(contents), qt.Contains, "DROP DATABASE `"+database+"`")
					createExcludedTableAfterPlanning(c, admin, database)

					result := clirun.Run(c, consumer.target, clirun.Options{Dir: filepath.Dir(plan)},
						"schema", "apply", consumer.urlFlag, server, "--plan", plan, "--auto-approve")

					c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					c.Assert(result.Stderr, qt.Contains, "cannot drop database")
					c.Assert(result.Stderr, qt.Contains, database+".t")
					var protected, neighboring int
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT id FROM `"+database+"`.t").Scan(&protected), qt.IsNil)
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT id FROM `"+database+"`.u").Scan(&neighboring), qt.IsNil)
					c.Assert(protected, qt.Equals, 42)
					c.Assert(neighboring, qt.Equals, 1)
				})
			}
		})
	}
}

// An excluded table may appear after planning an unrelated column addition.
// The presence of protection metadata alone must not make that plan stale.
func TestMySQLFamilySavedPlanAllowsUnrelatedExcludedTableE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, consumer := range savedPlanConsumers {
				t.Run(consumer.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, server, plan := planBeforeExcludedTable(c, engine.admin,
						"CREATE DATABASE `<database>`;\nCREATE TABLE `<database>`.u (id INT NOT NULL PRIMARY KEY, note INT);\n")
					createExcludedTableAfterPlanning(c, admin, database)

					result := clirun.Run(c, consumer.target, clirun.Options{Dir: filepath.Dir(plan)},
						"schema", "apply", consumer.urlFlag, server, "--plan", plan, "--auto-approve")

					c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					var protected int
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT id FROM `"+database+"`.t").Scan(&protected), qt.IsNil)
					c.Assert(protected, qt.Equals, 42)
					var note sql.NullInt64
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT note FROM `"+database+"`.u WHERE id = 1").Scan(&note), qt.IsNil)
					c.Assert(note.Valid, qt.IsFalse)
				})
			}
		})
	}
}
