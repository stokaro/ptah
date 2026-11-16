//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A rehearsal writes the target's current schema onto the dev database before
// it runs the plan there. Where a schema is a database, the target and the dev
// database are two databases of one server here, and each object the baseline
// writes has to land in the dev database, a view as much as a table. A MySQL
// routine or trigger is not rehearsed this way: the dev cleanup refuses a
// server where another database holds one, since it may reference the dev
// database. So these tests need a server whose other databases hold no
// routine and no trigger, as a CI service container is.
//
// Each plan drops the objects the baseline has to recreate, so the rehearsal
// fails on the dev database where one of them was left out or written into
// another database.

// devBaselineTarget holds a table and a view over it. The desired schema adds
// a column to the table and drops the view.
const (
	devBaselineTarget  = "CREATE TABLE t1 (id int PRIMARY KEY);\nCREATE SQL SECURITY DEFINER VIEW v1 AS SELECT id FROM t1;\n"
	devBaselineDesired = "CREATE TABLE t1 (id int PRIMARY KEY, note text);\n"
)

// devBaselineRoutineTarget adds a function to [devBaselineTarget], which the
// desired schema drops too: on a dev server of its own, the routine is
// rehearsed as well.
const devBaselineRoutineTarget = devBaselineTarget + "CREATE FUNCTION f1() RETURNS int DETERMINISTIC RETURN 1;\n"

// clickHouseDevBaselineTarget holds a table with a view and a materialized
// view over it, each stored the way Ptah creates one. The desired schema adds
// a column to the table and drops both views.
var clickHouseDevBaselineTarget = []string{
	"CREATE TABLE t1 (id Int32) ENGINE = MergeTree ORDER BY id",
	"CREATE VIEW v1 AS SELECT id FROM t1",
	"CREATE MATERIALIZED VIEW mv1 ENGINE = MergeTree ORDER BY tuple() AS SELECT id FROM t1",
}

const clickHouseDevBaselineDesired = "CREATE TABLE t1 (id Int32, note String) ENGINE = MergeTree ORDER BY id;\n"

// devBaselineRuns are the schema apply commands that rehearse a plan on a dev
// database: the native one and the Atlas-compatible one.
var devBaselineRuns = []struct {
	name    string
	program clirun.Target
	args    func(target, schema, dev string) []string
}{
	{
		name:    "ptah schema apply",
		program: clirun.Ptah,
		args: func(target, schema, dev string) []string {
			return []string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--dev-url", dev, "--dry-run"}
		},
	},
	{
		name:    "ptah-compat schema apply",
		program: clirun.Compat,
		args: func(target, schema, dev string) []string {
			return []string{"schema", "apply", "--url", target, "--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev, "--dry-run"}
		},
	},
}

// devBaselineSchema writes the desired schema and returns its path.
func devBaselineSchema(c *qt.C, schema string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyRehearsesAViewOnASiblingDevDatabaseE2E plans the added column
// and the dropped view against a target that also holds a view whose alias is
// spelled like the target's database, with a dev database of the same server.
// The rehearsal writes both views into the dev database, keeping the alias,
// and drops the first there; the dev database is left empty.
func TestSchemaApplyRehearsesAViewOnASiblingDevDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, run := range devBaselineRuns {
			t.Run(engine.name+"/"+run.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.admin)
				target, targetURL := scratch.builtFrom(c, devBaselineTarget)
				alias := "CREATE VIEW v_alias AS SELECT " + target + ".id FROM t1 " + target
				_, err := scratch.admin.ExecContext(c.Context(),
					"CREATE VIEW `"+target+"`.v_alias AS SELECT "+target+".id FROM `"+target+"`.t1 "+target)
				c.Assert(err, qt.IsNil)
				dev, devURL := scratch.database(c, "dev")
				schema := devBaselineSchema(c, devBaselineDesired+alias+";\n")

				got := clirun.Run(c, run.program, clirun.Options{Dir: c.TempDir()}, run.args(targetURL, schema, devURL)...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "ADD COLUMN `note`")
				c.Assert(got.Stdout, qt.Contains, "DROP VIEW `"+target+"`.`v1`")
				c.Assert(scratch.tablesOf(c, dev), qt.Equals, "")
			})
		}
	}
}

// TestSchemaApplyRehearsesAViewAndARoutineOnAProvisionedDevDatabaseE2E plans
// the same change against a target that also holds a function, dropped too,
// with a dev database of another name on a server a docker URL starts. The
// baseline creates the view and the function in that database, where the
// target's database does not exist, and the rehearsal drops both there.
func TestSchemaApplyRehearsesAViewAndARoutineOnAProvisionedDevDatabaseE2E(t *testing.T) {
	for _, engine := range ownedRehearsalEngines {
		for _, run := range devBaselineRuns {
			t.Run(engine.name+"/"+run.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.admin)
				target, targetURL := scratch.builtFrom(c, devBaselineRoutineTarget)
				schema := devBaselineSchema(c, devBaselineDesired)

				got := clirun.Run(c, run.program, clirun.Options{Dir: c.TempDir()}, run.args(targetURL, schema, engine.dockerDevURL+"/devdb")...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "ADD COLUMN `note`")
				c.Assert(got.Stdout, qt.Contains, "DROP VIEW `"+target+"`.`v1`")
				c.Assert(got.Stdout, qt.Contains, "DROP FUNCTION `"+target+"`.`f1`")
			})
		}
	}
}

// TestSchemaApplyRehearsesAMariaDBSequenceOnASiblingDevDatabaseE2E plans the
// drop of a sequence, which a MariaDB read records under the target's
// database, with a dev database of the same server. The baseline creates the
// sequence in the dev database and the rehearsal drops it there.
func TestSchemaApplyRehearsesAMariaDBSequenceOnASiblingDevDatabaseE2E(t *testing.T) {
	for _, run := range devBaselineRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin)
			target, targetURL := scratch.builtFrom(c, "CREATE TABLE t1 (id int PRIMARY KEY);\nCREATE SEQUENCE sq1;\n")
			dev, devURL := scratch.database(c, "dev")
			schema := devBaselineSchema(c, devBaselineDesired)

			got := clirun.Run(c, run.program, clirun.Options{Dir: c.TempDir()}, run.args(targetURL, schema, devURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "DROP SEQUENCE `"+target+"`.`sq1`")
			c.Assert(scratch.tablesOf(c, dev), qt.Equals, "")
		})
	}
}

// TestSchemaApplyRehearsesClickHouseViewsOnASiblingDevDatabaseE2E is the same
// plan on ClickHouse, whose read records the database on a view and on a
// materialized view alike: both are written into the dev database and dropped
// there, and it is left empty.
func TestSchemaApplyRehearsesClickHouseViewsOnASiblingDevDatabaseE2E(t *testing.T) {
	for _, run := range devBaselineRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			server := dbtarget.URL(c, dbtarget.ClickHouse)
			targetURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			target := connectDevDialect(c, targetURL)
			for _, statement := range clickHouseDevBaselineTarget {
				_, err := target.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
			devURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			schema := devBaselineSchema(c, clickHouseDevBaselineDesired)

			got := clirun.Run(c, run.program, clirun.Options{Dir: c.TempDir()}, run.args(targetURL, schema, devURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "ADD COLUMN note")
			c.Assert(got.Stdout, qt.Contains, "v1")
			c.Assert(got.Stdout, qt.Contains, "mv1")
			var left int64
			c.Assert(connectDevDialect(c, devURL).QueryRowContext(c.Context(),
				"SELECT toInt64(count()) FROM system.tables WHERE database = currentDatabase()").Scan(&left), qt.IsNil)
			c.Assert(left, qt.Equals, int64(0))
		})
	}
}
