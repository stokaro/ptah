//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/replaytrust"
)

// A rehearsal writes the target's grants onto the dev database with the rest
// of its schema. A one-database MySQL or MariaDB read names the database of
// each grant's object, and the dev database has another name than the target,
// so a grant has to land on the dev database's object. The dev database is on
// a server of its own, declared disposable, because a rehearsal refuses a
// role or a privilege on a dev server it does not own: neither is confined to
// a database.

// TestSchemaApplyRehearsesAGrantOnADevServerE2E plans a grant against a target
// whose table already holds one, which the baseline recreates on the dev
// database before the plan runs there. On a dev server where the target's
// database does not exist, a grant left naming it is refused with "Table
// doesn't exist" and the apply with it.
func TestSchemaApplyRehearsesAGrantOnADevServerE2E(t *testing.T) {
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(t)
	for _, engine := range mysqlDevServerEngines {
		for _, run := range devBaselineRuns {
			t.Run(engine.name+"/"+run.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.admin)
				devServer := newMySQLFamilyScratch(c, engine.dev)
				// A MySQL role name holds at most 32 characters.
				role := fmt.Sprintf("ptah_dgrant_%d", time.Now().UnixNano())
				for _, server := range []mysqlScratch{scratch, devServer} {
					c.Cleanup(func() {
						_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
						c.Check(err, qt.IsNil)
					})
				}
				_, targetURL := scratch.builtFrom(c, "CREATE TABLE t1 (id int PRIMARY KEY);\n"+
					"CREATE ROLE IF NOT EXISTS "+role+";\nGRANT SELECT ON t1 TO "+role+";\n")
				dev, devURL := devServer.database(c, "dev")
				schema := devBaselineSchema(c, "CREATE TABLE t1 (id int PRIMARY KEY);\nCREATE ROLE "+role+";\n"+
					"GRANT SELECT ON t1 TO "+role+";\nGRANT INSERT ON t1 TO "+role+";\n")

				got := clirun.Run(c, run.program, clirun.Options{Dir: c.TempDir()}, run.args(targetURL, schema, devURL)...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "GRANT INSERT ON `t1` TO `"+role+"`")
				c.Assert(devServer.tablesOf(c, dev), qt.Equals, "")
			})
		}
	}
}

// TestSchemaApplyRehearsesAClickHouseGrantOnASiblingDevDatabaseE2E is the same
// plan on ClickHouse, with the dev database on the target's server. The
// replayed SQL is trusted, as a rehearsal refuses a role or a privilege it
// cannot confine to the dev database. A ClickHouse grant names its database,
// and one left naming none is refused, so the baseline writes the target's
// grant onto the dev database's table. The server's query log shows that no
// grant the rehearsal sends names the target's database.
func TestSchemaApplyRehearsesAClickHouseGrantOnASiblingDevDatabaseE2E(t *testing.T) {
	envbooltest.Set(replaytrust.EnvVar, "1")(t)
	for _, run := range devBaselineRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			server := dbtarget.URL(c, dbtarget.ClickHouse)
			admin := connectDevDialect(c, server)
			role := fmt.Sprintf("ptah_dgrant_%d", time.Now().UnixNano())
			// A ClickHouse grant on a dropped table outlives it, so the role
			// goes when the test ends.
			c.Cleanup(func() {
				_, err := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
				c.Check(err, qt.IsNil)
			})
			targetURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			target := connectDevDialect(c, targetURL)
			var database string
			c.Assert(target.QueryRowContext(c.Context(), "SELECT currentDatabase()").Scan(&database), qt.IsNil)
			for _, statement := range []string{
				"CREATE TABLE items (id Int32) ENGINE = MergeTree ORDER BY id",
				"CREATE ROLE " + role,
				"GRANT SELECT ON items TO " + role,
			} {
				_, err := target.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
			devURL := createdDevDialectDatabase(c, server, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			schema := devBaselineSchema(c, "CREATE TABLE items (id Int32) ENGINE = MergeTree ORDER BY id;\n"+
				"CREATE ROLE "+role+";\nGRANT SELECT ON "+database+".items TO "+role+";\n"+
				"GRANT INSERT ON "+database+".items TO "+role+";\n")
			var since int64
			c.Assert(admin.QueryRowContext(c.Context(), "SELECT toUnixTimestamp64Micro(now64(6))").Scan(&since), qt.IsNil)

			got := clirun.Run(c, run.program, clirun.Options{Dir: c.TempDir()}, run.args(targetURL, schema, devURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "GRANT INSERT ON `"+database+"`.`items` TO `"+role+"`")
			_, err := admin.ExecContext(c.Context(), "SYSTEM FLUSH LOGS")
			c.Assert(err, qt.IsNil)
			var grantsOnTarget int64
			c.Assert(admin.QueryRowContext(c.Context(),
				"SELECT toInt64(count()) FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Grant' "+
					"AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, ?) > 0",
				since, database).Scan(&grantsOnTarget), qt.IsNil)
			c.Assert(grantsOnTarget, qt.Equals, int64(0))
			var left int64
			c.Assert(connectDevDialect(c, devURL).QueryRowContext(c.Context(),
				"SELECT toInt64(count()) FROM system.tables WHERE database = currentDatabase()").Scan(&left), qt.IsNil)
			c.Assert(left, qt.Equals, int64(0))
		})
	}
}
