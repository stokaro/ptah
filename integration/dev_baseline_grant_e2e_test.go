//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
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
