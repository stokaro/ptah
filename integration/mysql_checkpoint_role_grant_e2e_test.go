//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// TestMigrationsCheckpointReplaysARoleGrantIntoAnotherDatabaseE2E checkpoints
// a history that grants a role SELECT on t1 and UPDATE on its column name,
// then replays the directory into another database of the same server. The
// read of the shadow database names each grant's table with the shadow's
// name, and a checkpoint that writes it so fails on the replay with Error
// 1146, Table 'shadow.t1' doesn't exist, measured on MySQL 26.10 and MariaDB
// 12.3. The checkpoint names the table alone, the replay applies it, and the
// role holds both privileges on the replay database's t1.
func TestMigrationsCheckpointReplaysARoleGrantIntoAnotherDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.dev)
			shadowName, shadow := scratch.database(c, "ckp_role")
			replayName, replay := scratch.database(c, "ckp_replay")
			// A MySQL role name holds at most 32 characters.
			role := fmt.Sprintf("ptah_ckr_%d", time.Now().UnixNano()%1_000_000_000)
			c.Cleanup(func() {
				_, err := scratch.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
				c.Check(err, qt.IsNil)
			})
			dir := c.TempDir()
			for name, body := range map[string]string{
				"0000000001_base.up.sql": "CREATE TABLE t1 (id int PRIMARY KEY, name varchar(20));\nCREATE ROLE " + role + ";\n" +
					"GRANT SELECT ON t1 TO " + role + ";\nGRANT UPDATE (name) ON t1 TO " + role + ";\n",
				"0000000001_base.down.sql": "DROP ROLE " + role + ";\nDROP TABLE t1;\n",
			} {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
			}
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}

			checkpoint := clirun.Run(c, clirun.Ptah, options, "migrations", "checkpoint", "--migrations-dir", dir,
				"--shadow-db", shadow, "--trust-replay-sql")
			up, err := os.ReadFile(filepath.Join(dir, "0000000002_checkpoint.checkpoint.up.sql"))
			c.Assert(err, qt.IsNil)
			replayed := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--migrations-dir", dir, "--db-url", replay)

			c.Assert(checkpoint.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", checkpoint.Stdout, checkpoint.Stderr))
			c.Assert(string(up), qt.Contains, "GRANT SELECT ON `t1` TO `"+role+"`;")
			c.Assert(string(up), qt.Not(qt.Contains), shadowName)
			c.Assert(replayed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replayed.Stdout, replayed.Stderr))
			// MySQL names the role with its host, `role`@`%`; MariaDB does not.
			c.Assert(grantRows(c, scratch.admin, "SHOW GRANTS FOR "+role), qt.Any(qt.Matches),
				regexp.QuoteMeta("GRANT SELECT, UPDATE (`name`) ON `"+replayName+"`.`t1` TO `"+role+"`")+"(@`%`)?")
		})
	}
}
