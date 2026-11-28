//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// These tests grant a role privileges on a function, on a procedure and on the
// database itself on MySQL and MariaDB, and read back what the server holds
// with SHOW GRANTS. A grant on a routine is `GRANT EXECUTE ON FUNCTION f1`,
// and a grant on the database itself is `GRANT ... ON *`, which names the
// database the session selected.

// routineGrantRoutines declares the table t1, the function f1 and the
// procedure p1 that the grants below name.
const routineGrantRoutines = "CREATE TABLE t1 (id INT PRIMARY KEY);\n" +
	"CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1;\n" +
	"CREATE PROCEDURE p1() BEGIN SELECT 1; END;\n"

// routineGrantRole is a role name of at most 32 characters, as MySQL allows,
// dropped from each server behind dbs when the test ends.
func routineGrantRole(c *qt.C, prefix string, dbs ...*sql.DB) string {
	c.Helper()
	role := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()%1_000_000_000)
	for _, db := range dbs {
		c.Cleanup(func() {
			_, err := db.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
			c.Check(err, qt.IsNil)
		})
	}
	return role
}

// roleGrantLines is what SHOW GRANTS lists for role, sorted, without the host
// MySQL names a role with and MariaDB does not, so one expectation holds on
// both servers.
func roleGrantLines(c *qt.C, db *sql.DB, role string) []string {
	c.Helper()
	rows := grantRows(c, db, "SHOW GRANTS FOR "+role)
	for i, row := range rows {
		rows[i] = strings.TrimSuffix(row, "@`%`")
	}
	slices.Sort(rows)
	return rows
}

// writeRoutineGrantMigration writes the first migration of a directory: the
// table, the routines and the role, with the grants statements make.
func writeRoutineGrantMigration(c *qt.C, role string, statements ...string) string {
	c.Helper()
	dir := c.TempDir()
	up := "-- +ptah no_transaction\n" + routineGrantRoutines + "CREATE ROLE " + role + ";\n" + strings.Join(statements, "")
	for name, body := range map[string]string{
		"0000000001_base.up.sql":   up,
		"0000000001_base.down.sql": "DROP ROLE " + role + ";\nDROP PROCEDURE p1;\nDROP FUNCTION f1;\nDROP TABLE t1;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// TestMigrationsCheckpointReplaysRoutineAndDatabaseGrantsE2E checkpoints a
// history that grants a role EXECUTE on f1, EXECUTE and ALTER ROUTINE on p1,
// and INSERT on the database itself, then replays the directory into a
// database of another server. Without the routine grants the role loses
// EXECUTE silently, and a grant on the database that names the shadow
// database lands there instead of on the replay's. The checkpoint names the
// routines alone and the database as `*`, and on the replay's server the role
// holds each privilege on the replay database and nothing else. The
// checkpoint is marked no_transaction, since `migrations up` refuses a routine
// definition in its default transaction mode file. The shadow is on the other
// server because the role is the server's, and a DROP DATABASE keeps the
// grants the history's replay gave it on the shadow.
func TestMigrationsCheckpointReplaysRoutineAndDatabaseGrantsE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			shadowScratch := newMySQLFamilyScratch(c, engine.dev)
			shadowName, shadow := shadowScratch.database(c, "ckp_rg")
			replayName, replay := scratch.database(c, "ckp_rgr")
			role := routineGrantRole(c, "ptah_ckg", scratch.admin, shadowScratch.admin)
			dir := writeRoutineGrantMigration(c, role,
				"GRANT EXECUTE ON FUNCTION f1 TO "+role+";\n",
				"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE p1 TO "+role+";\n",
				"GRANT INSERT ON * TO "+role+";\n")
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}

			checkpoint := clirun.Run(c, clirun.Ptah, options, "migrations", "checkpoint", "--migrations-dir", dir,
				"--shadow-db", shadow, "--trust-replay-sql")
			c.Assert(checkpoint.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", checkpoint.Stdout, checkpoint.Stderr))
			up, err := os.ReadFile(filepath.Join(dir, "0000000002_checkpoint.checkpoint.up.sql"))
			c.Assert(err, qt.IsNil)
			replayed := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--migrations-dir", dir, "--db-url", replay)

			c.Assert(string(up), qt.Matches, `(?s)-- \+ptah no_transaction\n.*`)
			c.Assert(string(up), qt.Contains, "GRANT EXECUTE ON FUNCTION `f1` TO `"+role+"`;")
			c.Assert(string(up), qt.Contains, "GRANT INSERT ON * TO `"+role+"`;")
			c.Assert(string(up), qt.Not(qt.Contains), shadowName)
			c.Assert(replayed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replayed.Stdout, replayed.Stderr))
			c.Assert(roleGrantLines(c, scratch.admin, role), qt.DeepEquals, []string{
				"GRANT EXECUTE ON FUNCTION `" + replayName + "`.`f1` TO `" + role + "`",
				"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE `" + replayName + "`.`p1` TO `" + role + "`",
				"GRANT INSERT ON `" + replayName + "`.* TO `" + role + "`",
				"GRANT USAGE ON *.* TO `" + role + "`",
			})
		})
	}
}

// TestMigrationsCheckpointAtlasLayoutReplaysARoutineE2E checkpoints an Atlas
// layout history that creates a function, then replays the directory into
// another database. `migrations up` refuses a routine definition in its
// default transaction mode file, so the checkpoint carries
// `-- atlas:txmode none` in its directive block, and the replay creates the
// function.
func TestMigrationsCheckpointAtlasLayoutReplaysARoutineE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.dev)
			_, shadow := scratch.database(c, "ckp_atl")
			replayName, replay := scratch.database(c, "ckp_atlr")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "20250101000000_base.sql"),
				[]byte("-- atlas:txmode none\n\n"+routineGrantRoutines), 0o600), qt.IsNil)
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
			hashed := clirun.Run(c, clirun.Ptah, options, "migrations", "hash", "--dir", dir, "--dir-format", "atlas")
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))

			checkpoint := clirun.Run(c, clirun.Ptah, options, "migrations", "checkpoint", "--migrations-dir", dir,
				"--dir-format", "atlas", "--shadow-db", shadow, "--trust-replay-sql")
			c.Assert(checkpoint.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", checkpoint.Stdout, checkpoint.Stderr))
			written, err := filepath.Glob(filepath.Join(dir, "*_checkpoint.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 1)
			body, err := os.ReadFile(written[0])
			c.Assert(err, qt.IsNil)
			replayed := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--migrations-dir", dir,
				"--dir-format", "atlas", "--db-url", replay)

			c.Assert(string(body), qt.Matches, "(?s)-- atlas:checkpoint\n-- atlas:txmode none\n\n.*CREATE FUNCTION `f1`.*")
			c.Assert(replayed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replayed.Stdout, replayed.Stderr))
			c.Assert(scratch.databaseObjects(c, replayName), qt.Equals, "FUNCTION f1,PROCEDURE p1")
		})
	}
}

// TestMigrationsGenerateRevokesRoutineAndDatabaseGrantsE2E generates, from a
// replay on a dev database of another server, the migration that takes a
// role's EXECUTE on f1 and its INSERT on the database itself, and applies it
// to the target. The read of the dev database names the database grant with
// the dev database's name, and a REVOKE written so takes nothing on the
// target. The migration names the routine alone and the database as `*`, so
// the role keeps only SELECT on t1 there.
func TestMigrationsGenerateRevokesRoutineAndDatabaseGrantsE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			devScratch := newMySQLFamilyScratch(c, engine.dev)
			targetName, target := scratch.database(c, "gen_rg")
			devName, dev := devScratch.database(c, "gen_rgd")
			role := routineGrantRole(c, "ptah_gng", scratch.admin, devScratch.admin)
			dir := writeRoutineGrantMigration(c, role,
				"GRANT SELECT ON t1 TO "+role+";\n",
				"GRANT EXECUTE ON FUNCTION f1 TO "+role+";\n",
				"GRANT INSERT ON * TO "+role+";\n")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(routineGrantRoutines+"CREATE ROLE "+role+";\nGRANT SELECT ON t1 TO "+role+";\n"),
				0o600), qt.IsNil)
			options := clirun.Options{
				Dir: c.TempDir(), Timeout: 2 * time.Minute,
				Env: append(environmentWithoutPtahVariables(), "PTAH_TRUST_REPLAY_SQL=1"),
			}
			base := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
			c.Assert(base.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", base.Stdout, base.Stderr))

			generated := clirun.Run(c, clirun.Ptah, options, "migrations", "generate", "--dev-url", dev, "--replay",
				"--migrations-dir", dir, "--schema-file", schema, "--name", "revoke_grants")
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			files := generatedFiles(c, dir, "revoke_grants")
			up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", target, "--migrations-dir", dir)

			c.Assert(files, qt.Contains, "REVOKE EXECUTE ON FUNCTION `f1` FROM `"+role+"`;")
			c.Assert(files, qt.Contains, "REVOKE INSERT ON * FROM `"+role+"`;")
			c.Assert(files, qt.Not(qt.Contains), devName)
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(roleGrantLines(c, scratch.admin, role), qt.DeepEquals, []string{
				"GRANT SELECT ON `" + targetName + "`.`t1` TO `" + role + "`",
				"GRANT USAGE ON *.* TO `" + role + "`",
			})
		})
	}
}

// routineGrantsDeclared gives the role %[1]s EXECUTE on f1, EXECUTE and ALTER
// ROUTINE on p1, and INSERT on the database itself.
const routineGrantsDeclared = routineGrantRoutines + "CREATE ROLE %[1]s;\n" +
	"GRANT EXECUTE ON FUNCTION f1 TO %[1]s;\n" +
	"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE p1 TO %[1]s;\n" +
	"GRANT INSERT ON * TO %[1]s;\n"

// An apply gives the role the declared privileges on the routines and on the
// database, whatever it held before: nothing, or EXECUTE on the function
// spelled F1, which is the grant on f1 since a routine name compares without
// case, beside SELECT on the database, which the schema leaves out. The next
// plan finds nothing to change.
func TestSchemaApplyMySQLFamilyRoutineAndDatabaseGrantsE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		held []string
	}{
		{name: "nothing held"},
		{
			name: "another case of the function and a database grant left out",
			held: []string{"GRANT EXECUTE ON FUNCTION `%[1]s`.F1 TO %[2]s", "GRANT SELECT ON `%[1]s`.* TO %[2]s"},
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunIn(c, engine.admin, "rg_apply", routineGrantRoutines, test.held)

				applied := run.apply(c, routineGrantsDeclared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				granted := roleGrantLines(c, run.scratch.admin, run.role)
				c.Assert(granted, qt.Contains, "GRANT INSERT ON `"+run.database+"`.* TO `"+run.role+"`")
				c.Assert(granted, qt.Contains, "GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE `"+run.database+"`.`p1` TO `"+run.role+"`")
				c.Assert(granted, qt.HasLen, 4)

				again := run.apply(c, routineGrantsDeclared, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// A routine whose body changes is replaced with DROP and CREATE, and the plan
// grants back what the DROP takes. A privilege the same plan revokes stays
// revoked: the role held EXECUTE and ALTER ROUTINE on p1 and the schema
// declares EXECUTE alone, so the role holds EXECUTE afterwards, and the next
// plan finds nothing to change.
func TestSchemaApplyMySQLFamilyReplacedRoutineKeepsARevokeE2E(t *testing.T) {
	const declared = "CREATE TABLE t1 (id INT PRIMARY KEY);\n" +
		"CREATE FUNCTION f1() RETURNS INT DETERMINISTIC RETURN 1;\n" +
		"CREATE PROCEDURE p1() BEGIN SELECT 2; END;\n" +
		"CREATE ROLE %[1]s;\n" +
		"GRANT EXECUTE ON PROCEDURE p1 TO %[1]s;\n"
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunIn(c, engine.admin, "rg_replace", routineGrantRoutines,
				[]string{"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE `%[1]s`.p1 TO %[2]s"})

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(roleGrantLines(c, run.scratch.admin, run.role), qt.DeepEquals, []string{
				"GRANT EXECUTE ON PROCEDURE `" + run.database + "`.`p1` TO `" + run.role + "`",
				"GRANT USAGE ON *.* TO `" + run.role + "`",
			})

			again := run.apply(c, declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// EXECUTE on a target named without FUNCTION or PROCEDURE is a grant on a
// table, which the server answers with ERROR 1144 after the statements before
// it ran. It is refused before anything runs, with the spelling to use: the
// table t2 the plan would create first does not exist afterwards, and the
// role holds nothing.
func TestSchemaApplyMySQLFamilyExecuteOnATableE2E_FailurePath(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunIn(c, engine.admin, "rg_refuse", routineGrantRoutines, nil)

			applied := run.apply(c, routineGrantRoutines+"CREATE TABLE t2 (id INT PRIMARY KEY);\n"+
				"CREATE ROLE %[1]s;\nGRANT EXECUTE ON f1 TO %[1]s;\n", "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", applied.Stdout))
			c.Assert(applied.Stderr, qt.Contains, "write ON FUNCTION f1 or ON PROCEDURE f1")
			c.Assert(run.scratch.tablesOf(c, run.database), qt.Equals, "t1")
			c.Assert(roleGrantLines(c, run.scratch.admin, run.role), qt.DeepEquals, []string{"GRANT USAGE ON *.* TO `" + run.role + "`"})
		})
	}
}
