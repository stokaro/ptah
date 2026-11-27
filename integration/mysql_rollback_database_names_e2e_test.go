//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A plan Ptah writes from a read of one database runs on another: a derived
// rollback is planned from a read of the shadow database and runs on the
// target, and a migration generated from a replay is planned from a read of
// the dev database and runs wherever the directory is applied. So no statement
// may name the database that was read. The tests below put a database of the
// read database's name on the target's server, where such a statement lands,
// and read back that it was left alone (stokaro/ptah#6710).

// databaseNameMigrations are, one version each, a table, then a function, a
// procedure, a trigger and a view over the table that calls the function, then
// their removal with an event created beside it. A replay refuses CREATE
// EVENT, so the event is in the version above the one a rollback restores.
// The versions have the ten digits of the files `migrations generate` writes
// beside them.
var databaseNameMigrations = []map[string]string{
	{
		"0000000001_base.up.sql":   "CREATE TABLE t (id int PRIMARY KEY, n int);\n",
		"0000000001_base.down.sql": "DROP TABLE t;\n",
	},
	{
		"0000000002_objects.up.sql": "-- atlas:txmode none\n\n" +
			"CREATE FUNCTION f_double(x int) RETURNS int DETERMINISTIC NO SQL RETURN x * 2;\n" +
			"CREATE PROCEDURE p_count() READS SQL DATA SELECT count(*) FROM t;\n" +
			"CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.n = f_double(NEW.id);\n" +
			"CREATE VIEW v AS SELECT id, f_double(n) AS d FROM t;\n",
		"0000000002_objects.down.sql": "-- atlas:txmode none\n\n" +
			"DROP VIEW v;\nDROP TRIGGER t_bi;\nDROP PROCEDURE p_count;\nDROP FUNCTION f_double;\n",
	},
	{
		"0000000003_drop.up.sql": "-- atlas:txmode none\n\n" +
			"DROP VIEW v;\nDROP TRIGGER t_bi;\nDROP PROCEDURE p_count;\nDROP FUNCTION f_double;\n" +
			"CREATE EVENT e_tick ON SCHEDULE EVERY 1 DAY DO DELETE FROM t WHERE id < 0;\n",
		"0000000003_drop.down.sql": "DROP EVENT e_tick;\n",
	},
}

// writeDatabaseNameMigrations writes the first versions of
// databaseNameMigrations.
func writeDatabaseNameMigrations(c *qt.C, versions int) string {
	c.Helper()
	dir := c.TempDir()
	for _, version := range databaseNameMigrations[:versions] {
		for name, body := range version {
			c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
		}
	}
	return dir
}

// databaseObjects answers the routines, triggers, events and views a database
// holds, as kind and name, in that order.
func (s mysqlScratch) databaseObjects(c *qt.C, database string) string {
	c.Helper()
	var objects string
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT COALESCE(GROUP_CONCAT(o ORDER BY k, o SEPARATOR ','), '') FROM (
			SELECT 1 AS k, CONCAT(ROUTINE_TYPE, ' ', ROUTINE_NAME) AS o FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?
			UNION ALL SELECT 2, CONCAT('TRIGGER ', TRIGGER_NAME) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?
			UNION ALL SELECT 3, CONCAT('EVENT ', EVENT_NAME) FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?
			UNION ALL SELECT 4, CONCAT('VIEW ', TABLE_NAME) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ?
		) objects`, database, database, database, database).Scan(&objects)
	c.Assert(err, qt.IsNil)
	return objects
}

// viewReads inserts a row into t of database and answers what its view reads
// back, as id:d, so the trigger, the view and the function each run.
func (s mysqlScratch) viewReads(c *qt.C, database string) string {
	c.Helper()
	conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, s.adminDSN, database))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), "INSERT INTO t (id) VALUES (21)")
	c.Assert(err, qt.IsNil)
	var got string
	err = conn.QueryRowContext(c.Context(), "SELECT COALESCE(GROUP_CONCAT(CONCAT(id, ':', d) ORDER BY id), '') FROM v").Scan(&got)
	c.Assert(err, qt.IsNil)
	return got
}

// namesake creates, on the target's server, a database of name, dropped when
// the test ends.
func (s mysqlScratch) namesake(c *qt.C, name string) {
	c.Helper()
	createMySQLDatabase(c, c.Context(), s.admin, name)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), s.admin, name) })
}

// TestMigrationsDownPlanNamesNoShadowDatabaseE2E rolls the target back from
// version 3 to version 2 with a rollback derived on a shadow database of
// another server. Version 3 removed the function, the procedure, the trigger
// and the view, so the rollback creates them from the shadow's read. It
// creates them in the target and names no database, and the database of the
// shadow's name on the target's server is left empty. The event version 3
// created is not modeled: the rollback leaves it, and says so.
func TestMigrationsDownPlanNamesNoShadowDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			targetName, target := scratch.database(c, "rb_target")
			shadowName, shadow := newMySQLFamilyScratch(c, engine.dev).database(c, "rb_shadow")
			scratch.namesake(c, shadowName)
			dir := writeDatabaseNameMigrations(c, 3)
			options := clirun.Options{
				Dir: c.TempDir(), Timeout: 2 * time.Minute,
				Env: append(environmentWithoutPtahVariables(), "PTAH_TRUST_REPLAY_SQL=1"),
			}
			up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", target, "--migrations-dir", dir,
				"--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(scratch.databaseObjects(c, targetName), qt.Equals, "EVENT e_tick")

			got := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--db-url", target, "--migrations-dir", dir,
				"--shadow-db", shadow, "--target", "2", "--plan", "--confirm")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "CREATE FUNCTION `f_double`")
			c.Assert(got.Stdout, qt.Contains, "CREATE PROCEDURE `p_count`")
			c.Assert(got.Stdout, qt.Contains, "CREATE TRIGGER `t_bi`")
			c.Assert(got.Stdout, qt.Contains, "VIEW `v` AS")
			c.Assert(got.Stdout, qt.Not(qt.Contains), "`"+shadowName+"`")
			c.Assert(got.Stdout, qt.Not(qt.Contains), "`"+targetName+"`")
			c.Assert(got.Stderr, qt.Contains, "e_tick")
			c.Assert(scratch.databaseObjects(c, targetName), qt.Equals,
				"FUNCTION f_double,PROCEDURE p_count,TRIGGER t_bi,EVENT e_tick,VIEW v")
			c.Assert(scratch.viewReads(c, targetName), qt.Equals, "21:84")
			c.Assert(scratch.databaseObjects(c, shadowName), qt.Equals, "")
			c.Assert(scratch.tablesOf(c, shadowName), qt.Equals, "")
		})
	}
}

// TestMigrationsGenerateReplayNamesNoDevDatabaseE2E generates, from a replay
// on a dev database of another server, the migration that removes the
// function, the procedure, the trigger and the view, and applies it, and
// then its rollback, on the target. The target's server also holds a database
// of the dev database's name with the same objects. The files name no
// database, so each statement runs on the target, and the namesake keeps
// every object.
func TestMigrationsGenerateReplayNamesNoDevDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			targetName, target := scratch.database(c, "gen_target")
			devName, dev := newMySQLFamilyScratch(c, engine.dev).database(c, "gen_dev")
			scratch.namesake(c, devName)
			namesake := asMySQLURL(replaceMySQLDatabaseName(c, scratch.adminURL, devName))
			dir := writeDatabaseNameMigrations(c, 2)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id int PRIMARY KEY, n int);\n"), 0o600), qt.IsNil)
			options := clirun.Options{
				Dir: c.TempDir(), Timeout: 2 * time.Minute,
				Env: append(environmentWithoutPtahVariables(), "PTAH_TRUST_REPLAY_SQL=1"),
			}
			for _, url := range []string{target, namesake} {
				up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", url, "--migrations-dir", dir)
				c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			}
			const all = "FUNCTION f_double,PROCEDURE p_count,TRIGGER t_bi,VIEW v"

			generated := clirun.Run(c, clirun.Ptah, options, "migrations", "generate", "--dev-url", dev, "--replay",
				"--migrations-dir", dir, "--schema-file", schema, "--name", "drop_objects")

			c.Assert(generated.ExitCode, qt.Equals, 0,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			files := generatedFiles(c, dir, "drop_objects")
			c.Assert(files, qt.Contains, "DROP FUNCTION `f_double`")
			c.Assert(files, qt.Contains, "VIEW `v` AS")
			c.Assert(files, qt.Not(qt.Contains), devName)
			c.Assert(files, qt.Not(qt.Contains), targetName)

			up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", target, "--migrations-dir", dir,
				"--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(scratch.databaseObjects(c, targetName), qt.Equals, "")
			c.Assert(scratch.databaseObjects(c, devName), qt.Equals, all)

			down := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--db-url", target, "--migrations-dir", dir,
				"--target", "2", "--confirm")
			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			c.Assert(scratch.databaseObjects(c, targetName), qt.Equals, all)
			c.Assert(scratch.viewReads(c, targetName), qt.Equals, "21:84")
			c.Assert(scratch.databaseObjects(c, devName), qt.Equals, all)
		})
	}
}

// TestMigrationsCheckpointNamesNoShadowDatabaseE2E squashes the table, the
// function, the procedure, the trigger and the view into a checkpoint
// replayed on a shadow database of another server. The checkpoint is read
// from the shadow, and it runs wherever the directory is applied, so it names
// no database.
func TestMigrationsCheckpointNamesNoShadowDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			shadowName, shadow := newMySQLFamilyScratch(c, engine.dev).database(c, "ckp_shadow")
			dir := writeDatabaseNameMigrations(c, 2)
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}

			got := clirun.Run(c, clirun.Ptah, options, "migrations", "checkpoint", "--migrations-dir", dir,
				"--shadow-db", shadow, "--trust-replay-sql", "--dry-run")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "CREATE FUNCTION `f_double`")
			c.Assert(got.Stdout, qt.Contains, "CREATE PROCEDURE `p_count`")
			c.Assert(got.Stdout, qt.Contains, "CREATE TRIGGER `t_bi`")
			c.Assert(got.Stdout, qt.Contains, "VIEW `v` AS")
			c.Assert(got.Stdout, qt.Not(qt.Contains), shadowName)
		})
	}
}

// TestSchemaApplyViewNamingItsDatabasePlansNothingE2E applies a view, then
// checks a declaration of the same view whose body names the target's
// database, as `schema inspect` of the whole server writes it. The read of the
// target takes that qualifier off the stored body and the comparison takes it
// off the declaration, so the check plans nothing. A body that reads another
// table of that database is still a change.
func TestSchemaApplyViewNamingItsDatabasePlansNothingE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			targetName, target := newMySQLFamilyScratch(c, engine.admin).database(c, "qualified_view")
			dir := c.TempDir()
			const tables = "CREATE TABLE t (id int PRIMARY KEY);\nCREATE TABLE u (id int PRIMARY KEY);\n"
			declared := filepath.Join(dir, "declared.sql")
			c.Assert(os.WriteFile(declared, []byte(tables+"CREATE VIEW v AS SELECT id FROM t;\n"), 0o600), qt.IsNil)
			qualified := "`" + targetName + "`"
			inspected := filepath.Join(dir, "inspected.sql")
			c.Assert(os.WriteFile(inspected, []byte(tables+"CREATE VIEW v AS select "+qualified+".`t`.`id` AS `id` from "+
				qualified+".`t`;\n"), 0o600), qt.IsNil)
			other := filepath.Join(dir, "other.sql")
			c.Assert(os.WriteFile(other, []byte(tables+"CREATE VIEW v AS select "+qualified+".`u`.`id` AS `id` from "+
				qualified+".`u`;\n"), 0o600), qt.IsNil)
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
			applied := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", target, "--schema-file", declared,
				"--auto-approve")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

			got := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", target, "--schema-file", inspected,
				"--dry-run")
			changed := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", target, "--schema-file", other,
				"--dry-run")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(changed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", changed.Stdout, changed.Stderr))
			c.Assert(changed.Stdout, qt.Contains, "VIEW `v`")
			c.Assert(changed.Stdout, qt.Not(qt.Contains), "Schema is synced")
		})
	}
}

// generatedFiles answers the text of the up and the down file in dir whose
// names hold name.
func generatedFiles(c *qt.C, dir, name string) string {
	c.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*_"+name+".*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.HasLen, 2, qt.Commentf("the files in %s named %s", dir, name))
	var text strings.Builder
	for _, path := range paths {
		body, err := os.ReadFile(path)
		c.Assert(err, qt.IsNil)
		text.Write(body)
	}
	return text.String()
}
