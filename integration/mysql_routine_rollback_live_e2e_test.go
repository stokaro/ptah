//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// heldRoutine is what SHOW CREATE answers for a routine: the sql_mode it was
// created under and the statement that creates it again, which spells its
// definer, characteristics and body.
type heldRoutine struct {
	SQLMode string
	Create  string
}

// heldRoutines is what the server holds for the procedure rt and the function
// rf of the database db is connected to.
func heldRoutines(c *qt.C, db *sql.DB) map[string]heldRoutine {
	c.Helper()
	return map[string]heldRoutine{
		"rt": showCreateRoutine(c, db, "SHOW CREATE PROCEDURE rt"),
		"rf": showCreateRoutine(c, db, "SHOW CREATE FUNCTION rf"),
	}
}

// showCreateRoutine is the answer to statement, a SHOW CREATE PROCEDURE or
// SHOW CREATE FUNCTION.
func showCreateRoutine(c *qt.C, db *sql.DB, statement string) heldRoutine {
	c.Helper()
	var name, characterSet, connectionCollation, databaseCollation string
	var routine heldRoutine
	c.Assert(db.QueryRowContext(c.Context(), statement).Scan(
		&name, &routine.SQLMode, &routine.Create, &characterSet, &connectionCollation, &databaseCollation,
	), qt.IsNil, qt.Commentf("%s", statement))
	return routine
}

// routinePrivilegesIn is what mysql.procs_priv holds on the routines of
// database, one `user@host type name privileges` per row, in order.
func routinePrivilegesIn(c *qt.C, db *sql.DB, database string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `
		SELECT CONCAT(User, '@', Host, ' ', Routine_type, ' ', Routine_name, ' ', Proc_priv)
		FROM mysql.procs_priv
		WHERE Db = ?
		ORDER BY 1`, database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	held := make([]string, 0)
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		held = append(held, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// The down migration of a body change to a MySQL or MariaDB routine creates
// the routine again as the database held it before the up migration: the
// procedure and the function come back with their old bodies and
// characteristics, and every account holds on them what it held before, which
// the DROP takes. One account runs every migration here, under the server's
// own sql_mode, so the routines also keep their definer and their mode;
// [TestMigrationsDownMySQLFamilyCreatesAChangedRoutineUnderTheSessionModeLiveE2E]
// creates one under another mode. The up migration changes the body, the
// security and the volatility of each. What the down migration has
// to restore is read from the database between the first migration and the
// body change, and a plan of that read against the database after the down
// migration is empty, while one against the database after the up migration
// is not.
func TestMigrationsDownMySQLFamilyRestoresAChangedRoutineLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			url, schema, db := languageDatabase(c, ctx, test.engine)
			dev, _, _ := languageDatabase(c, ctx, test.engine)
			user := routineUser(c, ctx, db)
			dir := c.TempDir()
			writeRoutineMigration(c, dir, "1700000000_init.up.sql", "-- +ptah no_transaction\n"+
				"CREATE PROCEDURE rt() NOT DETERMINISTIC NO SQL SQL SECURITY INVOKER SELECT 1;\n"+
				"CREATE FUNCTION rf(a int) RETURNS int DETERMINISTIC SQL SECURITY DEFINER RETURN a + 1;\n"+
				"GRANT EXECUTE ON PROCEDURE rt TO '"+user+"'@'%';\n"+
				"GRANT EXECUTE, ALTER ROUTINE ON FUNCTION rf TO '"+user+"'@'%';\n")
			writeRoutineMigration(c, dir, "1700000000_init.down.sql", "DROP PROCEDURE rt;\nDROP FUNCTION rf;\n")
			desired := filepath.Join(c.TempDir(), "schema.hcl")
			c.Assert(os.WriteFile(desired, []byte(`procedure "rt" {
  security   = DEFINER
  volatility = VOLATILE
  as         = "SELECT 2"
}
function "rf" {
  arg "a" {
    type = int
  }
  return     = int
  security   = INVOKER
  volatility = STABLE
  as         = "RETURN a + 2"
}
`), 0o600), qt.IsNil)
			generated, err := runPtahNativeWithError("migrations", "generate", "--replay", "--trust-replay-sql",
				"--dev-url", dev, "--migrations-dir", dir, "--schema-file", desired, "--name", "body")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))
			initialized, err := runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", dir,
				"--to-version", "1700000000")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", initialized))
			routines, privileges := heldRoutines(c, db), routinePrivilegesIn(c, db, schema)
			// The user is a login, so the read reports its routine grants and
			// inspect notes on stderr that it leaves them out.
			read, err := runPtahNativeStdout("schema", "inspect", "--db-url", url)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", read))
			before := filepath.Join(c.TempDir(), "before.hcl")
			c.Assert(os.WriteFile(before, []byte(read), 0o600), qt.IsNil)

			applied, upErr := runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", dir,
				"--allow-destructive")
			changed := heldRoutines(c, db)
			pending, pendingErr := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", before, "--dry-run")
			rolledBack, downErr := runPtahNativeWithError("migrations", "down", "--db-url", url, "--migrations-dir", dir,
				"--limit", "1", "--confirm")
			settled, settledErr := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", before, "--dry-run")

			c.Assert(upErr, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(changed, qt.Not(qt.DeepEquals), routines)
			c.Assert(pendingErr, qt.IsNil, qt.Commentf("%s", pending))
			c.Assert(pending, qt.Not(qt.Contains), "Schema is synced")
			c.Assert(downErr, qt.IsNil, qt.Commentf("%s", rolledBack))
			c.Assert(routineMigrationNamed(c, dir, "_body.down.sql"), qt.Not(qt.Contains), "empty_rollback")
			c.Assert(heldRoutines(c, db), qt.DeepEquals, routines)
			c.Assert(routinePrivilegesIn(c, db, schema), qt.DeepEquals, privileges)
			c.Assert(privileges, qt.HasLen, 2)
			c.Assert(settledErr, qt.IsNil, qt.Commentf("%s", settled))
			c.Assert(settled, qt.Contains, "Schema is synced")
		})
	}
}

// The down migration of a body change to a MySQL or MariaDB routine creates the
// routine under the sql_mode of the session that runs it, not the mode the
// routine was created under: the down file writes no mode. A function created
// without a strict mode returns 127 for a TINYINT value it cannot hold.
// Created again by the down migration under the server's strict default, the
// same body fails with error 1264. The definition otherwise comes back as the
// database held it.
func TestMigrationsDownMySQLFamilyCreatesAChangedRoutineUnderTheSessionModeLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name    string
		engine  dbtarget.Engine
		refusal string
	}{
		{
			name: "mysql", engine: dbtarget.MySQLAdmin,
			refusal: `Error 1264 \(22003\): Out of range value for column 'rm\(\)' at row 1`,
		},
		{
			name: "mariadb", engine: dbtarget.MariaDBAdmin,
			refusal: `Error 1264 \(22003\): Out of range value for column 'rm\(\)' at row 0`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			url, _, db := languageDatabase(c, ctx, test.engine)
			lenient := withURLParameter(url, "sql_mode", "NO_ENGINE_SUBSTITUTION")
			dir := c.TempDir()
			writeRoutineMigration(c, dir, "1700000000_init.up.sql", "-- +ptah no_transaction\n"+
				"CREATE FUNCTION rm() RETURNS tinyint DETERMINISTIC RETURN 200;\n")
			writeRoutineMigration(c, dir, "1700000000_init.down.sql", "DROP FUNCTION rm;\n")
			desired := filepath.Join(c.TempDir(), "schema.hcl")
			c.Assert(os.WriteFile(desired, []byte(`function "rm" {
  return     = tinyint
  volatility = IMMUTABLE
  as         = "RETURN 100"
}
`), 0o600), qt.IsNil)
			initialized, err := runPtahNativeWithError("migrations", "up", "--db-url", lenient, "--migrations-dir", dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", initialized))
			created := showCreateRoutine(c, db, "SHOW CREATE FUNCTION rm")
			var returned int
			c.Assert(db.QueryRowContext(ctx, "SELECT rm()").Scan(&returned), qt.IsNil)
			var serverMode string
			c.Assert(db.QueryRowContext(ctx, "SELECT @@GLOBAL.sql_mode").Scan(&serverMode), qt.IsNil)
			generated, err := runPtahNativeWithError("migrations", "generate", "--db-url", url,
				"--migrations-dir", dir, "--schema-file", desired, "--name", "body")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))

			applied, upErr := runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", dir,
				"--allow-destructive")
			rolledBack, downErr := runPtahNativeWithError("migrations", "down", "--db-url", url, "--migrations-dir", dir,
				"--limit", "1", "--confirm")
			var restored int
			callErr := db.QueryRowContext(ctx, "SELECT rm()").Scan(&restored)

			c.Assert(created.SQLMode, qt.Equals, "NO_ENGINE_SUBSTITUTION")
			c.Assert(returned, qt.Equals, 127)
			c.Assert(upErr, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(downErr, qt.IsNil, qt.Commentf("%s", rolledBack))
			c.Assert(routineMigrationNamed(c, dir, "_body.down.sql"), qt.Not(qt.Contains), "empty_rollback")
			c.Assert(showCreateRoutine(c, db, "SHOW CREATE FUNCTION rm"), qt.Equals,
				heldRoutine{SQLMode: serverMode, Create: created.Create})
			c.Assert(callErr, qt.ErrorMatches, test.refusal)
		})
	}
}
