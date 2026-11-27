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

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// initializeRemovedRoutine applies the first migration before its privileges
// are granted outside the migration history.
func initializeRemovedRoutine(c *qt.C, target, sqlText string) string {
	c.Helper()
	dir := c.TempDir()
	writeRoutineMigration(c, dir, "1700000000_init.up.sql", sqlText)
	writeRoutineMigration(c, dir, "1700000000_init.down.sql", "DROP PROCEDURE rt;\nDROP TABLE keep_me;\n")
	initialized, err := runPtahNativeWithError("migrations", "up", "--db-url", target, "--migrations-dir", dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", initialized))
	return dir
}

// removeObservedRoutine generates from the live database, so its down file
// must capture privileges that the first migration never declared.
func removeObservedRoutine(c *qt.C, target, dir, schema string) {
	c.Helper()
	desired := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(desired, []byte(schema), 0o600), qt.IsNil)
	generated, err := runPtahNativeWithError("migrations", "generate", "--db-url", target,
		"--migrations-dir", dir, "--schema-file", desired, "--name", "remove_routine")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))
	applied, err := runPtahNativeWithError("migrations", "up", "--db-url", target,
		"--migrations-dir", dir, "--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
}

// restoreObservedRoutine applies only the generated rollback.
func restoreObservedRoutine(c *qt.C, target, dir string) {
	c.Helper()
	rolledBack, err := runPtahNativeWithError("migrations", "down", "--db-url", target,
		"--migrations-dir", dir, "--limit", "1", "--confirm")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", rolledBack))
}

// Routine removals delete mysql.procs_priv rows for users and roles. Their
// rollback restores both, including a user's grant option.
func TestMigrationsDownMySQLFamilyRestoresRemovedRoutinePrivilegesE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, database, db := languageDatabase(c, c.Context(), test.engine)
			user := routineUser(c, c.Context(), db)
			role := fmt.Sprintf("rt_role_%d", time.Now().UnixNano()%1_000_000_000)
			_, err := db.ExecContext(c.Context(), "CREATE ROLE `"+role+"`")
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				_, err := db.ExecContext(context.Background(), "DROP ROLE `"+role+"`")
				c.Check(err, qt.IsNil)
			})
			dir := initializeRemovedRoutine(c, target,
				"-- +ptah no_transaction\nCREATE TABLE keep_me (id INT PRIMARY KEY);\n"+
					"CREATE PROCEDURE rt() SELECT 1;\n")
			for _, statement := range []string{
				"GRANT EXECUTE ON PROCEDURE rt TO `" + role + "`",
				"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE rt TO '" + user + "'@'%' WITH GRANT OPTION",
			} {
				_, err := db.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			before := routinePrivilegesIn(c, db, database)
			c.Assert(before, qt.HasLen, 2)

			removeObservedRoutine(c, target, dir, "CREATE TABLE keep_me (id INT PRIMARY KEY);\nCREATE ROLE `"+role+"`;\n")
			c.Assert(routinePrivilegesIn(c, db, database), qt.HasLen, 0)
			restoreObservedRoutine(c, target, dir)

			c.Assert(routinePrivilegesIn(c, db, database), qt.DeepEquals, before)
		})
	}
}

// A DROP under automatic_sp_privileges=0 leaves a stale row. Enabling the
// automatic cleanup later does not turn that row into a privilege of a new
// routine, and the reader must not carry it into a restoration.
func TestMySQLReaderLeavesOutDroppedRoutinePrivilegesLive(t *testing.T) {
	c := qt.New(t)
	target, database, db := languageDatabase(c, c.Context(), dbtarget.MySQLAdmin)
	user := routineUser(c, c.Context(), db)
	for _, statement := range []string{
		"CREATE PROCEDURE rt() SELECT 1",
		"GRANT EXECUTE ON PROCEDURE rt TO '" + user + "'@'%'",
	} {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	withAutomaticSPPrivileges(c, c.Context(), db, "0")
	_, err := db.ExecContext(c.Context(), "DROP PROCEDURE rt")
	c.Assert(err, qt.IsNil)
	c.Assert(routinePrivilegesIn(c, db, database), qt.HasLen, 1)
	withAutomaticSPPrivileges(c, c.Context(), db, "1")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	read, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(read.RoutinePrivileges.Grants, qt.HasLen, 0)
}

// SQL Server retains the grantees outside the schema, but dropping their
// procedure deletes its permission rows. The down file restores the rows of
// a managed role, an unmanaged user, and a denied user.
func TestMigrationsDownSQLServerRestoresRemovedRoutinePrivilegesE2E(t *testing.T) {
	c := qt.New(t)
	target, db := declaredSQLServerDatabase(c)
	dir := initializeRemovedRoutine(c, target,
		"CREATE TABLE keep_me (id INT PRIMARY KEY);\nGO\n"+
			"CREATE PROCEDURE dbo.rt AS BEGIN SELECT 1 END;\nGO\n")
	declaredSQLServerExec(c, db,
		"CREATE ROLE rt_reader",
		"CREATE USER rt_user WITHOUT LOGIN",
		"CREATE USER rt_denied WITHOUT LOGIN",
		"GRANT EXECUTE ON dbo.rt TO rt_reader",
		"GRANT EXECUTE ON dbo.rt TO rt_user WITH GRANT OPTION",
		"DENY EXECUTE ON dbo.rt TO rt_denied",
	)
	before := removedSQLServerRoutinePermissions(c, db)
	c.Assert(before, qt.HasLen, 3)

	removeObservedRoutine(c, target, dir, "CREATE TABLE dbo.keep_me (id INT PRIMARY KEY);\nCREATE ROLE rt_reader;\n")
	c.Assert(removedSQLServerRoutinePermissions(c, db), qt.HasLen, 0)
	restoreObservedRoutine(c, target, dir)

	c.Assert(removedSQLServerRoutinePermissions(c, db), qt.DeepEquals, before)
}

// removedSQLServerRoutinePermissions reads permission rows, including DENY
// and WITH GRANT OPTION, for the procedure the migration removes.
func removedSQLServerRoutinePermissions(c *qt.C, db *sql.DB) []string {
	c.Helper()
	// Catalog names and permission descriptions can use different collations.
	// Read them separately so the server need not concatenate unlike strings.
	rows, err := db.QueryContext(c.Context(), `
		SELECT grantee.name, pe.permission_name, pe.state_desc
		FROM sys.database_permissions AS pe
		JOIN sys.database_principals AS grantee ON grantee.principal_id = pe.grantee_principal_id
		WHERE pe.class = 1 AND pe.major_id = OBJECT_ID(N'dbo.rt')`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	held := make([]string, 0)
	for rows.Next() {
		var grantee, permission, state string
		c.Assert(rows.Scan(&grantee, &permission, &state), qt.IsNil)
		held = append(held, strings.Join([]string{grantee, permission, state}, " "))
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(held)
	return held
}

// Oracle removes a routine's object grants on DROP. A grantable catalog row
// belongs to a user, and the rollback must restore its option as well as the
// plain grant to the role the schema declares.
func TestMigrationsDownOracleRestoresRemovedRoutinePrivilegesE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	dir := initializeRemovedRoutine(c, account.url,
		"CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n"+
			"CREATE OR REPLACE PROCEDURE rt AUTHID DEFINER IS\nBEGIN NULL; END;\n")
	for _, statement := range []string{
		"GRANT EXECUTE ON rt TO " + account.role,
		"GRANT EXECUTE ON rt TO " + account.user + " WITH GRANT OPTION",
	} {
		_, err := account.owner.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	before := removedOracleRoutinePrivileges(c, account.owner)
	c.Assert(before, qt.HasLen, 2)

	removeObservedRoutine(c, account.url, dir,
		"CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\nCREATE ROLE "+account.role+";\n")
	c.Assert(removedOracleRoutinePrivileges(c, account.owner), qt.HasLen, 0)
	restoreObservedRoutine(c, account.url, dir)

	c.Assert(removedOracleRoutinePrivileges(c, account.owner), qt.DeepEquals, before)
}

// removedOracleRoutinePrivileges reads the grantable flag separately from
// the privilege, so restoring only EXECUTE cannot pass.
func removedOracleRoutinePrivileges(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `
		SELECT grantee || ' ' || privilege || ' ' || grantable
		FROM user_tab_privs
		WHERE table_name = 'RT' AND owner = USER
		ORDER BY 1`)
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
