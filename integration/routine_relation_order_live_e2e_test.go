//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A schema-bound function resolves its source table at CREATE time, while a
// DEFAULT resolves the function when its table is created. The server accepts
// source_rows -> read_total -> target_rows, and rejects either flat ordering.
func TestSchemaApplyOrdersSQLServerRoutineAndTableDependenciesLiveE2E(t *testing.T) {
	c := qt.New(t)
	database := sqlServerDevDatabase(c)
	source := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(source, []byte(`CREATE TABLE dbo.target_rows (n int DEFAULT dbo.read_total());
CREATE FUNCTION dbo.read_total() RETURNS int WITH SCHEMABINDING
AS BEGIN RETURN (SELECT COUNT(*) FROM dbo.source_rows) END;
CREATE TABLE dbo.source_rows (id int NOT NULL);
`), 0o600), qt.IsNil)

	applied, err := runPtahNativeWithError("schema", "apply", "--db-url", database.url, "--schema-file", source, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	database.exec(c, []string{"INSERT INTO dbo.source_rows VALUES (1)", "INSERT INTO dbo.target_rows DEFAULT VALUES"})
	var total int
	c.Assert(database.conn.QueryRowContext(c.Context(), "SELECT n FROM dbo.target_rows").Scan(&total), qt.IsNil)
	c.Assert(total, qt.Equals, 1)
}

func TestMigrationsDownRestoresSQLServerRoutineAfterItsTableLiveE2E(t *testing.T) {
	c := qt.New(t)
	assertRoutineRelationRollback(c, sqlServerDevDatabase(c), routineRelationRollback{
		initial: "CREATE TABLE dbo.t (id int NOT NULL);\n" +
			"CREATE FUNCTION dbo.rt() RETURNS int WITH SCHEMABINDING AS BEGIN RETURN (SELECT COUNT(*) FROM dbo.t) END;\n",
		desired: "CREATE FUNCTION dbo.rt() RETURNS int WITH SCHEMABINDING AS BEGIN RETURN 0 END;\n",
		drop:    "DROP FUNCTION dbo.rt;\nDROP TABLE dbo.t;\n",
		status:  "SELECT CASE WHEN OBJECTPROPERTY(OBJECT_ID('dbo.rt'), 'IsSchemaBound') = 1 THEN 'VALID' ELSE 'INVALID' END",
		body:    "SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.rt'))",
		reads:   "FROM DBO.T",
		call:    "SELECT dbo.rt()",
	})
}

// Oracle can accept CREATE PROCEDURE with unresolved table references and
// report a successful rollback. USER_OBJECTS and an actual call must both
// confirm that the restored body is valid.
func TestMigrationsDownRestoresValidOracleRoutineAfterItsTableLiveE2E(t *testing.T) {
	c := qt.New(t)
	assertRoutineRelationRollback(c, oracleRoutineDatabase(c), routineRelationRollback{
		initial: "CREATE TABLE t (id NUMBER NOT NULL);\n" +
			"CREATE OR REPLACE PROCEDURE rt AUTHID DEFINER IS\n n NUMBER;\nBEGIN SELECT COUNT(*) INTO n FROM t; END;\n",
		desired: "CREATE OR REPLACE PROCEDURE rt AUTHID DEFINER IS\nBEGIN NULL; END;\n",
		drop:    "DROP PROCEDURE rt;\nDROP TABLE t;\n",
		status:  "SELECT status FROM user_objects WHERE object_name = 'RT' AND object_type = 'PROCEDURE'",
		body:    "SELECT LISTAGG(text) WITHIN GROUP (ORDER BY line) FROM user_source WHERE name = 'RT'",
		reads:   "FROM T",
		call:    "BEGIN rt; END;",
	})
}

// MySQL resolves a routine's body at its call, so it is a control for the
// shared planner: the generated up and down files still leave a callable body
// with the table it reads, even though CREATE would accept the opposite order.
func TestMigrationsDownRestoresMySQLRoutineAfterItsTableLiveE2E(t *testing.T) {
	c := qt.New(t)
	address, _, _ := languageDatabase(c, c.Context(), dbtarget.MySQLAdmin)
	assertRoutineRelationRollback(c, devDialectDatabase{url: address, conn: connectDevDialect(c, address)}, routineRelationRollback{
		initial: "-- +ptah no_transaction\nCREATE TABLE t (id int NOT NULL);\n" +
			"CREATE PROCEDURE rt() READS SQL DATA BEGIN SELECT COUNT(*) AS n FROM t; END;\n",
		desired: "CREATE PROCEDURE rt() READS SQL DATA BEGIN SELECT 0 AS n; END;\n",
		drop:    "-- +ptah no_transaction\nDROP PROCEDURE rt;\nDROP TABLE t;\n",
		status:  "SELECT 'VALID' FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_name = 'rt'",
		body:    "SELECT routine_definition FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_name = 'rt'",
		reads:   "FROM T",
		call:    "CALL rt()",
	})
}

type routineRelationRollback struct {
	initial, desired, drop string
	status, body, reads    string
	call                   string
}

func assertRoutineRelationRollback(c *qt.C, database devDialectDatabase, fixture routineRelationRollback) {
	c.Helper()
	dir := c.TempDir()
	writeRoutineMigration(c, dir, "1700000000_init.up.sql", fixture.initial)
	writeRoutineMigration(c, dir, "1700000000_init.down.sql", fixture.drop)
	source := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(source, []byte(fixture.desired), 0o600), qt.IsNil)
	initialized, err := runPtahNativeWithError("migrations", "up", "--db-url", database.url, "--migrations-dir", dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", initialized))
	c.Assert(routineRelationScalar(c, database, fixture.status), qt.Equals, "VALID")

	generated, err := runPtahNativeWithError("migrations", "generate", "--db-url", database.url,
		"--migrations-dir", dir, "--schema-file", source, "--name", "drop_table")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))
	applied, err := runPtahNativeWithError("migrations", "up", "--db-url", database.url, "--migrations-dir", dir,
		"--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(strings.ToUpper(routineRelationScalar(c, database, fixture.body)), qt.Not(qt.Contains), fixture.reads)
	rolledBack, err := runPtahNativeWithError("migrations", "down", "--db-url", database.url,
		"--migrations-dir", dir, "--limit", "1", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", rolledBack))
	c.Assert(routineRelationScalar(c, database, fixture.status), qt.Equals, "VALID")
	c.Assert(strings.ToUpper(routineRelationScalar(c, database, fixture.body)), qt.Contains, fixture.reads)
	database.exec(c, []string{fixture.call})
}

func routineRelationScalar(c *qt.C, database devDialectDatabase, query string) string {
	c.Helper()
	var value string
	c.Assert(database.conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
	return value
}
