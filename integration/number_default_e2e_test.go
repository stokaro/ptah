//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestApplySQLServerNumberDefaultE2E applies number defaults declared in SQL
// through `ptah schema apply`, then changes one. A number beside a number type
// is written bare, so SQL Server stores DEFAULT -1 as ((-1)), and quoted it
// would store the string ('-1'). The NVARCHAR default keeps its quotes. ADD
// DEFAULT ... FOR, which sets the changed default, writes the number bare too,
// and each second apply plans nothing.
func TestApplySQLServerNumberDefaultE2E(t *testing.T) {
	c := qt.New(t)
	database := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
		"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
	stored := "SELECT c.name + ' ' + dc.definition FROM sys.default_constraints AS dc JOIN sys.columns AS c " +
		"ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id WHERE dc.parent_object_id = OBJECT_ID(N'dbo.tallies')"
	declared := func(below string) string {
		return "CREATE TABLE tallies (\n" +
			"  id INT NOT NULL PRIMARY KEY,\n" +
			"  below INT DEFAULT " + below + ",\n" +
			"  above BIGINT DEFAULT 5,\n" +
			"  price DECIMAL(5,2) DEFAULT -1.50,\n" +
			"  code NVARCHAR(10) DEFAULT '007'\n" +
			");\n"
	}
	steps := []struct {
		below string
		want  []string
	}{
		{below: "-1", want: []string{"above ((5))", "below ((-1))", "code ('007')", "price ((-1.50))"}},
		{below: "-2", want: []string{"above ((5))", "below ((-2))", "code ('007')", "price ((-1.50))"}},
	}
	for _, step := range steps {
		schema := filepath.Join(c.TempDir(), "schema.sql")
		c.Assert(os.WriteFile(schema, []byte(declared(step.below)), 0o600), qt.IsNil)
		apply := []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve"}

		applied := runPtahIn(c, apply...)
		again := runPtahIn(c, apply...)

		c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
		c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
		c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		c.Assert(catalogReadBack(c, database, []string{stored}), qt.DeepEquals, [][]string{step.want})
	}
}

// TestApplyOracleNumberDefaultE2E is TestApplySQLServerNumberDefaultE2E on
// Oracle, where a quoted number is stored as written, as the string '-1', and a
// bare one as the number. The changed default goes through MODIFY.
func TestApplyOracleNumberDefaultE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	// DATA_DEFAULT keeps the line break after the last column's default.
	stored := "SELECT column_name || ' ' || RTRIM(data_default_vc, CHR(10)) FROM user_tab_cols " +
		"WHERE table_name = 'TALLIES' AND data_default_vc IS NOT NULL ORDER BY column_name"
	declared := func(below string) string {
		return "CREATE TABLE tallies (\n" +
			"  id NUMBER(10) PRIMARY KEY,\n" +
			"  below NUMBER(10) DEFAULT " + below + ",\n" +
			"  above NUMBER(19) DEFAULT 5,\n" +
			"  price NUMBER(5,2) DEFAULT -1.50,\n" +
			"  code VARCHAR2(10) DEFAULT '007'\n" +
			");\n"
	}
	steps := []struct {
		below string
		want  []string
	}{
		{below: "-1", want: []string{"ABOVE 5", "BELOW -1", "CODE '007'", "PRICE -1.50"}},
		{below: "-2", want: []string{"ABOVE 5", "BELOW -2", "CODE '007'", "PRICE -1.50"}},
	}
	for _, step := range steps {
		schema := writeDeclaredSchema(c, declared(step.below))
		apply := []string{"schema", "apply", "--db-url", dbURL, "--schema-file", schema, "--auto-approve"}

		applied := runPtahIn(c, apply...)
		again := runPtahIn(c, apply...)

		c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
		c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
		c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		c.Assert(accountNames(c, ctx, conn, stored), qt.DeepEquals, step.want)
	}
}
