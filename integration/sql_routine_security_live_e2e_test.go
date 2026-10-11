//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// mysqlStatedRoutines are a function and a procedure written in SQL for the
// MySQL family, each stating its security mode, and the function its
// determinism.
const mysqlStatedRoutines = `CREATE FUNCTION probe_f() RETURNS INT DETERMINISTIC SQL SECURITY DEFINER RETURN 1;
CREATE PROCEDURE probe_p() SQL SECURITY INVOKER BEGIN SELECT 1; END;
`

// postgresStatedProcedure is a PostgreSQL procedure written in SQL that runs
// with its definer's rights and pins its search_path.
const postgresStatedProcedure = `CREATE PROCEDURE probe_p() LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$ BEGIN NULL; END; $$;
`

// writeSQLSchema writes sql to a schema file.
func writeSQLSchema(c *qt.C, sql string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
	return path
}

// TestSchemaRenderKeepsARoutinesStatedSecurityFS renders routines written in
// SQL. The reader dropped what their statements state: a MySQL routine's
// characteristics and a PostgreSQL procedure's SECURITY and SET clauses, so a
// definer routine was rendered to run with its caller's rights.
func TestSchemaRenderKeepsARoutinesStatedSecurityFS(t *testing.T) {
	for _, test := range []struct {
		dialect string
		sql     string
		want    []string
	}{
		{dialect: "mysql", sql: mysqlStatedRoutines,
			want: []string{"RETURNS int DETERMINISTIC SQL SECURITY DEFINER", "`probe_p`() READS SQL DATA SQL SECURITY INVOKER"}},
		{dialect: "mariadb", sql: mysqlStatedRoutines,
			want: []string{"RETURNS int DETERMINISTIC SQL SECURITY DEFINER", "`probe_p`() READS SQL DATA SQL SECURITY INVOKER"}},
		{dialect: "postgres", sql: postgresStatedProcedure,
			want: []string{"LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog,pg_temp"}},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := writeSQLSchema(c, test.sql)

			out, err := runPtahNativeWithError("schema", "render", "--dialect", test.dialect, "--schema-file", schema)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			for _, want := range test.want {
				c.Assert(out, qt.Contains, want)
			}
		})
	}
}

// TestSchemaApplyKeepsAMySQLRoutinesStatedSecurityLiveE2E applies the MySQL
// routines, reads back what the catalog holds, and plans again: the second
// plan is empty.
func TestSchemaApplyKeepsAMySQLRoutinesStatedSecurityLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, _, db := languageDatabase(c, ctx, test.engine)
			schema := writeSQLSchema(c, mysqlStatedRoutines)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var functionSecurity, deterministic, procedureSecurity string
			c.Assert(db.QueryRowContext(ctx, `SELECT SECURITY_TYPE, IS_DETERMINISTIC FROM information_schema.ROUTINES
WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'probe_f'`).Scan(&functionSecurity, &deterministic), qt.IsNil)
			c.Assert(db.QueryRowContext(ctx, `SELECT SECURITY_TYPE FROM information_schema.ROUTINES
WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'probe_p'`).Scan(&procedureSecurity), qt.IsNil)
			c.Assert([]string{functionSecurity, deterministic, procedureSecurity}, qt.DeepEquals, []string{"DEFINER", "YES", "INVOKER"})
			second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
			c.Assert(second, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyKeepsAPostgresProceduresStatedSecurityLiveE2E applies the
// PostgreSQL procedure, reads back what the catalog holds, and plans again:
// the second plan is empty.
func TestSchemaApplyKeepsAPostgresProceduresStatedSecurityLiveE2E(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	schema := writeSQLSchema(c, postgresStatedProcedure)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var definer bool
	var settings string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT prosecdef, array_to_string(proconfig, ';') FROM pg_proc WHERE proname = 'probe_p'",
	).Scan(&definer, &settings), qt.IsNil)
	c.Assert(definer, qt.IsTrue)
	c.Assert(settings, qt.Equals, "search_path=pg_catalog, pg_temp")
	second, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "Schema is synced")
}
