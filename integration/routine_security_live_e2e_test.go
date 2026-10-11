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

// TestSchemaRenderWritesNoSecurityClauseForAnUnstatedModeFS renders a
// procedure that states no security mode. No clause is written, so each server
// applies its own default, which is what the comparison reads an unstated mode
// as.
func TestSchemaRenderWritesNoSecurityClauseForAnUnstatedModeFS(t *testing.T) {
	for _, test := range []struct {
		dialect string
		clause  string
	}{
		{dialect: "mysql", clause: "SQL SECURITY"},
		{dialect: "mariadb", clause: "SQL SECURITY"},
		{dialect: "postgres", clause: "SECURITY"},
		{dialect: "sqlserver", clause: "EXECUTE AS"},
		{dialect: "oracle", clause: "AUTHID"},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := writeLanguageFreeProcedure(c, "app")

			out, err := runPtahNativeWithError("schema", "render", "--dialect", test.dialect, "--schema-file", schema)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "language_probe")
			c.Assert(out, qt.Not(qt.Contains), test.clause)
		})
	}
}

// TestSchemaApplyMakesAnInvokerProcedureThatStatesNoModeADefinerOneLiveE2E
// applies, on MySQL and MariaDB, a declaration that states no security mode to
// a procedure the server runs with its caller's rights (SQL SECURITY INVOKER).
// The declaration means the server's default, DEFINER, so the plan replaces
// the procedure, the catalog then reports DEFINER, and the second plan is
// empty.
func TestSchemaApplyMakesAnInvokerProcedureThatStatesNoModeADefinerOneLiveE2E(t *testing.T) {
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
			url, schema, db := languageDatabase(c, ctx, test.engine)
			_, err := db.ExecContext(ctx, "CREATE PROCEDURE language_probe() SQL SECURITY INVOKER BEGIN SELECT 1; END")
			c.Assert(err, qt.IsNil)
			document := writeLanguageFreeProcedure(c, schema)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", document, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var security string
			c.Assert(db.QueryRowContext(ctx, "SELECT SECURITY_TYPE FROM information_schema.ROUTINES "+
				"WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'language_probe'").Scan(&security), qt.IsNil)
			c.Assert(security, qt.Equals, "DEFINER")
			second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", document, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
			c.Assert(second, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyReplacesADefinerFunctionThatStatesNoModeLiveE2E pins that a
// PostgreSQL function written with no SECURITY clause loses the definer's
// rights it had. CREATE OR REPLACE gives every attribute it does not state the
// server's default, so a function created SECURITY DEFINER by hand and
// declared with no mode is replaced as INVOKER, and the second plan is empty.
func TestSchemaApplyReplacesADefinerFunctionThatStatesNoModeLiveE2E(t *testing.T) {
	c := qt.New(t)
	schema := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(schema, []byte(`schema "public" {}
function "probe" {
  schema = schema.public
  return = integer
  as     = "BEGIN RETURN 1; END;"
}
`), 0o600), qt.IsNil)
	target := databaseBuiltFrom(c,
		`CREATE FUNCTION probe() RETURNS integer LANGUAGE plpgsql SECURITY DEFINER AS $$BEGIN RETURN 1; END;$$;`)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var definer bool
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT prosecdef FROM pg_proc WHERE proname = 'probe'").Scan(&definer), qt.IsNil)
	c.Assert(definer, qt.IsFalse)
	second, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "Schema is synced")
}
