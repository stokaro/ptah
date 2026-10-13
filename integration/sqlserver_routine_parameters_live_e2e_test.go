//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// Optional AS reaches the catalog as the same parameters. Principal changes
// still address the existing routine and preserve its other header options.
func TestSchemaApplySQLServerOptionalParameterASLiveE2E(t *testing.T) {
	for _, test := range []struct{ name, header, options, body string }{
		{"procedure", "CREATE PROCEDURE dbo.param_routine %s", "RECOMPILE", "SELECT @a;"},
		{"function", "CREATE FUNCTION dbo.param_routine(%s) RETURNS INT", "SCHEMABINDING", "RETURN @a;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, db := principalDatabase(c, ctx)
			for _, principal := range []string{"auditor", "reviewer"} {
				declaration := fmt.Sprintf(test.header, "@a AS INT, @amount AS DECIMAL(10, 2), @label AS NVARCHAR(50)") + " WITH " + test.options + ", EXECUTE AS '" + principal + "' AS BEGIN " + test.body + " END;\nGO\n"
				file := writeSQLSchema(c, declaration)
				out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", file, "--dry-run")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
				c.Assert(second, qt.Contains, "Schema is synced")
				var user string
				c.Assert(db.QueryRowContext(ctx, "SELECT USER_NAME(execute_as_principal_id) FROM sys.sql_modules WHERE object_id=OBJECT_ID('dbo.param_routine')").Scan(&user), qt.IsNil)
				c.Assert(user, qt.Equals, principal)
				var dimensions string
				c.Assert(db.QueryRowContext(ctx, "SELECT CONCAT(precision, ',', scale) FROM sys.parameters WHERE object_id=OBJECT_ID('dbo.param_routine') AND name='@amount'").Scan(&dimensions), qt.IsNil)
				c.Assert(dimensions, qt.Equals, "10,2")
				var length int
				c.Assert(db.QueryRowContext(ctx, "SELECT max_length FROM sys.parameters WHERE object_id=OBJECT_ID('dbo.param_routine') AND name='@label'").Scan(&length), qt.IsNil)
				c.Assert(length, qt.Equals, 100)
			}
		})
	}
}

// Every declaration source reaches the selected owner's normalization before
// comparison. Unmanaged header options do not trigger a replacement.
func TestSchemaApplySQLServerParameterSourcesLiveE2E(t *testing.T) {
	for _, test := range []struct{ name, filename, path, flag, content string }{
		{"HCL", "schema.hcl", "schema.hcl", "--schema-file", `schema "dbo" {}
procedure "p" {
 schema = schema.dbo
 params = "@a AS INT"
 security = DEFINER
 as = "BEGIN SELECT @a; END"
}
`},
		{"Go", "schema.go", ".", "--root-dir", `package entities
//ptah:schema:procedure name="dbo.p" params="@a AS INT" language="sql" security="DEFINER" body="BEGIN SELECT @a; END"
type P struct{}
`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, db := principalDatabase(c, ctx)
			sqlFile := writeSQLSchema(c, "CREATE PROCEDURE dbo.p @a INT WITH RECOMPILE, EXECUTE AS 'auditor' AS BEGIN SELECT @a; END;\nGO\n")
			out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", sqlFile, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var before, after int
			c.Assert(db.QueryRowContext(ctx, "SELECT OBJECT_ID('dbo.p')").Scan(&before), qt.IsNil)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, test.filename), []byte(test.content), 0o600), qt.IsNil)
			out, err = runPtahNativeWithError("schema", "apply", "--db-url", url, test.flag, filepath.Join(dir, test.path), "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, test.flag, filepath.Join(dir, test.path), "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
			c.Assert(second, qt.Contains, "Schema is synced")
			c.Assert(db.QueryRowContext(ctx, "SELECT OBJECT_ID('dbo.p')").Scan(&after), qt.IsNil)
			c.Assert(after, qt.Equals, before)
		})
	}
}
