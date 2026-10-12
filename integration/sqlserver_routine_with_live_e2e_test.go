//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// sqlServerOwnerRoutines are a procedure and a function written in SQL that
// run as their owner, as stokaro/ptah#4490 reported them.
const sqlServerOwnerRoutines = `CREATE PROCEDURE dbo.po WITH EXECUTE AS OWNER AS BEGIN SELECT 1; END;
GO
CREATE FUNCTION dbo.fo() RETURNS INT WITH EXECUTE AS OWNER AS BEGIN RETURN 1; END;
GO
`

// TestSchemaRenderKeepsASQLServerRoutinesNameAndBodyFS renders the routines.
// The AS of EXECUTE AS comes first. Taken for the body's, it would
// create the procedure as `[dbo].[po WITH EXECUTE]` with the body `OWNER AS
// BEGIN SELECT 1; END`, and give the function the result type `int with
// execute`.
func TestSchemaRenderKeepsASQLServerRoutinesNameAndBodyFS(t *testing.T) {
	c := qt.New(t)
	schema := writeSQLSchema(c, sqlServerOwnerRoutines)

	out, err := runPtahNativeWithError("schema", "render", "--dialect", "sqlserver", "--schema-file", schema)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	for _, want := range []string{
		"CREATE OR ALTER PROCEDURE [dbo].[po] \nWITH EXECUTE AS OWNER\nAS\nBEGIN SELECT 1; END;",
		"CREATE OR ALTER FUNCTION [dbo].[fo]()\nRETURNS int\nWITH EXECUTE AS OWNER\nAS\nBEGIN RETURN 1; END;",
	} {
		c.Assert(out, qt.Contains, want)
	}
}

// TestSchemaRenderRefusesASQLServerOptionItCannotKeepFS names the option the
// model has no place for, rather than rendering the routine without it.
func TestSchemaRenderRefusesASQLServerOptionItCannotKeepFS(t *testing.T) {
	c := qt.New(t)
	schema := writeSQLSchema(c, "CREATE PROCEDURE dbo.pe WITH ENCRYPTION AS BEGIN SELECT 1; END;\nGO\n")

	out, err := runPtahNativeWithError("schema", "render", "--dialect", "sqlserver", "--schema-file", schema)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "the sqlserver procedure dbo.pe states ENCRYPTION, which Ptah can neither write nor compare back")
}

// TestSchemaApplyCreatesASQLServerRoutineThatRunsAsItsOwnerLiveE2E applies the
// routines, reads them back from the catalog, and plans again: the second plan
// is empty.
func TestSchemaApplyCreatesASQLServerRoutineThatRunsAsItsOwnerLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	url, _, db := languageDatabase(c, ctx, dbtarget.SQLServer)
	schema := writeSQLSchema(c, sqlServerOwnerRoutines)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	rows, err := db.QueryContext(ctx, "SELECT o.name, m.execute_as_principal_id FROM sys.sql_modules m "+
		"JOIN sys.objects o ON o.object_id = m.object_id WHERE o.name IN ('po', 'fo') ORDER BY o.name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name string
		var principal int
		c.Assert(rows.Scan(&name, &principal), qt.IsNil)
		got = append(got, name+" "+map[int]string{-2: "OWNER"}[principal])
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"fo OWNER", "po OWNER"})
	second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "Schema is synced")
}

// principalProcedure is a procedure written in SQL that runs as principal,
// spelled as an EXECUTE AS clause writes it: a quoted user name, or OWNER.
func principalProcedure(principal, body string) string {
	return fmt.Sprintf("CREATE PROCEDURE dbo.audit_it WITH EXECUTE AS %s AS BEGIN %s END;\nGO\n", principal, body)
}

// principalDatabase creates a throwaway database holding two users without a
// login, auditor and reviewer, for a routine to run as.
func principalDatabase(c *qt.C, ctx context.Context) (string, *sql.DB) {
	c.Helper()
	url, db := newSQLServerMigrationDatabase(c, ctx, dbtarget.URL(c, dbtarget.SQLServer))
	for _, user := range []string{"auditor", "reviewer"} {
		_, err := db.ExecContext(ctx, "CREATE USER "+user+" WITHOUT LOGIN")
		c.Assert(err, qt.IsNil)
	}
	return url, db
}

// runsAs reads the principal dbo.audit_it runs as: CALLER, OWNER or the name
// of the user.
func runsAs(c *qt.C, ctx context.Context, db *sql.DB) string {
	c.Helper()
	var principal string
	c.Assert(db.QueryRowContext(ctx, "SELECT CASE WHEN m.execute_as_principal_id IS NULL THEN 'CALLER' "+
		"WHEN m.execute_as_principal_id = -2 THEN 'OWNER' ELSE USER_NAME(m.execute_as_principal_id) END "+
		"FROM sys.sql_modules m WHERE m.object_id = OBJECT_ID('dbo.audit_it')").Scan(&principal), qt.IsNil)
	return principal
}

// TestSchemaApplyKeepsASQLServerRoutinesPrincipalLiveE2E applies a procedure
// that runs as a named user. The catalog reports that user, and the second
// plan is empty: the principal reads back as the facet the SQL declared.
func TestSchemaApplyKeepsASQLServerRoutinesPrincipalLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	url, db := principalDatabase(c, ctx)
	schema := writeSQLSchema(c, principalProcedure("'auditor'", "SELECT 1;"))

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(runsAs(c, ctx, db), qt.Equals, "auditor")
	second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "Schema is synced")
}

// TestSchemaApplyChangesASQLServerRoutinesPrincipalLiveE2E declares another
// principal for a procedure the server runs as one. The security mode calls a
// routine that runs as a named user and one that runs as its owner both
// DEFINER, so the principal is compared on its own. The change is planned, the
// catalog reports the declared principal, and the second plan is empty.
func TestSchemaApplyChangesASQLServerRoutinesPrincipalLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name     string
		from, to string
		want     string
	}{
		{name: "a user to the owner", from: "'auditor'", to: "OWNER", want: "OWNER"},
		{name: "the owner to a user", from: "OWNER", to: "'auditor'", want: "auditor"},
		{name: "a user to another", from: "'auditor'", to: "N'reviewer'", want: "reviewer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, db := principalDatabase(c, ctx)
			first := writeSQLSchema(c, principalProcedure(test.from, "SELECT 1;"))
			out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", first, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			declared := writeSQLSchema(c, principalProcedure(test.to, "SELECT 1;"))

			out, err = runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", declared, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(runsAs(c, ctx, db), qt.Equals, test.want)
			second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", declared, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
			c.Assert(second, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyRefusesToReplaceARoutineItsSourceCannotGiveAPrincipalLiveE2E
// changes the body of a procedure that runs as a named user, from an HCL
// document, which has no way to write EXECUTE AS. The document states
// DEFINER, as the procedure runs, so the body is the one difference.
// Replacing the procedure would create it to run as its owner, so the plan is
// refused, and the procedure keeps its principal.
func TestSchemaApplyRefusesToReplaceARoutineItsSourceCannotGiveAPrincipalLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	url, db := principalDatabase(c, ctx)
	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file",
		writeSQLSchema(c, principalProcedure("'auditor'", "SELECT 1;")), "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	document := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(document, []byte(`schema "dbo" {}
procedure "audit_it" {
  schema   = schema.dbo
  security = DEFINER
  as       = "BEGIN SELECT 2; END"
}
`), 0o600), qt.IsNil)

	out, err = runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", document, "--auto-approve")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, `holds "ptah.run/mssql/routine-options", which the declaration's source has no way to state`)
	c.Assert(runsAs(c, ctx, db), qt.Equals, "auditor")
}

// TestSchemaApplyLeavesARoutineItsSourceCannotGiveAPrincipalLiveE2E is the
// control for the refusal above: the same HCL document with the body the
// procedure has plans nothing. The principal is left to the procedure rather
// than reported as a difference no HCL document could resolve.
func TestSchemaApplyLeavesARoutineItsSourceCannotGiveAPrincipalLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	url, db := principalDatabase(c, ctx)
	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file",
		writeSQLSchema(c, principalProcedure("'auditor'", "SELECT 1;")), "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	document := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(document, []byte(`schema "dbo" {}
procedure "audit_it" {
  schema   = schema.dbo
  security = DEFINER
  as       = "BEGIN SELECT 1; END"
}
`), 0o600), qt.IsNil)

	out, err = runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", document, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced")
	c.Assert(runsAs(c, ctx, db), qt.Equals, "auditor")
}

// optionRoutines are the routines of stokaro/ptah#4531 written in SQL: a
// function WITH SCHEMABINDING and RETURNS NULL ON NULL INPUT, and a procedure
// WITH RECOMPILE, each with the body given.
func optionRoutines(functionBody, procedureBody string) string {
	return fmt.Sprintf("CREATE FUNCTION dbo.f(@a INT) RETURNS INT WITH SCHEMABINDING, RETURNS NULL ON NULL INPUT AS BEGIN %s END;\nGO\n"+
		"CREATE PROCEDURE dbo.p WITH RECOMPILE AS BEGIN %s END;\nGO\n", functionBody, procedureBody)
}

// heldOptions reads the options sys.sql_modules reports for dbo.f and dbo.p,
// in that order: is_schema_bound, null_on_null_input and is_recompiled.
func heldOptions(c *qt.C, ctx context.Context, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, "SELECT o.name, m.is_schema_bound, m.null_on_null_input, m.is_recompiled FROM sys.sql_modules m "+
		"JOIN sys.objects o ON o.object_id = m.object_id WHERE o.name IN ('f', 'p') ORDER BY o.name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var held []string
	for rows.Next() {
		var name string
		var schemaBound, nullOnNullInput, recompiled bool
		c.Assert(rows.Scan(&name, &schemaBound, &nullOnNullInput, &recompiled), qt.IsNil)
		held = append(held, fmt.Sprintf("%s schema_bound=%t null_on_null_input=%t recompiled=%t", name, schemaBound, nullOnNullInput, recompiled))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// TestSchemaApplyKeepsASQLServerRoutinesOptionsLiveE2E applies the routines
// of stokaro/ptah#4531, then replaces their bodies. The catalog reports every
// option after each apply, and the plan after each is empty: the read reports
// the options as the SQL declares them, and a replacement writes them again.
func TestSchemaApplyKeepsASQLServerRoutinesOptionsLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	url, db := principalDatabase(c, ctx)
	want := []string{
		"f schema_bound=true null_on_null_input=true recompiled=false",
		"p schema_bound=false null_on_null_input=false recompiled=true",
	}
	for _, bodies := range [][2]string{{"RETURN @a;", "SELECT 1;"}, {"RETURN @a + 1;", "SELECT 2;"}} {
		schema := writeSQLSchema(c, optionRoutines(bodies[0], bodies[1]))

		out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(heldOptions(c, ctx, db), qt.DeepEquals, want)
		second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")
		c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
		c.Assert(second, qt.Contains, "Schema is synced")
	}
}

// TestSchemaApplyRemovesASQLServerRoutinesOptionsLiveE2E declares the
// routines of TestSchemaApplyKeepsASQLServerRoutinesOptionsLiveE2E without
// their options. A SQL document states every option a routine has, so the
// plan removes them, and the second plan is empty.
func TestSchemaApplyRemovesASQLServerRoutinesOptionsLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	url, db := principalDatabase(c, ctx)
	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file",
		writeSQLSchema(c, optionRoutines("RETURN @a;", "SELECT 1;")), "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	schema := writeSQLSchema(c, "CREATE FUNCTION dbo.f(@a INT) RETURNS INT AS BEGIN RETURN @a; END;\nGO\n"+
		"CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;\nGO\n")

	out, err = runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(heldOptions(c, ctx, db), qt.DeepEquals, []string{
		"f schema_bound=false null_on_null_input=false recompiled=false",
		"p schema_bound=false null_on_null_input=false recompiled=false",
	})
	second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "Schema is synced")
}

// TestSchemaApplyRefusesAnEncryptedSQLServerRoutineLiveE2E reads a database
// holding a function created WITH ENCRYPTION, whose definition
// sys.sql_modules does not hold. The read refuses it by name rather than read
// it with no body, which a plan would replace without the option.
func TestSchemaApplyRefusesAnEncryptedSQLServerRoutineLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	url, db := principalDatabase(c, ctx)
	_, err := db.ExecContext(ctx, "CREATE FUNCTION dbo.f() RETURNS INT WITH ENCRYPTION AS BEGIN RETURN 1; END")
	c.Assert(err, qt.IsNil)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file",
		writeSQLSchema(c, "CREATE FUNCTION dbo.f() RETURNS INT AS BEGIN RETURN 1; END;\nGO\n"), "--dry-run")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "sqlserver function dbo.f is created WITH ENCRYPTION, and sys.sql_modules holds no definition of it")
}

func TestSchemaApplySQLServerPrincipalCaseLiveE2E(t *testing.T) {
	for _, test := range []struct{ name, collation, users, want string }{
		{"case insensitive", "SQL_Latin1_General_CP1_CI_AS", "CREATE USER auditor WITHOUT LOGIN", "auditor"},
		{"case sensitive", "SQL_Latin1_General_CP1_CS_AS", "CREATE USER auditor WITHOUT LOGIN; CREATE USER Auditor WITHOUT LOGIN", "Auditor"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, db := newSQLServerMigrationDatabase(c, ctx, dbtarget.URL(c, dbtarget.SQLServer))
			_, err := db.ExecContext(ctx, "ALTER DATABASE CURRENT COLLATE "+test.collation)
			c.Assert(err, qt.IsNil)
			_, err = db.ExecContext(ctx, test.users)
			c.Assert(err, qt.IsNil)
			first := writeSQLSchema(c, principalProcedure("'auditor'", "SELECT 1;"))
			out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", first, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			desired := writeSQLSchema(c, principalProcedure("'Auditor'", "SELECT 1;"))
			out, err = runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", desired, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(runsAs(c, ctx, db), qt.Equals, test.want)
			second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", desired, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
			c.Assert(second, qt.Contains, "Schema is synced")
		})
	}
}

// Explicit INLINE settings must survive replacement or stop it, including on
// a function whose body makes both catalog inlineability flags zero.
func TestSchemaApplyRefusesSQLServerInlineLiveE2E(t *testing.T) {
	for _, test := range []struct{ name, option, body string }{
		{"inlineable off", "OFF", "RETURN 1;"},
		{"not inlineable off", "OFF", "DECLARE @t TABLE (v int); RETURN 1;"},
		{"explicit on", "ON", "RETURN 1;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, db := principalDatabase(c, ctx)
			_, err := db.ExecContext(ctx, "CREATE FUNCTION dbo.f() RETURNS INT WITH INLINE = "+test.option+" AS BEGIN "+test.body+" END")
			c.Assert(err, qt.IsNil)
			out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", writeSQLSchema(c, "CREATE FUNCTION dbo.f() RETURNS INT AS BEGIN RETURN 2; END;\nGO\n"), "--dry-run")
			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "sqlserver function dbo.f is created WITH INLINE = "+test.option)
			var definition string
			c.Assert(db.QueryRowContext(ctx, "SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.f'))").Scan(&definition), qt.IsNil)
			c.Assert(definition, qt.Contains, test.body)
		})
	}
}
