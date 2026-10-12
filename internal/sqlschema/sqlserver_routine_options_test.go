package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// TestToDatabase_ASQLServerRoutineRunsAsItsHeaderSays pins that EXECUTE AS
// CALLER and EXECUTE AS OWNER reach the model as the security mode the reader
// reports for them, and that CALLED ON NULL INPUT, the default, is accepted.
func TestToDatabase_ASQLServerRoutineRunsAsItsHeaderSays(t *testing.T) {
	for _, test := range []struct {
		name         string
		sql          string
		wantName     string
		wantSecurity string
		wantBody     string
	}{
		{name: "owner", sql: "CREATE PROCEDURE dbo.po WITH EXECUTE AS OWNER AS BEGIN SELECT 1; END",
			wantName: "dbo.po", wantSecurity: "DEFINER", wantBody: "BEGIN SELECT 1; END"},
		{name: "caller", sql: "CREATE PROCEDURE dbo.pc WITH EXECUTE AS CALLER AS BEGIN SELECT 1; END",
			wantName: "dbo.pc", wantSecurity: "INVOKER", wantBody: "BEGIN SELECT 1; END"},
		{name: "a function with the default null handling", sql: "CREATE FUNCTION dbo.f(@a INT) RETURNS INT WITH CALLED ON NULL INPUT, EXECUTE AS OWNER AS BEGIN RETURN @a; END",
			wantName: "dbo.f", wantSecurity: "DEFINER", wantBody: "BEGIN RETURN @a; END"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			routines := routinesOf(c, "sqlserver", test.sql)

			c.Assert(routines, qt.HasLen, 1)
			c.Assert([]string{routines[0].Name, routines[0].Security, routines[0].Body}, qt.DeepEquals,
				[]string{test.wantName, test.wantSecurity, test.wantBody})
		})
	}
}

// TestToDatabase_ASQLServerRoutineRunsAsItsHeaderSays_FailurePath refuses,
// by name, each option the model has no place for. Each was measured on SQL
// Server 2025 RTM-CU9: the server accepts it, and the catalog reports it
// (ENCRYPTION by keeping no definition at all), so a routine kept without it
// would be one the author did not write, and would compare as changed or never
// be compared.
func TestToDatabase_ASQLServerRoutineRunsAsItsHeaderSays_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "recompile", sql: "CREATE PROCEDURE dbo.p WITH RECOMPILE AS SELECT 1 AS a",
			wantErr: `.*the sqlserver procedure dbo.p states RECOMPILE, which Ptah can neither write nor compare back.*`},
		{name: "encryption", sql: "CREATE PROCEDURE dbo.p WITH ENCRYPTION AS SELECT 1 AS a",
			wantErr: `.*the sqlserver procedure dbo.p states ENCRYPTION, .*`},
		{name: "for replication", sql: "CREATE PROCEDURE dbo.p FOR REPLICATION AS SELECT 1 AS a",
			wantErr: `.*the sqlserver procedure dbo.p states FOR REPLICATION, .*`},
		{name: "schemabinding", sql: "CREATE FUNCTION dbo.f() RETURNS INT WITH SCHEMABINDING AS BEGIN RETURN 1; END",
			wantErr: `.*the sqlserver function dbo.f states SCHEMABINDING, .*`},
		{name: "null on null input", sql: "CREATE FUNCTION dbo.f(@a INT) RETURNS INT WITH RETURNS NULL ON NULL INPUT AS BEGIN RETURN @a; END",
			wantErr: `.*the sqlserver function dbo.f states RETURNS NULL ON NULL INPUT, .*`},
		{name: "inline", sql: "CREATE FUNCTION dbo.f() RETURNS INT WITH INLINE = OFF AS BEGIN RETURN 1; END",
			wantErr: `.*the sqlserver function dbo.f states INLINE = OFF, .*`},
		{name: "native compilation", sql: "CREATE PROCEDURE dbo.p WITH NATIVE_COMPILATION, SCHEMABINDING AS BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english') SELECT 1 AS a; END",
			wantErr: `.*the sqlserver procedure dbo.p states NATIVE_COMPILATION, .*`},
		{name: "an option after a known one", sql: "CREATE PROCEDURE dbo.p WITH EXECUTE AS OWNER, RECOMPILE AS SELECT 1 AS a",
			wantErr: `.*the sqlserver procedure dbo.p states RECOMPILE, .*`},
		{name: "self", sql: "CREATE PROCEDURE dbo.p WITH EXECUTE AS SELF AS SELECT 1 AS a",
			wantErr: `.*the sqlserver procedure dbo.p states EXECUTE AS SELF, which the catalog records as the principal that created it.*write EXECUTE AS 'name'.*`},
		{name: "a user no owner reads", sql: "CREATE PROCEDURE dbo.p WITH EXECUTE AS 'auditor' AS SELECT 1 AS a",
			wantErr: `.*the sqlserver procedure dbo.p states EXECUTE AS 'auditor', which Ptah can neither write nor compare back.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect("sqlserver")).Parse()
			c.Assert(err, qt.IsNil)

			database, err := sqlschema.ToDatabase(statements, "sqlserver")

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Functions, qt.HasLen, 0)
		})
	}
}

// A user the routine runs as is the SQL Server owner's to read: with the
// bundled owners selected, the routine is a definer routine whose facet names
// the user, and the document claims to describe the clause.
func TestReadWith_ASQLServerRoutineRunsAsTheUserItNames(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.ReadWith(c.Context(),
		[]byte("CREATE PROCEDURE dbo.p WITH EXECUTE AS N'auditor' AS BEGIN SELECT 1; END;\nGO\n"), "sqlserver", nil,
		sqlschema.Options{Owners: must.Must(builtin.New()).SQL()})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 1)
	principal, found, err := mssqlschema.ExecuteAsPrincipal(database.Functions[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert([]any{database.Functions[0].Security, principal, found}, qt.DeepEquals, []any{"DEFINER", "auditor", true})
	c.Assert(database.FeatureCoverage.Lookup(mssqlschema.ExecuteAsKind, objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).Procedure("dbo.p", "")).State,
		qt.Equals, schemaext.Complete)
}

// Routine claims keep distinct principals when one document declares several routines.
func TestReadWith_EachSQLServerRoutineKeepsItsPrincipal(t *testing.T) {
	c := qt.New(t)
	database, _, err := readWithOwners([]byte("CREATE FUNCTION dbo.f() RETURNS INT WITH EXECUTE AS 'auditor' BEGIN RETURN 1; END;\nGO\nCREATE PROCEDURE dbo.p WITH EXECUTE AS 'reviewer' AS BEGIN SELECT 1; END;\nGO\n"), "sqlserver")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 2)
	got := make(map[string]string)
	for _, function := range database.Functions {
		principal, found, err := mssqlschema.ExecuteAsPrincipal(function.Facets)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		got[function.Name] = principal
	}
	c.Assert(got, qt.DeepEquals, map[string]string{"dbo.f": "auditor", "dbo.p": "reviewer"})
}
