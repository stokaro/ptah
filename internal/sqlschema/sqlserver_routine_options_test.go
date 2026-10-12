package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
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
// by name, each option the model has no place for without the SQL Server
// owner, which reads some of them. Each was measured on SQL Server 2025
// RTM-CU9: the server accepts it, and the catalog reports it (ENCRYPTION by
// keeping no definition at all), so a routine kept without it would be one the
// author did not write, and would compare as changed or never be compared.
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

// The options the SQL Server owner reads reach the routine as one facet, and
// the document claims to describe them: a user the routine runs as makes it a
// definer routine, and SCHEMABINDING, RECOMPILE and RETURNS NULL ON NULL INPUT
// are kept beside it.
func TestReadWith_ASQLServerRoutineKeepsTheOptionsItsOwnerReads(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("sqlserver"))
	for _, test := range []struct {
		name         string
		sql          string
		subject      objectidentity.ID
		wantSecurity string
		want         mssqlschema.RoutineOptions
	}{
		{name: "a user", sql: "CREATE PROCEDURE dbo.p WITH EXECUTE AS N'auditor' AS BEGIN SELECT 1; END;\nGO\n",
			subject: builder.Procedure("dbo.p", ""), wantSecurity: "DEFINER", want: mssqlschema.RoutineOptions{Principal: "auditor"}},
		{name: "recompile", sql: "CREATE PROCEDURE dbo.p WITH RECOMPILE AS BEGIN SELECT 1; END;\nGO\n",
			subject: builder.Procedure("dbo.p", ""), want: mssqlschema.RoutineOptions{Recompile: true}},
		{name: "schema binding", sql: "CREATE FUNCTION dbo.f() RETURNS INT WITH SCHEMABINDING AS BEGIN RETURN 1; END;\nGO\n",
			subject: builder.Function("dbo.f", ""), want: mssqlschema.RoutineOptions{SchemaBinding: true}},
		{name: "every option of a function", subject: builder.Function("dbo.f", "int"), wantSecurity: "DEFINER",
			sql:  "CREATE FUNCTION dbo.f(@a INT) RETURNS INT WITH SCHEMABINDING, RETURNS NULL ON NULL INPUT, EXECUTE AS 'auditor' AS BEGIN RETURN @a; END;\nGO\n",
			want: mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, ReturnsNullOnNullInput: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := readWithOwners([]byte(test.sql), "sqlserver")

			c.Assert(err, qt.IsNil)
			c.Assert(database.Functions, qt.HasLen, 1)
			options, found, err := mssqlschema.RoutineOptionsOf(database.Functions[0].Facets)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(options, qt.Equals, test.want)
			c.Assert(database.Functions[0].Security, qt.Equals, test.wantSecurity)
			c.Assert(database.FeatureCoverage.Lookup(mssqlschema.RoutineOptionsKind, test.subject).State, qt.Equals, schemaext.Complete)
		})
	}
}

// Each routine of a document gets the options its own header writes, however
// many routines the document declares.
func TestReadWith_EachSQLServerRoutineKeepsItsOwnOptions(t *testing.T) {
	c := qt.New(t)

	database, _, err := readWithOwners([]byte("CREATE FUNCTION dbo.f() RETURNS INT WITH SCHEMABINDING, EXECUTE AS 'auditor' AS BEGIN RETURN 1; END;\nGO\n"+
		"CREATE PROCEDURE dbo.p WITH RECOMPILE, EXECUTE AS 'reviewer' AS BEGIN SELECT 1; END;\nGO\n"), "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 2)
	got := make(map[string]mssqlschema.RoutineOptions)
	for _, function := range database.Functions {
		options, _, err := mssqlschema.RoutineOptionsOf(function.Facets)
		c.Assert(err, qt.IsNil)
		got[function.Name] = options
	}
	c.Assert(got, qt.DeepEquals, map[string]mssqlschema.RoutineOptions{
		"dbo.f": {Principal: "auditor", SchemaBinding: true},
		"dbo.p": {Principal: "reviewer", Recompile: true},
	})
}

// With the owner selected, an option it does not read is refused by name as
// it is without one.
func TestReadWith_ASQLServerRoutineKeepsTheOptionsItsOwnerReads_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "encryption", sql: "CREATE PROCEDURE dbo.p WITH ENCRYPTION, RECOMPILE AS SELECT 1 AS a;\nGO\n",
			wantErr: `.*the sqlserver procedure dbo.p states ENCRYPTION, which Ptah can neither write nor compare back.*`},
		{name: "inline", sql: "CREATE FUNCTION dbo.f() RETURNS INT WITH SCHEMABINDING, INLINE = OFF AS BEGIN RETURN 1; END;\nGO\n",
			wantErr: `.*the sqlserver function dbo.f states INLINE = OFF, .*`},
		{name: "for replication", sql: "CREATE PROCEDURE dbo.p WITH RECOMPILE FOR REPLICATION AS SELECT 1 AS a;\nGO\n",
			wantErr: `.*the sqlserver procedure dbo.p states FOR REPLICATION, .*`},
		{name: "self", sql: "CREATE PROCEDURE dbo.p WITH EXECUTE AS SELF AS SELECT 1 AS a;\nGO\n",
			wantErr: `.*the sqlserver procedure dbo.p states EXECUTE AS SELF.*write EXECUTE AS 'name'.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := readWithOwners([]byte(test.sql), "sqlserver")

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Functions, qt.HasLen, 0)
		})
	}
}

// Routine claims keep distinct principals when one document declares several routines.
func TestReadWith_EachSQLServerRoutineKeepsItsPrincipal(t *testing.T) {
	c := qt.New(t)
	database, _, err := readWithOwners([]byte("CREATE FUNCTION dbo.f() RETURNS INT WITH EXECUTE AS 'auditor' BEGIN RETURN 1; END;\nGO\nCREATE PROCEDURE dbo.p WITH EXECUTE AS 'reviewer' AS BEGIN SELECT 1; END;\nGO\n"), "sqlserver")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 2)
	got := make(map[string]string)
	for _, function := range database.Functions {
		options, found, err := mssqlschema.RoutineOptionsOf(function.Facets)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		got[function.Name] = options.Principal
	}
	c.Assert(got, qt.DeepEquals, map[string]string{"dbo.f": "auditor", "dbo.p": "reviewer"})
}

// Null-input rules occupy one header slot even when a rule states the default.
func TestReadWith_SQLServerNullInputRules_FailurePath(t *testing.T) {
	for _, options := range []string{
		"CALLED ON NULL INPUT, RETURNS NULL ON NULL INPUT",
		"RETURNS NULL ON NULL INPUT, CALLED ON NULL INPUT",
		"CALLED ON NULL INPUT, CALLED ON NULL INPUT",
		"RETURNS NULL ON NULL INPUT, RETURNS NULL ON NULL INPUT",
	} {
		t.Run(options, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := readWithOwners([]byte("CREATE FUNCTION dbo.f(@a INT) RETURNS INT WITH "+options+" AS BEGIN RETURN @a; END;"), "sqlserver")
			c.Assert(err, qt.ErrorMatches, `.*states the option twice.*`)
			c.Assert(database.Functions, qt.HasLen, 0)
		})
	}
}
