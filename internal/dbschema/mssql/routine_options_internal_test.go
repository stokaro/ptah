package mssql

// White-box testing required: the row is what sys.sql_modules reports of a
// routine, which only a live server produces; the live suite drives the
// options a routine can be created with, and this pins the mapping and the
// refusals for options the facet cannot hold.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Each catalog column reaches the facet, the values measured on SQL Server
// 2025 RTM-CU9: execute_as_principal_id is NULL for CALLER and -2 for OWNER,
// which the security mode carries, and a user's id otherwise.
func TestRoutineOptionsRow_Facets(t *testing.T) {
	for _, test := range []struct {
		name string
		row  routineOptionsRow
		want mssqlschema.RoutineOptions
	}{
		{name: "the caller", row: routineOptionsRow{executeAs: "CALLER", inlined: true, inlineable: true}},
		{name: "the owner", row: routineOptionsRow{executeAs: "-2"}},
		{name: "a user", row: routineOptionsRow{executeAs: "5", principal: "auditor"}, want: mssqlschema.RoutineOptions{Principal: "auditor"}},
		{name: "every option", row: routineOptionsRow{executeAs: "5", principal: "auditor", schemaBound: true, recompiled: true, nullOnNullInput: true},
			want: mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, Recompile: true, ReturnsNullOnNullInput: true}},
		{name: "a function the server cannot inline", row: routineOptionsRow{executeAs: "CALLER", schemaBound: true},
			want: mssqlschema.RoutineOptions{SchemaBinding: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			facets, err := test.row.facets("function", "dbo.f")

			c.Assert(err, qt.IsNil)
			c.Assert(facets.Equal(observedOptions(test.want)), qt.IsTrue)
		})
	}
}

// observedOptions holds options as a read records them, and none for none.
func observedOptions(options mssqlschema.RoutineOptions) schemaext.Facets {
	if options.IsZero() {
		return schemaext.Facets{}
	}
	return must.Must(mssqlschema.WithObservedRoutineOptions(schemaext.Facets{}, options))
}

// An option the facet cannot hold is refused by name rather than read as
// absent, and so is a principal the read cannot name.
func TestRoutineOptionsRow_Facets_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		row     routineOptionsRow
		wantErr string
	}{
		{name: "encryption", row: routineOptionsRow{encrypted: 1, executeAs: "CALLER"},
			wantErr: `unsupported feature: sqlserver function dbo.f is created WITH ENCRYPTION, and sys.sql_modules holds no definition of it; Ptah can neither compare nor replace it`},
		{name: "native compilation", row: routineOptionsRow{nativelyCompiled: true, executeAs: "-2", schemaBound: true},
			wantErr: `unsupported feature: sqlserver function dbo.f is created WITH NATIVE_COMPILATION, which Ptah cannot write; Ptah can neither compare nor replace it`},
		{name: "explicit inline off without eligibility", row: routineOptionsRow{inlineOption: "INLINE = OFF", executeAs: "CALLER"},
			wantErr: `unsupported feature: sqlserver function dbo.f is created WITH INLINE = OFF, which Ptah cannot write; Ptah can neither compare nor replace it`},
		{name: "explicit inline on", row: routineOptionsRow{inlineOption: "INLINE = ON", executeAs: "CALLER"},
			wantErr: `unsupported feature: sqlserver function dbo.f is created WITH INLINE = ON, which Ptah cannot write; Ptah can neither compare nor replace it`},
		{name: "inline off", row: routineOptionsRow{inlineable: true, executeAs: "CALLER"},
			wantErr: `unsupported feature: sqlserver function dbo.f is created WITH INLINE = OFF, which Ptah cannot write; Ptah can neither compare nor replace it`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			facets, err := test.row.facets("function", "dbo.f")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(facets.IsZero(), qt.IsTrue)
		})
	}
}

// A principal id USER_NAME does not name is refused, not read as the owner.
func TestRoutineOptionsRow_Facets_AnUnnamedPrincipal(t *testing.T) {
	c := qt.New(t)

	facets, err := routineOptionsRow{executeAs: "9"}.facets("procedure", "dbo.p")

	c.Assert(err, qt.ErrorMatches, `routine "dbo.p" runs as principal id 9, which USER_NAME does not name`)
	c.Assert(facets.IsZero(), qt.IsTrue)
}

func TestRoutineInlineOption(t *testing.T) {
	for _, test := range []struct{ name, definition, want string }{
		{"off", "CREATE FUNCTION dbo.f() RETURNS INT WITH INLINE = OFF AS BEGIN RETURN 1; END", "INLINE = OFF"},
		{"on and comments", "CREATE FUNCTION dbo.f() RETURNS INT WITH SCHEMABINDING, /* inline */ INLINE /* value */ = ON AS BEGIN RETURN 1; END;", "INLINE = ON"},
		{"no body AS", "CREATE FUNCTION dbo.f() RETURNS INT WITH INLINE = OFF BEGIN RETURN 1; END", "INLINE = OFF"},
		{"body string", "CREATE FUNCTION dbo.f() RETURNS VARCHAR(40) AS BEGIN RETURN 'WITH INLINE = OFF'; END", ""},
		{"principal string", "CREATE FUNCTION dbo.f() RETURNS INT WITH EXECUTE AS 'INLINE = OFF' AS BEGIN RETURN 1; END", ""},
		{"comment", "CREATE FUNCTION dbo.f() RETURNS INT WITH /* INLINE = OFF */ SCHEMABINDING AS BEGIN RETURN 1; END", ""},
		{"with parameter", "CREATE FUNCTION dbo.f(@with INT) RETURNS INT AS BEGIN RETURN @with; END", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(routineInlineOption(test.definition, "FN"), qt.Equals, test.want)
		})
	}
}
