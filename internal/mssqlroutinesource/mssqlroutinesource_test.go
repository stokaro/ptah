package mssqlroutinesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/mssqlroutinesource"
)

// parseExecuteAs reads one claim of the owner's pattern.
func parseExecuteAs(c *qt.C, text string) (sqlext.Result, error) {
	c.Helper()
	extension := mssqlroutinesource.SQL()
	return extension.Parser.ParseSQL(c.Context(), sqlext.Request{Target: "sqlserver", Claims: []sqlext.Claim{{
		Pattern: extension.Patterns[0].Name, Site: sqlext.RoutineOption, Subject: "dbo.p", Text: text, Offset: 30,
	}}})
}

// A principal in quotes, plain or Unicode, is the routine's facet; CALLER,
// OWNER and SELF are the frontend's to read, and give none.
func TestSQL_ReadsThePrincipal(t *testing.T) {
	declared := func(principal string) schemaext.Facets {
		return must.Must(mssqlschema.WithExecuteAs(schemaext.Facets{}, principal))
	}
	for _, test := range []struct {
		name string
		text string
		want schemaext.Facets
	}{
		{name: "a quoted user", text: "EXECUTE AS 'auditor'", want: declared("auditor")},
		{name: "a Unicode string", text: "EXECUTE AS N'auditor'", want: declared("auditor")},
		{name: "a doubled quote", text: "execute as N'it''s'", want: declared("it's")},
		{name: "caller", text: "EXECUTE AS CALLER"},
		{name: "owner", text: "EXECUTE AS OWNER"},
		{name: "self", text: "EXECUTE AS SELF"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parseExecuteAs(c, test.text)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Values, qt.HasLen, 1)
			c.Assert(result.Values[0].Facets.Equal(test.want), qt.IsTrue, qt.Commentf("%v", result.Values[0].Facets.Kinds()))
		})
	}
}

// What names no principal is refused at its position, rather than read as the
// owner.
func TestSQL_ReadsThePrincipal_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		text    string
		wantErr string
	}{
		{name: "a bare word", text: "EXECUTE AS auditor", wantErr: `.*EXECUTE AS auditor of dbo.p at position 30: EXECUTE AS names CALLER, SELF, OWNER or a user name in quotes, not auditor`},
		{name: "two principals", text: "EXECUTE AS 'a' 'b'", wantErr: `.*EXECUTE AS takes one principal`},
		{name: "an empty name", text: "EXECUTE AS ''", wantErr: `.*must name a database user.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parseExecuteAs(c, test.text)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Values, qt.IsNil)
		})
	}
}

// A SQL Server document describes the clause of every routine it declares.
func TestSQL_ClaimsTheClause(t *testing.T) {
	c := qt.New(t)

	claim, err := mssqlroutinesource.SQL().Coverage(sqlext.CoverageRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(claim.Equal(must.Must(mssqlschema.ExecuteAsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))), qt.IsTrue)
}

func TestSQL_RefusesRepeatedSecurityOptions(t *testing.T) {
	for _, test := range []struct{ name, first, second string }{
		{"caller then owner", "EXECUTE AS CALLER", "EXECUTE AS OWNER"},
		{"owner then user", "EXECUTE AS OWNER", "EXECUTE AS 'auditor'"},
		{"user then caller", "EXECUTE AS 'auditor'", "EXECUTE AS CALLER"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := mssqlroutinesource.SQL().Parser.ParseSQL(c.Context(), sqlext.Request{Target: "sqlserver", Claims: []sqlext.Claim{
				{Pattern: "execute-as", Definition: 1, Text: test.first},
				{Pattern: "execute-as", Definition: 1, Text: test.second},
			}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*states.*twice.*`)
			c.Assert(result, qt.DeepEquals, sqlext.Result{})
		})
	}
}
