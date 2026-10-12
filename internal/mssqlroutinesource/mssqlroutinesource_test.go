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

// option is one claim of a routine's WITH list: the pattern that claimed it,
// the routine header it belongs to, and its text.
type option struct {
	pattern    string
	definition int
	text       string
}

// parseOptions reads the claims of one document, each at offset 30 plus ten
// per claim.
func parseOptions(c *qt.C, options ...option) (sqlext.Result, error) {
	c.Helper()
	claims := make([]sqlext.Claim, 0, len(options))
	for i, option := range options {
		claims = append(claims, sqlext.Claim{Pattern: option.pattern, Site: sqlext.RoutineOption, Subject: "dbo.p",
			Text: option.text, Definition: option.definition, Offset: 30 + 10*i})
	}
	return mssqlroutinesource.SQL().Parser.ParseSQL(c.Context(), sqlext.Request{Target: "sqlserver", Claims: claims})
}

// declared holds options as a declaration does.
func declared(options mssqlschema.RoutineOptions) schemaext.Facets {
	return must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, options))
}

// A principal in quotes, plain or Unicode, is the routine's facet; CALLER,
// OWNER and SELF are the frontend's to read, and give none.
func TestSQL_ReadsThePrincipal(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
		want schemaext.Facets
	}{
		{name: "a quoted user", text: "EXECUTE AS 'auditor'", want: declared(mssqlschema.RoutineOptions{Principal: "auditor"})},
		{name: "a Unicode string", text: "EXECUTE AS N'auditor'", want: declared(mssqlschema.RoutineOptions{Principal: "auditor"})},
		{name: "a doubled quote", text: "execute as N'it''s'", want: declared(mssqlschema.RoutineOptions{Principal: "it's"})},
		{name: "caller", text: "EXECUTE AS CALLER"},
		{name: "owner", text: "EXECUTE AS OWNER"},
		{name: "self", text: "EXECUTE AS SELF"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parseOptions(c, option{pattern: "execute-as", definition: 1, text: test.text})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Values, qt.HasLen, 1)
			c.Assert(result.Values[0].Facets.Equal(test.want), qt.IsTrue, qt.Commentf("%v", result.Values[0].Facets.Kinds()))
		})
	}
}

// The options of one routine become one facet, answered on its first claim;
// its other claims answer an empty value. Each routine gets its own facet.
func TestSQL_CombinesTheOptionsOfOneRoutine(t *testing.T) {
	c := qt.New(t)

	result, err := parseOptions(c,
		option{pattern: "schemabinding", definition: 1, text: "SCHEMABINDING"},
		option{pattern: "returns-null-on-null-input", definition: 1, text: "returns null on null input"},
		option{pattern: "execute-as", definition: 1, text: "EXECUTE AS N'auditor'"},
		option{pattern: "recompile", definition: 2, text: "RECOMPILE"},
		option{pattern: "execute-as", definition: 2, text: "EXECUTE AS OWNER"},
		option{pattern: "execute-as", definition: 3, text: "EXECUTE AS CALLER"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Values, qt.HasLen, 6)
	c.Assert(result.Values[0].Facets.Equal(declared(mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, ReturnsNullOnNullInput: true})), qt.IsTrue)
	c.Assert(result.Values[1].Facets.IsZero(), qt.IsTrue)
	c.Assert(result.Values[2].Facets.IsZero(), qt.IsTrue)
	c.Assert(result.Values[3].Facets.Equal(declared(mssqlschema.RoutineOptions{Recompile: true})), qt.IsTrue)
	c.Assert(result.Values[4].Facets.IsZero(), qt.IsTrue)
	c.Assert(result.Values[5].Facets.IsZero(), qt.IsTrue)
}

// What the facet cannot hold as written is refused at its position, rather
// than read as the owner or as one option.
func TestSQL_ReadsTheOptions_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		options []option
		wantErr string
	}{
		{name: "a bare word", options: []option{{pattern: "execute-as", definition: 1, text: "EXECUTE AS auditor"}},
			wantErr: `.*EXECUTE AS auditor of dbo.p at position 30: EXECUTE AS names CALLER, SELF, OWNER or a user name in quotes, not auditor`},
		{name: "two principals in one clause", options: []option{{pattern: "execute-as", definition: 1, text: "EXECUTE AS 'a' 'b'"}},
			wantErr: `.*EXECUTE AS takes one principal`},
		{name: "an empty name", options: []option{{pattern: "execute-as", definition: 1, text: "EXECUTE AS ''"}},
			wantErr: `.*must name a database user.*`},
		{name: "two EXECUTE AS clauses", wantErr: `.*EXECUTE AS 'b' of dbo.p at position 40: the routine states EXECUTE AS twice`,
			options: []option{{pattern: "execute-as", definition: 1, text: "EXECUTE AS 'a'"}, {pattern: "execute-as", definition: 1, text: "EXECUTE AS 'b'"}}},
		{name: "an option twice", wantErr: `.*RECOMPILE of dbo.p at position 40: the routine states the option twice`,
			options: []option{{pattern: "recompile", definition: 1, text: "RECOMPILE"}, {pattern: "recompile", definition: 1, text: "RECOMPILE"}}},
		{name: "an option with a value", options: []option{{pattern: "schemabinding", definition: 1, text: "SCHEMABINDING = ON"}},
			wantErr: `.*SCHEMABINDING = ON of dbo.p at position 30: the option takes no value`},
		{name: "a partial null input rule", options: []option{{pattern: "returns-null-on-null-input", definition: 1, text: "RETURNS NULL ON INPUT"}},
			wantErr: `.*RETURNS NULL ON INPUT of dbo.p at position 30: the option is RETURNS NULL ON NULL INPUT`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parseOptions(c, test.options...)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Values, qt.IsNil)
		})
	}
}

// A SQL Server document describes the options of every routine it declares.
func TestSQL_ClaimsTheOptions(t *testing.T) {
	c := qt.New(t)

	claim, err := mssqlroutinesource.SQL().Coverage(sqlext.CoverageRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(claim.Equal(must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))), qt.IsTrue)
}

func TestSQL_RefusesRepeatedSecurityOptions(t *testing.T) {
	for _, test := range []struct{ name, first, second string }{
		{"caller then owner", "EXECUTE AS CALLER", "EXECUTE AS OWNER"},
		{"owner then user", "EXECUTE AS OWNER", "EXECUTE AS 'auditor'"},
		{"user then caller", "EXECUTE AS 'auditor'", "EXECUTE AS CALLER"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := parseOptions(c, option{pattern: "execute-as", definition: 1, text: test.first}, option{pattern: "execute-as", definition: 1, text: test.second})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*states.*twice.*`)
			c.Assert(result, qt.DeepEquals, sqlext.Result{})
		})
	}
}
