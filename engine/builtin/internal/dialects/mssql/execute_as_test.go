package mssql_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin/internal/dialects/mssql"
)

// tenancy is a routine setting another owner states. SQL Server has no clause
// for it.
type tenancy struct{}

func (tenancy) Kind() schemaext.Kind             { return "ptah.run/test/tenancy" }
func (tenancy) Clone() schemaext.Value           { return tenancy{} }
func (tenancy) Equal(other schemaext.Value) bool { return other == schemaext.Value(tenancy{}) }

// principalFunction is a SQL Server function with the security mode and the
// facets given.
func principalFunction(security string, facets schemaext.Facets) *ast.CreateFunctionNode {
	node := ast.NewCreateFunction("dbo.audit_it").
		SetReturns("int").
		SetLanguage("sql").
		SetSecurity(security).
		SetBody("BEGIN RETURN 1; END")
	node.Facets = facets
	return node
}

// The principal a routine runs as is written in its header, where the server
// reads it from. sys.sql_modules.execute_as_principal_id reports OWNER as -2
// and a named user by its id, and CALLER as NULL, which is why INVOKER takes
// no clause at all.
func TestRenderer_ARoutineRunsAsThePrincipalItStates(t *testing.T) {
	for _, test := range []struct {
		name      string
		security  string
		principal string
		want      string
	}{
		{name: "a named user", security: "DEFINER", principal: "auditor",
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH EXECUTE AS N'auditor'\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "a quote in the name", security: "DEFINER", principal: "o'brien",
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH EXECUTE AS N'o''brien'\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "the owner", security: "DEFINER",
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH EXECUTE AS OWNER\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "the caller", security: "INVOKER",
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nAS\nBEGIN RETURN 1; END;\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := mssql.New().Render(principalFunction(test.security, executeAs(c, test.principal)))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// executeAs returns facets stating the principal, and none for an empty one.
func executeAs(c *qt.C, principal string) schemaext.Facets {
	c.Helper()
	if principal == "" {
		return schemaext.Facets{}
	}
	facets, err := mssqlschema.WithExecuteAs(schemaext.Facets{}, principal)
	c.Assert(err, qt.IsNil)
	return facets
}

// A routine is not created without what it states: a named principal beside
// INVOKER contradicts itself, and a setting SQL Server has no clause for would
// be dropped.
func TestRenderer_ARoutineHeaderRefusesWhatItCannotWrite(t *testing.T) {
	for _, test := range []struct {
		name     string
		security string
		facets   schemaext.Facets
		want     string
	}{
		{name: "a user and the caller", security: "INVOKER",
			facets: must.Must(mssqlschema.WithExecuteAs(schemaext.Facets{}, "auditor")),
			want:   `function "dbo.audit_it" runs as its caller and as "auditor" at once; a named principal is a definer's rights`},
		{name: "another owner's setting", security: "DEFINER",
			facets: must.Must(schemaext.NewFacets(tenancy{})),
			want:   `function "dbo.audit_it" carries "ptah.run/test/tenancy", which SQL Server has no clause for`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := mssql.New().Render(principalFunction(test.security, test.facets))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*: `+regexp.QuoteMeta(test.want))
			c.Assert(sql, qt.Equals, "")
		})
	}
}
