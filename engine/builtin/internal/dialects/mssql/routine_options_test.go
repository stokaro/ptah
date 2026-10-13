package mssql_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
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

// The options a routine states are written in its header's WITH list, where
// the server reads them from: sys.sql_modules.execute_as_principal_id reports
// OWNER as -2, a named user by its id and CALLER as NULL, which is why
// INVOKER takes no clause at all, and is_schema_bound, null_on_null_input and
// is_recompiled report the rest.
func TestRenderer_ARoutineWritesTheOptionsItStates(t *testing.T) {
	for _, test := range []struct {
		name     string
		security string
		options  mssqlschema.RoutineOptions
		want     string
	}{
		{name: "a named user", security: "DEFINER", options: mssqlschema.RoutineOptions{Principal: "auditor"},
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH EXECUTE AS N'auditor'\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "a quote in the name", security: "DEFINER", options: mssqlschema.RoutineOptions{Principal: "o'brien"},
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH EXECUTE AS N'o''brien'\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "the owner", security: "DEFINER",
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH EXECUTE AS OWNER\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "the caller", security: "INVOKER",
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "every option of a function", security: "DEFINER",
			options: mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, ReturnsNullOnNullInput: true},
			want:    "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH SCHEMABINDING, RETURNS NULL ON NULL INPUT, EXECUTE AS N'auditor'\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "schema binding beside the owner", security: "DEFINER", options: mssqlschema.RoutineOptions{SchemaBinding: true},
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH SCHEMABINDING, EXECUTE AS OWNER\nAS\nBEGIN RETURN 1; END;\n"},
		{name: "schema binding beside the caller", security: "INVOKER", options: mssqlschema.RoutineOptions{SchemaBinding: true},
			want: "CREATE OR ALTER FUNCTION [dbo].[audit_it]()\nRETURNS int\nWITH SCHEMABINDING\nAS\nBEGIN RETURN 1; END;\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := mssql.New().Render(principalFunction(test.security, routineOptions(c, test.options)))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// A procedure writes its WITH list after its parameters, as T-SQL takes it.
func TestRenderer_AProcedureWritesTheOptionsItStates(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateFunction("dbo.p_report").
		SetKind(schemamodel.FunctionKindProcedure).
		SetParameters("@id int").
		SetLanguage("sql").
		SetSecurity("DEFINER").
		SetBody("BEGIN SELECT @id; END")
	node.Facets = routineOptions(c, mssqlschema.RoutineOptions{Principal: "auditor", Recompile: true})

	sql, err := mssql.New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE OR ALTER PROCEDURE [dbo].[p_report] @id int\nWITH RECOMPILE, EXECUTE AS N'auditor'\nAS\nBEGIN SELECT @id; END;\n")
}

// routineOptions returns facets stating options, and none for no option.
func routineOptions(c *qt.C, options mssqlschema.RoutineOptions) schemaext.Facets {
	c.Helper()
	if options.IsZero() {
		return schemaext.Facets{}
	}
	facets, err := mssqlschema.WithRoutineOptions(schemaext.Facets{}, options)
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
			facets: must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: "auditor"})),
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
