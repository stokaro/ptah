package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A function that declares OUT arguments may leave RETURNS out, and PostgreSQL
// records the type the arguments imply. The comparison fills that type in on
// the declared side, or the empty clause differs from the catalog's and every
// plan drops and creates the function again (stokaro/ptah#3690). A procedure
// has no result, and the catalog reports none.
func TestFunctionDefinitions_AnOmittedReturnIsTheOneTheArgumentsImply(t *testing.T) {
	tests := []struct {
		name       string
		kind       string
		parameters string
		observed   string
	}{
		{name: "one OUT argument", parameters: "a integer, out b text", observed: "text"},
		{name: "two OUT arguments", parameters: "a integer, out b integer, out c text", observed: "record"},
		{name: "an INOUT argument", parameters: "inout a int4", observed: "integer"},
		{name: "a procedure", kind: schemamodel.FunctionKindProcedure, parameters: "a integer, out b text", observed: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitionsWithDialect(
				schemamodel.Function{Name: "f", Kind: test.kind, Parameters: test.parameters},
				catalog.Function{Name: "f", Kind: test.kind, Parameters: test.parameters, Returns: test.observed},
				platform.Postgres,
			)

			c.Assert(diff.Changes["returns"], qt.Equals, "")
		})
	}
}

// The control: an implied type is compared, not skipped. A function the
// catalog holds as SETOF text is not the one a declaration without RETURNS
// describes, and a declared RETURNS is compared as written.
func TestFunctionDefinitions_AnImpliedReturnStillSeesAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "a set against the implied type", observed: "SETOF text"},
		{name: "a declared return is not replaced", declared: "integer", observed: "text"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitionsWithDialect(
				schemamodel.Function{Name: "f", Parameters: "a integer, out b text", Returns: test.declared},
				catalog.Function{Name: "f", Parameters: "a integer, out b text", Returns: test.observed},
				platform.Postgres,
			)

			c.Assert(diff.Changes["returns"], qt.Not(qt.Equals), "")
		})
	}
}
