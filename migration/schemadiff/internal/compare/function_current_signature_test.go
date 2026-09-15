package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A modification carries the argument list that addresses the routine as the
// database holds it. A planner that rebuilds the routine runs its drop against
// the catalog, and the declared parameter list describes the routine the change
// creates instead (stokaro/ptah#3288).

func TestFunctionDefinitionsWithDialect_CarriesTheCatalogIdentity(t *testing.T) {
	c := qt.New(t)

	diff := compare.FunctionDefinitionsWithDialect(
		schemamodel.Function{Name: "total", Parameters: "m bigint", Returns: "bigint", Language: "sql", Body: "SELECT 1"},
		catalog.Function{
			Name:              "total",
			Parameters:        "n integer DEFAULT 1",
			IdentityArguments: new("n integer"),
			Returns:           "integer",
			Language:          "sql",
			Body:              "SELECT 1",
		},
		platform.Postgres,
	)

	c.Assert(diff.Changes["returns"], qt.Equals, "integer -> bigint")
	c.Assert(diff.CurrentSignature, qt.Equals, "n integer")
}

// A reader that supplies no identity leaves the recorded parameters, as the
// catalog spelled them rather than as the comparison folds them.
func TestFunctionDefinitionsWithDialect_CarriesTheRecordedParametersWithoutAnIdentity(t *testing.T) {
	c := qt.New(t)

	diff := compare.FunctionDefinitionsWithDialect(
		schemamodel.Function{Name: "total", Parameters: "n integer", Returns: "bigint", Language: "sql", Body: "SELECT 1"},
		catalog.Function{Name: "total", Parameters: "IN n integer", Returns: "integer", Language: "sql", Body: "SELECT 1"},
		platform.Postgres,
	)

	c.Assert(diff.Changes["returns"], qt.Equals, "integer -> bigint")
	c.Assert(diff.CurrentSignature, qt.Equals, "IN n integer")
}
