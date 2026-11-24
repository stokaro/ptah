package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestViewDefinitionsWithDialect_AQualifierOfAnotherSchemaIsAChange compares a
// view of the schema " a" whose stored body reads a.src. The qualifier names
// the schema a, not the view's own " a", so it is not the one a catalog adds
// and the comparison keeps it: the body differs from one that reads the src
// of its own schema.
func TestViewDefinitionsWithDialect_AQualifierOfAnotherSchemaIsAChange(t *testing.T) {
	c := qt.New(t)

	diff := compare.ViewDefinitionsWithDialect(
		schemamodel.View{Name: "v", Body: "SELECT id FROM src"},
		catalog.View{Name: "v", Schema: " a", Body: "SELECT id FROM a.src"},
		platform.Postgres,
	)

	c.Assert(diff.Changes["body"], qt.Equals, "SELECT id FROM a.src -> SELECT id FROM src")
}

// TestViewDefinitionsWithDialect_AQualifierOfItsOwnSchemaIsNoChange is the
// control: the qualifier a catalog adds for the view's own schema a is still
// removed, so the test above cannot pass on a comparison that strips nothing.
func TestViewDefinitionsWithDialect_AQualifierOfItsOwnSchemaIsNoChange(t *testing.T) {
	c := qt.New(t)

	diff := compare.ViewDefinitionsWithDialect(
		schemamodel.View{Name: "v", Body: "SELECT id FROM src"},
		catalog.View{Name: "v", Schema: "a", Body: "SELECT id FROM a.src"},
		platform.Postgres,
	)

	c.Assert(diff.Changes["body"], qt.Equals, "")
}
