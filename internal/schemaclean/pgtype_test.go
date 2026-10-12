package schemaclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/schemaclean"
)

// TestPlanFromSchemaNamesAnOwnersTypes pins that a plan names the types the
// PostgreSQL type owner observed as it names common ones, on a dialect whose
// writer drops types, and names none where the writer drops no type. The
// writer drops what the database holds, whichever model the read handed it
// to.
func TestPlanFromSchemaNamesAnOwnersTypes(t *testing.T) {
	read := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.DomainKind, "app", "email"), &pgtype.ObservedDomain{BaseType: "text"})),
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.CompositeKind, "public", "addr"),
			&pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "text"}}})),
		must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.RangeKind, "public", "span"), &pgtype.ObservedRange{Subtype: "int4"})),
	))}
	tests := []struct {
		name    string
		dialect string
		want    []schemaclean.Object
	}{
		{name: "postgres", dialect: "postgres", want: []schemaclean.Object{
			{Type: "composite", Schema: "public", Name: "addr"},
			{Type: "domain", Schema: "app", Name: "email"},
			{Type: "range", Schema: "public", Name: "span"},
		}},
		{name: "mysql", dialect: "mysql", want: make([]schemaclean.Object, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			plan := schemaclean.PlanFromSchema(read, test.dialect)

			c.Assert(plan.Objects, qt.DeepEquals, test.want)
		})
	}
}
