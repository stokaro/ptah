package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// undescribedStorage is a description that does not describe the storage
// parameters of the materialized view named, as an HCL document records.
func undescribedStorage(view string) coverage.Set {
	return coverage.Set{}.With(coverage.Object{Kind: coverage.MaterializedViewOptions, Name: view, Reason: coverage.Unsupported})
}

// TestHeldMaterializedViewOptions pins which materialized views take the
// storage parameters the database holds: the ones whose description says it
// does not describe them, matched by the default schema where a name has
// none, and no other.
func TestHeldMaterializedViewOptions(t *testing.T) {
	semantics := identifier.ForDialect("postgres")
	semantics.SearchPath = []string{"public"}
	current := &catalog.Database{MatViews: []catalog.MaterializedView{
		{Name: "totals", Schema: "public", Body: "SELECT 1", Options: []string{"fillfactor=70"}},
		// A live read reports a view in the default schema without one.
		{Name: "daily", Body: "SELECT 1", Options: []string{"fillfactor=80"}},
	}}
	tests := []struct {
		name         string
		view         string
		notDescribed coverage.Set
		want         []string
	}{
		{name: "not described, named with its schema", view: "public.totals", notDescribed: undescribedStorage("public.totals"),
			want: []string{"fillfactor=70"}},
		{name: "not described, named without one", view: "totals", notDescribed: undescribedStorage("totals"),
			want: []string{"fillfactor=70"}},
		{name: "not described, held in the default schema without one", view: "public.daily", notDescribed: undescribedStorage("public.daily"),
			want: []string{"fillfactor=80"}},
		{name: "described", view: "public.totals"},
		{name: "another view not described", view: "public.totals", notDescribed: undescribedStorage("public.other")},
		{name: "a view the database does not hold", view: "public.fresh", notDescribed: undescribedStorage("public.fresh")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				MaterializedViews: []schemamodel.MaterializedView{{Name: test.view, Body: "SELECT 1"}},
				NotDescribed:      test.notDescribed,
			}

			got := compare.HeldMaterializedViewOptions(desired, current, semantics)

			c.Assert(got.MaterializedViews, qt.HasLen, 1)
			c.Assert(got.MaterializedViews[0].Options, qt.DeepEquals, test.want)
			c.Assert(desired.MaterializedViews[0].Options, qt.IsNil)
		})
	}
}
