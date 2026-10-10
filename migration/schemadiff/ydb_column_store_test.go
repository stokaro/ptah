package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A format incapable of declaring column storage keeps the held storage,
// while an explicit row-table declaration reports the incompatible change.
// Neither changes the declaration it was given.
func TestCompare_YDBColumnStoreFormatLimit(t *testing.T) {
	for _, test := range []struct {
		name          string
		coverage      schemaext.Coverage
		modifications int
	}{
		{name: "explicit row storage", coverage: must.Must(ydbschema.ColumnStoreCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)), modifications: 1},
		{name: "format cannot describe storage"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := ydbFamilyDeclaration()
			desired.FeatureCoverage = test.coverage
			current := ydbFamilyCatalog()
			current.Tables[0].Facets = must.Must(must.Must(schemaext.NewFacets(&ydbschema.ObservedColumnStore{
				ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, Partitions: 8},
			})).WithTargetScope(ydbschema.ColumnStoreKind, platform.YDB))
			current.FeatureCoverage = must.Must(ydbschema.ColumnStoreCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, test.modifications)
			c.Assert(desired.Tables[0].Facets.Len(), qt.Equals, 0)
		})
	}
}
