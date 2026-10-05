package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/migration/schemadiff"
)

// A format incapable of declaring column storage keeps the held storage,
// while an explicit row-table declaration reports the incompatible change.
func TestCompare_YDBColumnStoreFormatLimit(t *testing.T) {
	for _, test := range []struct {
		name          string
		undescribed   coverage.Set
		modifications int
	}{
		{name: "explicit row storage", modifications: 1},
		{name: "format cannot describe storage", undescribed: coverage.Set{}.With(coverage.Object{Kind: coverage.ColumnTable, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact})},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := ydbFamilyDeclaration()
			desired.NotDescribed = test.undescribed
			current := ydbFamilyCatalog()
			current.Tables[0].YDBColumnTable = &ast.YDBColumnTableSpec{HashColumns: []string{"id"}, Partitions: 8}
			diff := schemadiff.CompareWithDialect(desired, current, platform.YDB)
			c.Assert(diff.TablesModified, qt.HasLen, test.modifications)
			c.Assert(desired.Tables[0].YDBColumnTable, qt.IsNil)
		})
	}
}
