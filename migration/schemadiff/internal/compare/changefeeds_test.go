package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestAdoptUndescribedChangefeeds takes from the database each changefeed the
// declaration neither declares nor describes. A changefeed the declaration
// declares keeps its declaration, so a name is never adopted twice, and the
// declaration passed in is left as it was: the caller may hold it beyond this
// comparison.
func TestAdoptUndescribedChangefeeds(t *testing.T) {
	held := ast.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	keys := ast.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	redeclared := ast.ChangefeedSpec{Name: "updates", Mode: "NEW_IMAGE", Format: "JSON"}
	database := &catalog.Database{Tables: []catalog.Table{{Name: "events", Changefeeds: []ast.ChangefeedSpec{held, keys}}}}
	tests := []struct {
		name     string
		declared []ast.ChangefeedSpec
		want     []ast.ChangefeedSpec
	}{
		{name: "none declared", want: []ast.ChangefeedSpec{held, keys}},
		{name: "one declared under a name the database holds", declared: []ast.ChangefeedSpec{redeclared},
			want: []ast.ChangefeedSpec{redeclared, keys}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables:       []schemamodel.Table{{Name: "events", Changefeeds: test.declared}},
				NotDescribed: coverage.Set{}.WithKind(coverage.Changefeed),
			}

			adopted := compare.AdoptUndescribedChangefeeds(desired, database, platform.YDB,
				identifier.ForDialect(platform.YDB))

			c.Assert(adopted.Tables[0].Changefeeds, qt.DeepEquals, test.want)
			c.Assert(desired.Tables[0].Changefeeds, qt.DeepEquals, test.declared)
		})
	}
}
