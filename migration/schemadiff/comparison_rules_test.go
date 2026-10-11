package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// differingIndexes reads every index's options as changed, and records the
// indexes it was asked about.
type differingIndexes struct {
	asked *[]string
}

func (r differingIndexes) OptionsEqual(desired schemamodel.Index, _ catalog.Index) bool {
	*r.asked = append(*r.asked, desired.Name)
	return false
}

func (differingIndexes) RenameCarries(schemaext.Kind) bool { return false }

// foldingViews reads every pair of queries as one query.
type foldingViews struct{}

func (foldingViews) QueryEqual(_, _ string) bool { return true }

// rulesRuntime registers target "custom", alias "alternate", with rules.
func rulesRuntime(rules schemacomparison.Rules) *engine.Runtime {
	return must.Must(engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{
		Name: "custom", Aliases: []string{"alternate"}, Preparation: schemapreparation.Identity{}, Comparison: rules,
	}}}))
}

// indexedItems is table items with index by_price on both sides, the same
// on each, and a view whose query is spelled differently on each.
func indexedItems() (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields:  []schemamodel.Field{{StructName: "Item", Name: "price", Type: "BIGINT", Nullable: true}},
		Indexes: []schemamodel.Index{{StructName: "Item", TableName: "items", Name: "by_price", Fields: []string{"price"}}},
		Views:   []schemamodel.View{{Name: "cheap", Body: "SELECT price FROM items"}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "items", Type: "TABLE", Columns: []catalog.Column{
			{Name: "price", DataType: "BIGINT", ColumnType: "BIGINT", IsNullable: "YES", OrdinalPosition: 1},
		}}},
		Indexes: []catalog.Index{{TableName: "items", Name: "by_price", Columns: []string{"price"}}},
		Views:   []catalog.View{{Name: "cheap", Body: "select price from (items)"}},
	}
	return desired, current
}

// TestCompare_ReachesTheTargetsIndexRules shows that a comparison reads an
// index's options through the rules the selected target registered, and
// rebuilds an index whose options they call changed. The rules are asked
// twice: once for the renames the feature comparison reads first, and once
// for the diff. The control registers none, and the target, which no shared
// rule measures, keeps the index.
func TestCompare_ReachesTheTargetsIndexRules(t *testing.T) {
	var asked []string
	cases := []struct {
		name        string
		rules       schemacomparison.Rules
		wantRebuilt []difftypes.IndexRef
		wantAsked   []string
	}{
		{name: "registered", rules: schemacomparison.Rules{Indexes: differingIndexes{asked: &asked}},
			wantRebuilt: []difftypes.IndexRef{{TableName: "items", Name: "by_price"}}, wantAsked: []string{"by_price", "by_price"}},
		{name: "absent"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			asked = nil
			desired, current := indexedItems()

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", rulesRuntime(tt.rules))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.IndexAdditions(), qt.DeepEquals, tt.wantRebuilt)
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, tt.wantRebuilt)
			c.Assert(asked, qt.DeepEquals, tt.wantAsked)
		})
	}
}

// TestCompare_ReachesTheTargetsViewRules shows that a comparison reads a
// view's query through the rules the selected target registered, in place of
// the shared reading. The control registers none, and the shared reading
// calls the two spellings different queries.
func TestCompare_ReachesTheTargetsViewRules(t *testing.T) {
	cases := []struct {
		name         string
		rules        schemacomparison.Rules
		wantModified int
	}{
		{name: "registered", rules: schemacomparison.Rules{Views: foldingViews{}}, wantModified: 0},
		{name: "absent", wantModified: 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := indexedItems()

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "custom", rulesRuntime(tt.rules))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, tt.wantModified)
		})
	}
}
