package schemadiff_test

import (
	"maps"
	"strings"
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

// literalColumns is a target whose catalog spells every type with a _T
// suffix and every default as lit(<value>), and which creates every column
// NOT NULL.
type literalColumns struct{}

func (literalColumns) TypeChange(desired schemamodel.Field, _ catalog.Column, currentType string) string {
	if strings.EqualFold(desired.Type+"_T", currentType) {
		return ""
	}
	return currentType + " -> " + desired.Type
}

func (literalColumns) DefaultChange(_ schemamodel.Field, _ catalog.Column, declared, held string) string {
	if "lit("+declared+")" == held {
		return ""
	}
	return held + " -> " + declared
}

func (literalColumns) AlwaysNotNull(schemamodel.Field) bool { return true }

func (literalColumns) SequenceChanges(schemamodel.Field, catalog.Column) map[string]string {
	return nil
}

func (literalColumns) DefaultValue(reported string) (string, bool) {
	return strings.CutSuffix(strings.TrimPrefix(reported, "lit("), ")")
}

// literalItems is table items with column price, declared as the column
// literalColumns reads the catalog's column to be, and column legacy, which
// only the catalog holds.
func literalItems() (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "price", Type: "money", Nullable: true, Default: "5"}},
	}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "items", Type: "TABLE", Columns: []catalog.Column{
		{Name: "price", DataType: "MONEY_T", ColumnType: "MONEY_T", IsNullable: "NO", OrdinalPosition: 1, ColumnDefault: new("lit(5)")},
		{Name: "legacy", DataType: "TEXT", ColumnType: "TEXT", IsNullable: "NO", OrdinalPosition: 2, ColumnDefault: new("lit(x)")},
	}}}}
	return desired, current
}

// TestCompare_ReachesTheTargetsColumnRules shows that a comparison reads a
// column's type, default and nullability through the rules the selected
// target registered, and describes a dropped column with the default its
// rules read back. The control registers none, and the shared reading calls
// the default an expression it cannot match and the column nullable.
func TestCompare_ReachesTheTargetsColumnRules(t *testing.T) {
	cases := []struct {
		name        string
		rules       schemacomparison.Rules
		wantChanges map[string]string
		wantRemoved schemamodel.Field
	}{
		{name: "registered", rules: schemacomparison.Rules{Columns: literalColumns{}}, wantChanges: make(map[string]string),
			wantRemoved: schemamodel.Field{Name: "legacy", Type: "TEXT", Default: "x", DefaultSet: true}},
		{name: "absent", wantChanges: map[string]string{"default_expr": "lit(5) -> 5", "nullable": "false -> true", "type": "money_t -> money"},
			wantRemoved: schemamodel.Field{Name: "legacy", Type: "TEXT", DefaultExpr: "lit(x)"}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := literalItems()

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "custom", rulesRuntime(tt.rules))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(tableColumnChanges(diff.TablesModified[0]), qt.DeepEquals, tt.wantChanges)
			c.Assert(diff.TablesModified[0].ColumnsRemoved, qt.HasLen, 1)
			removed := diff.TablesModified[0].ColumnsRemoved[0]
			c.Assert([]string{removed.Name, removed.Type, removed.Default, removed.DefaultExpr},
				qt.DeepEquals, []string{tt.wantRemoved.Name, tt.wantRemoved.Type, tt.wantRemoved.Default, tt.wantRemoved.DefaultExpr})
			c.Assert(removed.DefaultSet, qt.Equals, tt.wantRemoved.DefaultSet)
		})
	}
}

// tableColumnChanges merges the changes of every modified column of table.
func tableColumnChanges(table difftypes.TableDiff) map[string]string {
	changes := make(map[string]string)
	for _, column := range table.ColumnsModified {
		maps.Copy(changes, column.Changes)
	}
	return changes
}
