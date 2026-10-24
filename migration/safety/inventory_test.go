package safety_test

import (
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/safety"
)

func TestInventory_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []safety.InventoryEntry
	}{
		{
			name: "a diff that paired nothing counts nothing",
			diff: &difftypes.SchemaDiff{},
			want: make([]safety.InventoryEntry, 0),
		},
		{
			name: "pairs count on both sides",
			diff: &difftypes.SchemaDiff{Matched: difftypes.MatchedCounts{Tables: 3, Columns: 7}},
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 7, Observed: 7},
				{Kind: "tables", Desired: 3, Observed: 3},
			},
		},
		{
			name: "an addition counts on the desired side and a removal on the database side",
			diff: &difftypes.SchemaDiff{
				Matched:       difftypes.MatchedCounts{Tables: 2},
				TablesAdded:   difftypes.TableChanges{{Name: "orders"}},
				TablesRemoved: difftypes.TableRemovals{{Name: "legacy"}, {Name: "audit"}},
			},
			want: []safety.InventoryEntry{{Kind: "tables", Desired: 3, Observed: 4}},
		},
		{
			name: "a kind only one side holds is reported with a zero",
			diff: &difftypes.SchemaDiff{
				IndexesAdded: difftypes.IndexChanges{{TableName: "users"}, {TableName: "orders"}},
			},
			want: []safety.InventoryEntry{{Kind: "indexes", Desired: 2, Observed: 0}},
		},
		{
			name: "columns are summed over the tables both sides hold",
			diff: &difftypes.SchemaDiff{
				Matched: difftypes.MatchedCounts{Tables: 2, Columns: 5},
				TablesModified: []difftypes.TableDiff{
					{TableName: "users", ColumnsAdded: difftypes.ColumnChanges{{Name: "nick"}}},
					{
						TableName:      "orders",
						ColumnsAdded:   difftypes.ColumnChanges{{Name: "total"}},
						ColumnsRemoved: difftypes.ColumnChanges{{Name: "sum"}, {Name: "legacy"}},
					},
				},
			},
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 7, Observed: 7},
				{Kind: "tables", Desired: 2, Observed: 2},
			},
		},
		{
			name: "enum values are summed over the enum types both sides hold",
			diff: &difftypes.SchemaDiff{
				Matched: difftypes.MatchedCounts{Enums: 2, EnumValues: 4},
				EnumsModified: []difftypes.EnumDiff{
					{EnumName: "mood", ValuesAdded: []string{"calm"}},
					{EnumName: "status", ValuesRemoved: []string{"gone", "lost"}},
				},
			},
			want: []safety.InventoryEntry{
				{Kind: "enum_values", Desired: 5, Observed: 6},
				{Kind: "enums", Desired: 2, Observed: 2},
			},
		},
		{
			name: "a removed procedure is a routine the database holds",
			diff: &difftypes.SchemaDiff{
				Matched:           difftypes.MatchedCounts{Functions: 1},
				FunctionsRemoved:  difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "old_fn"}}},
				ProceduresRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "old_proc"}}},
			},
			want: []safety.InventoryEntry{{Kind: "functions", Desired: 1, Observed: 3}},
		},
		{
			name: "entries are sorted by kind",
			diff: &difftypes.SchemaDiff{
				Matched: difftypes.MatchedCounts{Tables: 1, Indexes: 1, Constraints: 1, Columns: 1, Roles: 1},
			},
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 1, Observed: 1},
				{Kind: "constraints", Desired: 1, Observed: 1},
				{Kind: "indexes", Desired: 1, Observed: 1},
				{Kind: "roles", Desired: 1, Observed: 1},
				{Kind: "tables", Desired: 1, Observed: 1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(safety.Inventory(test.diff), qt.DeepEquals, test.want)
		})
	}
}

func TestInventory_NilDiff(t *testing.T) {
	c := qt.New(t)

	c.Assert(safety.Inventory(nil), qt.IsNil)
}

// inventoryKinds are the category stems the inventory reports. Each counts a
// kind of object the comparison pairs one by one.
var inventoryKinds = []string{
	"columns", "constraints", "enum_values", "enums", "functions", "indexes",
	"rls_enabled_tables", "rls_policies", "roles", "schemas", "tables",
}

// notInventoryKinds are the category stems the inventory does not report,
// each with the reason it is not a kind of object the inventory can count.
var notInventoryKinds = map[string]string{
	"unique_protections": "counts index removals a UNIQUE constraint enforces, each one of the indexes_removed already",
	"rls_force":          "counts a table's FORCE ROW LEVEL SECURITY flag turning on or off, a setting of a table rather than an object",
	"vector_dimension":   "counts column modifications, which columns_modified counts as well",
	"table_constraints":  "counts a table's own constraint lists, which the comparison never fills; it reports every constraint it compares under constraints",
	"feature_changes":    "counts the changes a feature owner reported; the owner pairs its objects and reports only the ones that differ",
}

// TestInventory_KindsAreTheFindingStems holds the inventory and the findings to
// one vocabulary. Every category the classifier reports for a diff with an
// entry in every list either is the stem of an inventory kind or is named
// above with the reason it is not, and the inventory of that diff reports
// exactly the kinds.
//
// A category added to the classifier fails the first assertion until the
// decision is written down here. An inventory kind no category names fails
// the second.
func TestInventory_KindsAreTheFindingStems(t *testing.T) {
	c := qt.New(t)
	diff := everyListDiff(c)

	findings := safety.ClassifySchemaDiff(diff)

	stems := make(map[string]bool)
	for _, finding := range findings {
		stems[findingStem(finding.Category)] = true
	}
	want := slices.Clone(inventoryKinds)
	for stem := range notInventoryKinds {
		want = append(want, stem)
	}
	slices.Sort(want)
	c.Assert(slices.Sorted(maps.Keys(stems)), qt.DeepEquals, want)

	var reported []string
	for _, entry := range safety.Inventory(diff) {
		reported = append(reported, entry.Kind)
	}
	c.Assert(reported, qt.DeepEquals, inventoryKinds)
}

// TestInventory_CountsWhatTheFindingsCount holds each kind's sides to its
// findings: with no pair, what the desired side holds of a kind is what
// stem_added counts and what the database holds is what stem_removed counts.
func TestInventory_CountsWhatTheFindingsCount(t *testing.T) {
	c := qt.New(t)
	diff := everyListDiff(c)

	counts := make(map[string]int)
	for _, finding := range safety.ClassifySchemaDiff(diff) {
		counts[finding.Category] = finding.Count
	}
	inventory := safety.Inventory(diff)

	c.Assert(inventory, qt.HasLen, len(inventoryKinds))
	for _, entry := range inventory {
		c.Assert(entry, qt.Equals, safety.InventoryEntry{
			Kind: entry.Kind, Desired: counts[entry.Kind+"_added"], Observed: counts[entry.Kind+"_removed"],
		})
	}
}

// TestInventory_ReadsEveryMatchedCount sets one pair count at a time and
// requires the inventory to report it, once, under a kind of its own. A count
// the comparison tallies and the inventory never reads is a kind that reads as
// held by neither side.
func TestInventory_ReadsEveryMatchedCount(t *testing.T) {
	fields := reflect.TypeFor[difftypes.MatchedCounts]().NumField()
	c := qt.New(t)
	c.Assert(fields > 0, qt.IsTrue, qt.Commentf("MatchedCounts has no field; this test asserts nothing"))

	kinds := make(map[string]bool)
	for index := range fields {
		field := reflect.TypeFor[difftypes.MatchedCounts]().Field(index)
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			reflect.ValueOf(&diff.Matched).Elem().Field(index).SetInt(1)

			inventory := safety.Inventory(diff)

			c.Assert(inventory, qt.HasLen, 1)
			c.Assert(inventory[0].Desired, qt.Equals, 1)
			c.Assert(inventory[0].Observed, qt.Equals, 1)
			kinds[inventory[0].Kind] = true
		})
	}
	c.Assert(kinds, qt.HasLen, fields, qt.Commentf("two pair counts are reported under one kind: %v", kinds))
}

// TestClassifySchemaDiff_CountsARemovedProcedureAsARemovedFunction pins the
// finding a dropped procedure reports. The diff keeps procedure removals apart
// because the DROP verb differs, and without this a drift report on a database
// holding an undeclared procedure said drift with no finding at all.
func TestClassifySchemaDiff_CountsARemovedProcedureAsARemovedFunction(t *testing.T) {
	c := qt.New(t)

	findings := safety.ClassifySchemaDiff(&difftypes.SchemaDiff{
		ProceduresRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "archive_orders"}}},
	})

	c.Assert(findings, qt.DeepEquals, []safety.Finding{
		{Category: "functions_removed", Count: 1, Severity: safety.Destructive},
	})
}

// everyListDiff is a diff with one zero entry in every list the diff, its
// table and its enum modifications carry, and the two entries the classifier
// tells apart by value: a FORCE turned on beside the one turned off, and a
// column whose vector dimension changes.
func everyListDiff(c *qt.C) *difftypes.SchemaDiff {
	c.Helper()
	diff := &difftypes.SchemaDiff{}
	fillLists(reflect.ValueOf(diff).Elem())
	table := difftypes.TableDiff{}
	fillLists(reflect.ValueOf(&table).Elem())
	table.ColumnsModified[0].Changes = map[string]string{"type": "vector(3) -> vector(4)"}
	enum := difftypes.EnumDiff{}
	fillLists(reflect.ValueOf(&enum).Elem())
	diff.TablesModified = []difftypes.TableDiff{table}
	diff.EnumsModified = []difftypes.EnumDiff{enum}
	diff.RLSForceChanged = append(diff.RLSForceChanged, schemamodel.RLSEnabledTable{Forced: true})
	c.Assert(diff.HasChanges(), qt.IsTrue)
	return diff
}

// fillLists puts one zero element in every exported slice field of value.
func fillLists(value reflect.Value) {
	for index := range value.NumField() {
		field := value.Type().Field(index)
		if field.IsExported() && field.Type.Kind() == reflect.Slice {
			value.Field(index).Set(reflect.MakeSlice(field.Type, 1, 1))
		}
	}
}

// findingStem is the kind a category names: the category without its
// direction, and the generic feature category for every feature category,
// which names an owner's change kind rather than a kind the inventory counts.
func findingStem(category string) string {
	if strings.HasPrefix(category, "feature_") {
		return "feature_changes"
	}
	for _, suffix := range []string{"_added", "_removed", "_modified", "_changed"} {
		if stem, found := strings.CutSuffix(category, suffix); found {
			return stem
		}
	}
	return category
}
