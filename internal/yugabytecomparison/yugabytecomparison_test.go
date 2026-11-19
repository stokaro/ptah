package yugabytecomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/yugabytecomparison"
)

// TestIndexes_OptionsEqual reads a declared index's access method and each
// key's operator class and NULLS ordering against what YugabyteDB reports.
// YugabyteDB builds btree and hash as lsm and gin as ybgin, and its reader
// records only a NULLS ordering against the direction's default.
func TestIndexes_OptionsEqual(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Index
		reported catalog.Index
		want     bool
	}{
		{
			name:     "no method is lsm",
			declared: schemamodel.Index{Fields: []string{"a"}},
			reported: catalog.Index{Method: "lsm", Parts: []catalog.IndexPart{{Name: "a"}}},
			want:     true,
		},
		{
			name:     "BTREE and HASH are lsm",
			declared: schemamodel.Index{Type: "HASH", Fields: []string{"a"}},
			reported: catalog.Index{Method: "lsm", Columns: []string{"a"}},
			want:     true,
		},
		{
			name:     "GIN is ybgin",
			declared: schemamodel.Index{Type: "GIN", Fields: []string{"j"}},
			reported: catalog.Index{Method: "ybgin", Parts: []catalog.IndexPart{{Name: "j"}}},
			want:     true,
		},
		{
			name:     "an index-level operator class applies to each key",
			declared: schemamodel.Index{Operator: "TEXT_PATTERN_OPS", Fields: []string{"a"}},
			reported: catalog.Index{Method: "lsm", Parts: []catalog.IndexPart{{Name: "a", Operator: "text_pattern_ops"}}},
			want:     true,
		},
		{
			name: "an explicit spelling of the default ordering",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{
				{Name: "a", NullsOrder: "LAST"}, {Name: "b", Desc: true, NullsOrder: "FIRST"},
			}},
			reported: catalog.Index{Method: "lsm", Parts: []catalog.IndexPart{{Name: "a"}, {Name: "b", Desc: true}}},
			want:     true,
		},
		{
			name:     "lsm against ybgin",
			declared: schemamodel.Index{Fields: []string{"j"}},
			reported: catalog.Index{Method: "ybgin", Parts: []catalog.IndexPart{{Name: "j"}}},
			want:     false,
		},
		{
			name:     "a changed operator class",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a", Operator: "text_pattern_ops"}}},
			reported: catalog.Index{Method: "lsm", Parts: []catalog.IndexPart{{Name: "a"}}},
			want:     false,
		},
		{
			name:     "a changed NULLS ordering",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "b", NullsOrder: "FIRST"}}},
			reported: catalog.Index{Method: "lsm", Parts: []catalog.IndexPart{{Name: "b"}}},
			want:     false,
		},
		{
			name:     "a NULLS ordering only the second key changes",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a"}, {Name: "b", NullsOrder: "FIRST"}}},
			reported: catalog.Index{Method: "lsm", Parts: []catalog.IndexPart{{Name: "a"}, {Name: "b"}}},
			want:     false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := yugabytecomparison.Indexes{}.OptionsEqual(test.declared, test.reported)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestIndexes_RenameCarries keeps every facet through a rename, because
// ALTER INDEX ... RENAME TO changes the name alone.
func TestIndexes_RenameCarries(t *testing.T) {
	c := qt.New(t)

	c.Assert(yugabytecomparison.Indexes{}.RenameCarries(schemaext.Kind("any.index.facet")), qt.IsTrue)
}

// TestRules_RegistersIndexesAlone is the shape the built-in runtime installs:
// index rules and nothing else, so the other comparisons stay shared.
func TestRules_RegistersIndexesAlone(t *testing.T) {
	c := qt.New(t)

	rules := yugabytecomparison.Rules()

	c.Assert(rules.Indexes, qt.Equals, yugabytecomparison.Indexes{})
	c.Assert(rules.Columns, qt.IsNil)
	c.Assert(rules.Views, qt.IsNil)
	c.Assert(rules.Routines, qt.IsNil)
}
