package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/tableidentity"
	"ptah.run/migration/schemadiff/internal/compare"
)

// exactNulls compares the NULLS order of each key part of a declared index
// with the held index's, as the rules of a target that prepares the orders
// do. It records the parts it is handed.
type exactNulls struct{ handed *[]schemamodel.IndexPart }

func (r exactNulls) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	*r.handed = append(*r.handed, desired.Parts...)
	for i, part := range desired.Parts {
		if part.NullsOrder != current.Parts[i].NullsOrder {
			return false
		}
	}
	return true
}

func (exactNulls) RenameCarries(schemaext.Kind) bool { return false }

// statedNullsTables is table t prepared with the NULLS orders its
// preparation states for the key parts of index i.
func statedNullsTables(orders ...string) map[objectidentity.Key]schemapreparation.Table {
	subject := tableidentity.Subject("", "t", fixtureTarget, identifier.ForDialect(fixtureTarget))
	return map[objectidentity.Key]schemapreparation.Table{subject.Key(): {Subject: subject, IndexNulls: map[string][]string{"i": orders}}}
}

// The rules compare the NULLS order a table's preparation states for each key
// part of a declared index, written as the source wrote it or not at all,
// with the held order, and the declaration keeps what the source wrote.
func TestIndexes_PreparedNullsOrdersReachTheRules(t *testing.T) {
	ref := []difftypes.IndexRef{{Name: "i", TableName: "t"}}
	for _, row := range []struct {
		name     string
		declared string
		prepared map[objectidentity.Key]schemapreparation.Table
		held     string
		replaced []difftypes.IndexRef
		handed   []schemamodel.IndexPart
	}{
		{"the default spelled out, prepared as the default", schemamodel.NullsOrderFirst, statedNullsTables(""), "", nil, []schemamodel.IndexPart{{Name: "a"}}},
		{"another order, prepared as it is", schemamodel.NullsOrderLast, statedNullsTables(schemamodel.NullsOrderLast), "", ref,
			[]schemamodel.IndexPart{{Name: "a", NullsOrder: schemamodel.NullsOrderLast}}},
		{"no order, against a held order", "", statedNullsTables(""), schemamodel.NullsOrderFirst, ref, []schemamodel.IndexPart{{Name: "a"}}},
		// Without a stated order the rules read the parts the source wrote.
		{"nothing prepared", schemamodel.NullsOrderFirst, nil, "", ref, []schemamodel.IndexPart{{Name: "a", NullsOrder: schemamodel.NullsOrderFirst}}},
		{"orders that do not number the key", schemamodel.NullsOrderFirst, statedNullsTables("", ""), "", ref,
			[]schemamodel.IndexPart{{Name: "a", NullsOrder: schemamodel.NullsOrderFirst}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "t"}},
				Indexes: []schemamodel.Index{{Name: "i", TableName: "t", Parts: []schemamodel.IndexPart{{Name: "a", NullsOrder: row.declared}}}}}
			current := &catalog.Database{Indexes: []catalog.Index{{Name: "i", TableName: "t", Columns: []string{"a"}, Parts: []catalog.IndexPart{{Name: "a", NullsOrder: row.held}}}}}
			var handed []schemamodel.IndexPart
			diff := &difftypes.SchemaDiff{}
			compare.IndexesWithSemantics(desired, current, diff, fixtureTarget, identifier.ForDialect(fixtureTarget), nil,
				renamingCaps, schemacomparison.Rules{Indexes: exactNulls{handed: &handed}}, row.prepared)
			c.Assert(diff.IndexesRemoved, qt.DeepEquals, row.replaced)
			c.Assert(handed, qt.DeepEquals, row.handed)
			c.Assert(desired.Indexes[0].Parts[0].NullsOrder, qt.Equals, row.declared)
		})
	}
}

// A declared index written with fields and no parts is handed to the rules
// with one part for each field, in order, carrying the stated orders.
func TestIndexes_PreparedNullsOrdersOfFields(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "t"}},
		Indexes: []schemamodel.Index{{Name: "i", TableName: "t", Fields: []string{"a", "b"}}}}
	current := &catalog.Database{Indexes: []catalog.Index{{Name: "i", TableName: "t", Columns: []string{"a", "b"},
		Parts: []catalog.IndexPart{{Name: "a"}, {Name: "b", NullsOrder: schemamodel.NullsOrderLast}}}}}
	var handed []schemamodel.IndexPart
	diff := &difftypes.SchemaDiff{}

	compare.IndexesWithSemantics(desired, current, diff, fixtureTarget, identifier.ForDialect(fixtureTarget), nil,
		renamingCaps, schemacomparison.Rules{Indexes: exactNulls{handed: &handed}}, statedNullsTables("", schemamodel.NullsOrderLast))

	c.Assert(diff.IndexesRemoved, qt.IsNil)
	c.Assert(handed, qt.DeepEquals, []schemamodel.IndexPart{{Name: "a"}, {Name: "b", NullsOrder: schemamodel.NullsOrderLast}})
	c.Assert(desired.Indexes[0].Parts, qt.IsNil)
}
