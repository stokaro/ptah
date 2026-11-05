package generator

// White-box testing required: the prepared tables are planning parents only
// a provider's planner reads, and no built-in planner renders from them, so no
// public plan shows which table a reverse index change is planned against.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/builtintest"
)

// indexedPreparation is a forward plan that adds the index items_label to
// the table items, which it otherwise leaves alone, with the prepared capture
// of the table as the forward comparison takes it: declared with the index,
// held without it. prior is the table as the database held it.
func indexedPreparation() (forward, reverse *difftypes.SchemaDiff, prior *schemamodel.Database) {
	table := schemamodel.Table{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}}
	fields := []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INTEGER", Primary: true}, {StructName: "Item", Name: "label", Type: "TEXT", Nullable: true}}
	index := schemamodel.Index{StructName: "Item", Name: "items_label", Fields: []string{"label"}}
	prior = &schemamodel.Database{Tables: []schemamodel.Table{table}, Fields: fields}
	held := schemacapture.TableObservation{Table: catalog.Table{Name: "items", Columns: []catalog.Column{
		{Name: "id", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		{Name: "label", DataType: "TEXT", ColumnType: "TEXT", IsNullable: "YES", OrdinalPosition: 2},
	}}}
	declared := schemacapture.TableDeclaration{Table: table, Fields: fields, Indexes: []schemamodel.Index{index}}
	preparation := &schemapreparation.Capture{Prepared: []schemapreparation.Table{{Desired: declared, Current: held}}}
	forward = &difftypes.SchemaDiff{TablePreparation: preparation, IndexesAdded: difftypes.IndexChanges{{Index: index, TableName: "items"}}}
	reverse = &difftypes.SchemaDiff{TablePreparation: cloneTablePreparation(preparation), IndexesRemoved: []difftypes.IndexRef{{Name: "items_label", TableName: "items"}}}
	return forward, reverse, prior
}

// The reverse plan reads a table whose only change is an index the forward
// plan adds as the forward plan leaves it, the index included, and moves it
// to the declaration the database held, without the index. The forward
// capture is left as it was.
func TestReverseTablePreparationProjectsTheForwardIndexChanges(t *testing.T) {
	c := qt.New(t)
	forward, reverse, prior := indexedPreparation()

	err := reverseTablePreparation(t.Context(), forward, reverse, prior, "sqlite", builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	prepared := reverse.TablePreparation.Prepared[0]
	c.Assert(prepared.Current.Indexes, qt.HasLen, 1)
	c.Assert(prepared.Current.Indexes[0].Name, qt.Equals, "items_label")
	c.Assert(prepared.Current.Table.Columns, qt.HasLen, 2)
	c.Assert(prepared.Desired.Table.Name, qt.Equals, "items")
	c.Assert(prepared.Desired.Indexes, qt.HasLen, 0)
	c.Assert(prepared.Desired.Fields, qt.HasLen, 2)
	c.Assert(forward.TablePreparation.Prepared[0].Current.Indexes, qt.HasLen, 0)
	c.Assert(forward.TablePreparation.Prepared[0].Desired.Indexes, qt.HasLen, 1)
}

// A table the reverse plan alters is planned from its own capture, which
// replaces the prepared one, so the prepared entry is left as the forward
// comparison took it.
func TestReverseTablePreparationLeavesAnAlteredTable(t *testing.T) {
	c := qt.New(t)
	forward, reverse, prior := indexedPreparation()
	reverse.TablesModified = []difftypes.TableDiff{{TableName: "items"}}

	err := reverseTablePreparation(t.Context(), forward, reverse, prior, "sqlite", builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(reverse.TablePreparation.Prepared[0].Current.Indexes, qt.HasLen, 0)
	c.Assert(reverse.TablePreparation.Prepared[0].Desired.Indexes, qt.HasLen, 1)
}

// keyedPreparation is a forward plan that changes the key of the table items
// from (b, a) to (a, b), which it otherwise leaves alone, with the prepared
// capture of the table as the forward comparison takes it: declared with the
// new key, held with the old one. prior is the table as the database held it.
func keyedPreparation() (forward, reverse *difftypes.SchemaDiff, prior *schemamodel.Database) {
	held := catalog.Constraint{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY", ColumnName: "b", ColumnNames: []string{"b", "a"}}
	fields := []schemamodel.Field{{StructName: "Item", Name: "a", Type: "INTEGER"}, {StructName: "Item", Name: "b", Type: "TEXT"}}
	prior = &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"b", "a"}}}, Fields: fields}
	current := schemacapture.TableObservation{Table: catalog.Table{Name: "items", Columns: []catalog.Column{
		{Name: "a", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		{Name: "b", DataType: "TEXT", ColumnType: "TEXT", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 2},
	}}, Constraints: []catalog.Constraint{held}}
	declared := schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "Item", Name: "items", PrimaryKey: []string{"a", "b"}}, Fields: fields}
	preparation := &schemapreparation.Capture{Prepared: []schemapreparation.Table{{Desired: declared, Current: current}}}
	forward = &difftypes.SchemaDiff{TablePreparation: preparation,
		ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY", Columns: []string{"a", "b"}}},
		ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY"}}}
	reverse = &difftypes.SchemaDiff{TablePreparation: cloneTablePreparation(preparation),
		ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY", Columns: []string{"b", "a"}}},
		ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY"}}}
	return forward, reverse, prior
}

// The reverse plan reads a table whose only change is its key as the forward
// plan leaves it, with the new key, and moves it to the key the database held,
// so the owner restores that key rather than find it already held.
func TestReverseTablePreparationProjectsTheForwardKeyChange(t *testing.T) {
	c := qt.New(t)
	forward, reverse, prior := keyedPreparation()

	err := reverseTablePreparation(t.Context(), forward, reverse, prior, "sqlite", builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	prepared := reverse.TablePreparation.Prepared[0]
	c.Assert(prepared.Current.Constraints, qt.HasLen, 1)
	c.Assert(prepared.Current.Constraints[0].ColumnNames, qt.DeepEquals, []string{"a", "b"})
	c.Assert(prepared.Desired.Table.PrimaryKey, qt.DeepEquals, []string{"b", "a"})
	c.Assert(forward.TablePreparation.Prepared[0].Current.Constraints[0].ColumnNames, qt.DeepEquals, []string{"b", "a"})
}
