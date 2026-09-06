package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// multiColumnDeferrableDeclaration is a description whose child table declares a
// COMPOSITE foreign key with the given deferral.
//
// Composite on purpose: a multi-column key is carried as a constraint in the
// description rather than on a field, so it reaches the addition record without
// passing through the field-level synthesis. Both arities have to arrive at the
// planner carrying the deferral, and only one of them goes through synthesis.
func multiColumnDeferrableDeclaration(deferrable bool, initially string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parent"},
			{StructName: "Child", Name: "child"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "a", Type: "INTEGER"},
			{StructName: "Parent", Name: "b", Type: "INTEGER"},
			{StructName: "Child", Name: "pa", Type: "INTEGER"},
			{StructName: "Child", Name: "pb", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Name: "fk_multi", Type: "FOREIGN KEY", Table: "child",
			Columns: []string{"pa", "pb"}, ForeignTable: "parent",
			ForeignColumn: "a", ForeignColumns: []string{"a", "b"},
			Deferrable: deferrable, Initially: initially,
		}},
	}
}

// emptyChildCatalog is the same two tables with no foreign key at all, so the
// declared one is recorded as an addition.
func emptyChildCatalog() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parent", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "a", DataType: "integer", IsNullable: "NO"},
				{Name: "b", DataType: "integer", IsNullable: "NO"},
			}},
			{Name: "child", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "pa", DataType: "integer", IsNullable: "NO"},
				{Name: "pb", DataType: "integer", IsNullable: "NO"},
			}},
		},
	}
}

// TestConstraints_AnAddedForeignKeyCarriesItsDeferral pins that the property
// survives the record the planner reads.
//
// The comparator compared the deferral and then dropped it on the way out:
// ConstraintAdditionInfo had nowhere to put it, so a difference the comparator
// had just detected reached the planner with the property already gone, and the
// ALTER that was meant to apply it built a plain key (stokaro/ptah#2216).
func TestConstraints_AnAddedForeignKeyCarriesItsDeferral(t *testing.T) {
	tests := []struct {
		name       string
		deferrable bool
		initially  string
	}{
		{name: "deferred", deferrable: true, initially: "DEFERRED"},
		{name: "immediate", deferrable: true, initially: "IMMEDIATE"},
		{name: "deferrable with no timing", deferrable: true, initially: ""},
		// The control: nothing declared, nothing carried.
		{name: "not deferrable", deferrable: false, initially: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.ConstraintsWithSemantics(
				multiColumnDeferrableDeclaration(tt.deferrable, tt.initially),
				emptyChildCatalog(), diff, nil, identifier.ForDialect("postgres"))

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
			added := diff.ConstraintsAdded[0]
			c.Assert(added.Type, qt.Equals, "FOREIGN KEY")
			c.Assert(added.Deferrable, qt.Equals, tt.deferrable)
			c.Assert(added.Initially, qt.Equals, tt.initially)
		})
	}
}
