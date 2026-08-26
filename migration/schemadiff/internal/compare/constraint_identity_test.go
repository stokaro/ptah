package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestConstraints_AModificationCarriesOneIdentityUnderTwoSpellings is what the
// identity is for.
//
// A modified constraint is expressed as a removal and an addition of the same
// object, and the two arrive spelled differently: the addition carries the
// description's `widget`, the removal the catalog's `public.widget`. Every
// consumer that had to pair them folded the names again to do it -- applying the
// target's rule a second time, on one side of the pipeline only, which is how a
// drop came to be paired with a constraint the comparator never removed
// (stokaro/ptah#1987).
//
// The comparator already knew they were one object: it found them under one key.
// This asserts it now says so.
func TestConstraints_AModificationCarriesOneIdentityUnderTwoSpellings(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("postgres")
	diff := &difftypes.SchemaDiff{}

	compare.ConstraintsWithSemantics(
		widgetDeclaringScopeOn("tenant"),
		widgetCatalogHoldingScopeOn("tenant", "code"),
		diff, nil, semantics,
	)

	c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 1)
	c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 1)
	added, removed := diff.ConstraintsAddedWithTables[0], diff.ConstraintsRemovedWithTables[0]

	// The premise: the two records really do spell the table differently.
	c.Assert(added.TableName, qt.Equals, "widget")
	c.Assert(removed.TableName, qt.Equals, "public.widget")

	// The fact: they are one object, and the record says so without anyone
	// having to fold the spellings again.
	c.Assert(added.Identity, qt.Equals, removed.Identity)
	c.Assert(added.Identity, qt.Equals, difftypes.ConstraintIdentity{
		Schema: "public", Table: "widget", Name: "uq_widget_scope",
	})
}

// widgetDeclaringScopeOn is the description, which leaves the table unqualified
// the way a description does.
func widgetDeclaringScopeOn(columns ...string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: "widget"}},
		Fields: []schemamodel.Field{
			{StructName: "Widget", Name: "id", Type: "int", Primary: true},
			{StructName: "Widget", Name: "tenant", Type: "text"},
			{StructName: "Widget", Name: "code", Type: "text"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Widget", Name: "uq_widget_scope", Table: "widget",
			Type: "UNIQUE", Columns: columns,
		}},
	}
}

// widgetCatalogHoldingScopeOn is the read, which reports the table with its
// schema the way a catalog does, and the constraint over other columns so the
// pair is a modification rather than nothing.
func widgetCatalogHoldingScopeOn(columns ...string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: "public", Name: "widget", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsPrimaryKey: true, IsNullable: "NO"},
			{Name: "tenant", DataType: "text", IsNullable: "NO"},
			{Name: "code", DataType: "text", IsNullable: "NO"},
		}}},
		Constraints: []catalog.Constraint{{
			Schema: "public", TableName: "widget", Name: "uq_widget_scope",
			Type: "UNIQUE", ColumnNames: columns,
		}},
	}
}
