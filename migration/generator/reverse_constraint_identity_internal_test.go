package generator

// White-box testing required: reverseSchemaDiffWithSchemaForDialect is the
// function that rebuilds these records, and the invariant is about what it
// produces rather than about any statement rendered from it. Reaching it
// through an exported entry point would test the planner too, and a planner
// that happened to tolerate a zero identity would hide the defect.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestReverseSchemaDiff_EveryConstraintRecordCarriesAnIdentity is the invariant
// a consumer keying on identity depends on.
//
// The comparator fills the identity on the forward diff, but a down migration is
// not a comparison: the generator turns the diff around and rebuilds most of
// these records, restoring bodies from the introspected schema and regenerating
// names the description never wrote. A record it builds without an identity is
// worse than one with no identity field at all -- the zero value is a single
// key, so every such constraint would pair with every other, and the down
// migration would drop one and call it all of them (stokaro/ptah#1663).
func TestReverseSchemaDiff_EveryConstraintRecordCarriesAnIdentity(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("postgres")
	forward := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		ConstraintsAdded:    []string{"uq_widget_scope"},
		ConstraintsRemoved:  []string{"uq_widget_scope"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
			Name: "uq_widget_scope", TableName: "widget", Type: "UNIQUE",
			Columns:  []string{"tenant"},
			Identity: difftypes.ConstraintIdentity{Schema: "public", Table: "widget", Name: "uq_widget_scope"},
		}},
		ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{{
			Name: "uq_widget_scope", TableName: "public.widget", Type: "UNIQUE",
			Identity: difftypes.ConstraintIdentity{Schema: "public", Table: "widget", Name: "uq_widget_scope"},
		}},
	}

	reversed := reverseSchemaDiffWithSchemaForDialect(
		forward, widgetSchemaForReversal(), widgetCatalogForReversal(), "postgres")

	c.Assert(reversed.ConstraintsAddedWithTables, qt.Not(qt.HasLen), 0)
	c.Assert(reversed.ConstraintsRemovedWithTables, qt.Not(qt.HasLen), 0)
	for _, info := range reversed.ConstraintsAddedWithTables {
		c.Assert(info.Identity, qt.Not(qt.Equals), difftypes.ConstraintIdentity{},
			qt.Commentf("addition %q on %q carries no identity", info.Name, info.TableName))
	}
	for _, info := range reversed.ConstraintsRemovedWithTables {
		c.Assert(info.Identity, qt.Not(qt.Equals), difftypes.ConstraintIdentity{},
			qt.Commentf("removal %q on %q carries no identity", info.Name, info.TableName))
	}
}

func widgetSchemaForReversal() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: "widget"}},
		Fields: []schemamodel.Field{
			{StructName: "Widget", Name: "id", Type: "int", Primary: true},
			{StructName: "Widget", Name: "tenant", Type: "text"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Widget", Name: "uq_widget_scope", Table: "widget",
			Type: "UNIQUE", Columns: []string{"tenant"},
		}},
	}
}

func widgetCatalogForReversal() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: "public", Name: "widget", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsPrimaryKey: true, IsNullable: "NO"},
			{Name: "tenant", DataType: "text", IsNullable: "NO"},
		}}},
		Constraints: []catalog.Constraint{{
			Schema: "public", TableName: "widget", Name: "uq_widget_scope",
			Type: "UNIQUE", ColumnNames: []string{"tenant", "code"},
		}},
	}
}
