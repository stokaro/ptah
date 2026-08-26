package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// compositeDeclaringForeignTable is a description whose child table declares a
// COMPOSITE foreign key naming the referenced table the given way.
//
// Composite is what makes the spelling reachable: a single-column key is carried
// on the field, as `parent(a)`, which is always unqualified. Only a multi-column
// key becomes a constraint whose ForeignTable the HCL parser writes qualified.
func compositeDeclaringForeignTable(foreignTable string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parent"},
			{StructName: "Child", Name: "child_multi"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "a", Type: "INTEGER"},
			{StructName: "Parent", Name: "b", Type: "INTEGER"},
			{StructName: "Child", Name: "pa", Type: "INTEGER"},
			{StructName: "Child", Name: "pb", Type: "INTEGER"},
		},
		Constraints: []goschema.Constraint{{
			StructName: "child_multi", Name: "fk_multi", Type: "FOREIGN KEY",
			Table:   "public.child_multi",
			Columns: []string{"pa", "pb"}, ForeignTable: foreignTable,
			ForeignColumn: "a", ForeignColumns: []string{"a", "b"},
			OnDelete: "NO ACTION", OnUpdate: "NO ACTION",
		}},
	}
}

// catalogReportingForeignTable is the same key as a reader reports it, with the
// referenced table's schema spelled the given way.
//
// The empty spelling is not hypothetical: a reader blanks the schema for the one
// it was scoped to, so this is what a PostgreSQL read of the default schema
// actually produces.
func catalogReportingForeignTable(foreignSchema string) *types.DBSchema {
	parent := "parent"
	return &types.DBSchema{
		Tables: []types.DBTable{
			{Name: "parent", Schema: "public", Type: "BASE TABLE", Columns: []types.DBColumn{
				{Name: "a", DataType: "integer", IsNullable: "NO"},
				{Name: "b", DataType: "integer", IsNullable: "NO"},
			}},
			{Name: "child_multi", Schema: "public", Type: "BASE TABLE", Columns: []types.DBColumn{
				{Name: "pa", DataType: "integer", IsNullable: "NO"},
				{Name: "pb", DataType: "integer", IsNullable: "NO"},
			}},
		},
		Constraints: []types.DBConstraint{{
			Schema: "public", TableName: "child_multi", Name: "fk_multi", Type: "FOREIGN KEY",
			ColumnName:  "pa",
			ColumnNames: []string{"pa", "pb"}, ForeignTable: &parent,
			ForeignSchema: foreignSchema, ForeignColumn: new("a"),
			ForeignColumns: []string{"a", "b"},
			DeleteRule:     noAction(), UpdateRule: noAction(),
		}},
	}
}

func noAction() *string {
	action := "NO ACTION"
	return &action
}

// qualificationDiff compares one description against one catalog.
func qualificationDiff(c *qt.C, generated *goschema.Database, database *types.DBSchema) *difftypes.SchemaDiff {
	c.Helper()
	diff := &difftypes.SchemaDiff{}
	compare.ConstraintsWithSemantics(generated, database, diff, nil, identifier.ForDialect("postgres"))
	return diff
}

// TestConstraints_AForeignTableIsTheSameTableHoweverEachSideSpellsIt pins that
// the two sides may qualify differently.
//
// They do, always: `schema inspect` writes the declaration qualified, and a
// reader blanks the schema for the one it was scoped to. Comparing the two as
// bare strings made a composite foreign key differ from ITSELF -- applying a
// description to the database it was read from planned a DROP and an ADD on
// every run, each taking a validating lock, and never reported the schema as
// synced (stokaro/ptah#2219).
func TestConstraints_AForeignTableIsTheSameTableHoweverEachSideSpellsIt(t *testing.T) {
	tests := []struct {
		name          string
		declared      string
		catalogSchema string
	}{
		// The reproduction: this is what ptah's own inspect and read produce.
		{name: "qualified declaration, blank catalog schema", declared: "public.parent", catalogSchema: ""},
		{name: "qualified on both sides", declared: "public.parent", catalogSchema: "public"},
		{name: "unqualified on both sides", declared: "parent", catalogSchema: ""},
		{name: "unqualified declaration, qualified catalog", declared: "parent", catalogSchema: "public"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := qualificationDiff(c,
				compositeDeclaringForeignTable(tt.declared),
				catalogReportingForeignTable(tt.catalogSchema))

			c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 0)
		})
	}
}

// TestConstraints_AForeignTableInAnotherSchemaIsAnotherTable is the control.
//
// Resolving an unqualified name to the default schema must not make every
// spelling equal. A declaration naming a schema that is NOT the default one
// names a different table, and the catalog's blank schema means "the one that
// was read" -- not "whichever the declaration meant".
func TestConstraints_AForeignTableInAnotherSchemaIsAnotherTable(t *testing.T) {
	tests := []struct {
		name          string
		declared      string
		catalogSchema string
	}{
		{name: "declared elsewhere, catalog in the default schema", declared: "other.parent", catalogSchema: ""},
		{name: "declared in the default schema, catalog elsewhere", declared: "public.parent", catalogSchema: "other"},
		{name: "a different table entirely", declared: "public.elsewhere", catalogSchema: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := qualificationDiff(c,
				compositeDeclaringForeignTable(tt.declared),
				catalogReportingForeignTable(tt.catalogSchema))

			c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 1)
		})
	}
}
