package generator

// White-box testing required: the down migration comes from
// generateDownMigrationSQL, which the exported generator reaches only through a
// migration directory and a live database. This pins the reversed addition
// that restores the prior key, list included.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// A migration that widens ON DELETE SET NULL (parent_id) to every column rolls
// back to the narrowed key. Rebuilt without its list, the down migration would
// leave the wider key in place while claiming to restore the old one.
func TestGenerateDownMigration_RestoresTheDeleteColumnList(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents", PrimaryKey: []string{"tenant", "id"}},
			{StructName: "Child", Name: "children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "tenant", Type: "INTEGER"},
			{StructName: "Parent", Name: "id", Type: "INTEGER"},
			{StructName: "Child", Name: "tenant", Type: "INTEGER", Nullable: true},
			{StructName: "Child", Name: "parent_id", Type: "INTEGER", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Table: "children", Name: "children_parent_fk", Type: "FOREIGN KEY",
			Columns: []string{"tenant", "parent_id"}, ForeignTable: "parents",
			ForeignColumns: []string{"tenant", "id"}, OnDelete: "SET NULL",
		}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parents", Columns: []catalog.Column{
				{Name: "tenant", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "children", Columns: []catalog.Column{
				{Name: "tenant", DataType: "integer", IsNullable: "YES"},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{{
			Name: "children_parent_fk", TableName: "children", Type: "FOREIGN KEY",
			ColumnNames: []string{"tenant", "parent_id"}, ForeignTable: new("parents"),
			ForeignColumns: []string{"tenant", "id"},
			DeleteRule:     new("SET NULL"), UpdateRule: new("NO ACTION"),
			OnDeleteColumns: []string{"parent_id"},
		}},
	}
	upDiff := schemadiff.CompareWithDialect(desired, current, "postgres")

	downSQL, err := generateDownMigrationSQL(upDiff, desired, current, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(legacyRenderedSQL(downSQL), qt.Contains, "ON DELETE SET NULL (parent_id)")
}
