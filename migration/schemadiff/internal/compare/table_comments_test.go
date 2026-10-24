package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// commentedDeclaration declares a table and a column with a comment each.
func commentedDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users", Comment: "People who log in"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "email", Type: "TEXT", Nullable: true, Comment: "Where mail goes"}},
	}
}

// uncommentedCatalog holds the same table with no comment, as a target that
// stores none reports it.
func uncommentedCatalog() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{
		{Name: "users", Columns: []catalog.Column{{Name: "email", DataType: "TEXT", IsNullable: "YES"}}},
	}}
}

// TestTablesAndColumns_ComparesCommentsWhereTheTargetStoresThem is the control
// for the test below: on PostgreSQL a declared table and column comment the
// database does not hold is a change the plan makes.
func TestTablesAndColumns_ComparesCommentsWhereTheTargetStoresThem(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.TablesAndColumnsWithDialect(commentedDeclaration(), uncommentedCatalog(), diff, "postgres")

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].CommentChange, qt.DeepEquals, &difftypes.CommentChange{Desired: "People who log in"})
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].CommentChange, qt.DeepEquals, &difftypes.CommentChange{Desired: "Where mail goes"})
}

// TestTablesAndColumns_LeavesCommentsATargetCannotStore pins that SQLite,
// which stores no comment on a table or a column, compares none: its catalog
// reports no comment whatever was declared, so a declared one would be a
// difference on every run that no plan can close, and the drift report would
// never find the database in sync.
func TestTablesAndColumns_LeavesCommentsATargetCannotStore(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.TablesAndColumnsWithDialect(commentedDeclaration(), uncommentedCatalog(), diff, "sqlite")

	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(diff.Matched, qt.Equals, difftypes.MatchedCounts{Tables: 1, Columns: 1})
}
