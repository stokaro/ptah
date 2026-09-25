package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// Two unnamed keys over the same columns that differ only in their ON DELETE
// column list are two keys, so each gets a name of its own rather than both
// being allocated the one name.
func TestAssignDefaultForeignKeyNames_KeysDifferingInTheDeleteColumnList(t *testing.T) {
	c := qt.New(t)
	key := func(listed ...string) schemamodel.Constraint {
		return schemamodel.Constraint{
			StructName: "Child", Table: "children", Type: "FOREIGN KEY",
			Columns: []string{"tenant", "parent_id"}, ForeignTable: "parents",
			ForeignColumns: []string{"tenant", "id"}, OnDelete: "SET NULL", OnDeleteColumns: listed,
		}
	}
	database := &schemamodel.Database{
		Tables:      []schemamodel.Table{{StructName: "Child", Name: "children"}, {StructName: "Parent", Name: "parents"}},
		Constraints: []schemamodel.Constraint{key("parent_id"), key("tenant")},
	}

	named := schemaprep.AssignDefaultForeignKeyNames(database, "postgres")

	c.Assert(named.Constraints, qt.HasLen, 2)
	c.Assert(named.Constraints[0].Name, qt.Not(qt.Equals), "")
	c.Assert(named.Constraints[0].Name, qt.Not(qt.Equals), named.Constraints[1].Name)
}
