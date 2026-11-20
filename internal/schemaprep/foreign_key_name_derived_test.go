package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

func orderTable() []schemamodel.Table {
	return []schemamodel.Table{{StructName: "Order", Name: "orders"}}
}

// A comparison pairs a key the server named with a declaration whose name Ptah
// chose, so the preparation says which names it chose.
func TestAssignDefaultForeignKeyNames_MarksTheNamesItDerives_HappyPath(t *testing.T) {
	t.Run("a field without a name", func(t *testing.T) {
		c := qt.New(t)
		database := &schemamodel.Database{
			Tables: orderTable(),
			Fields: []schemamodel.Field{{StructName: "Order", Name: "customer_id", Foreign: "customers(id)"}},
		}

		assigned := schemaprep.AssignDefaultForeignKeyNames(database, platform.Oracle)

		c.Assert(assigned.Fields[0].ForeignKeyName, qt.Equals, "fk_orders_customer_id")
		c.Assert(assigned.Fields[0].ForeignKeyNameDerived, qt.IsTrue)
		c.Assert(database.Fields[0].ForeignKeyNameDerived, qt.IsFalse)
	})
	t.Run("a table-level key without a name", func(t *testing.T) {
		c := qt.New(t)
		database := &schemamodel.Database{
			Tables: orderTable(),
			Constraints: []schemamodel.Constraint{{
				StructName: "Order", Type: "FOREIGN KEY", Table: "orders",
				Columns: []string{"x", "y"}, ForeignTable: "p", ForeignColumns: []string{"a", "b"},
			}},
		}

		assigned := schemaprep.AssignDefaultForeignKeyNames(database, platform.Oracle)

		c.Assert(assigned.Constraints[0].Name, qt.Equals, "fk_orders_x_y")
		c.Assert(assigned.Constraints[0].NameDerived, qt.IsTrue)
		c.Assert(database.Constraints[0].NameDerived, qt.IsFalse)
	})
	t.Run("a second pass keeps the mark", func(t *testing.T) {
		c := qt.New(t)
		database := &schemamodel.Database{
			Tables: orderTable(),
			Constraints: []schemamodel.Constraint{{
				StructName: "Order", Type: "FOREIGN KEY", Table: "orders",
				Columns: []string{"x"}, ForeignTable: "p", ForeignColumns: []string{"a"},
			}},
		}

		once := schemaprep.AssignDefaultForeignKeyNames(database, platform.Oracle)
		twice := schemaprep.AssignDefaultForeignKeyNames(once, platform.Oracle)

		c.Assert(twice.Constraints[0].NameDerived, qt.IsTrue)
	})
}

// A name the author wrote is part of the declaration and is never marked.
func TestAssignDefaultForeignKeyNames_LeavesAWrittenNameUnmarked_HappyPath(t *testing.T) {
	t.Run("a field", func(t *testing.T) {
		c := qt.New(t)
		database := &schemamodel.Database{
			Tables: orderTable(),
			Fields: []schemamodel.Field{{
				StructName: "Order", Name: "customer_id", Foreign: "customers(id)", ForeignKeyName: "fk_mine",
			}},
		}

		assigned := schemaprep.AssignDefaultForeignKeyNames(database, platform.Oracle)

		c.Assert(assigned.Fields[0].ForeignKeyName, qt.Equals, "fk_mine")
		c.Assert(assigned.Fields[0].ForeignKeyNameDerived, qt.IsFalse)
	})
	t.Run("a table-level key", func(t *testing.T) {
		c := qt.New(t)
		database := &schemamodel.Database{
			Tables: orderTable(),
			Constraints: []schemamodel.Constraint{{
				StructName: "Order", Name: "fk_mine", Type: "FOREIGN KEY", Table: "orders",
				Columns: []string{"x"}, ForeignTable: "p", ForeignColumns: []string{"a"},
			}},
		}

		assigned := schemaprep.AssignDefaultForeignKeyNames(database, platform.Oracle)

		c.Assert(assigned.Constraints[0].NameDerived, qt.IsFalse)
	})
}
