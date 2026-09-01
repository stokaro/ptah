package schemaprep_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaprep"
)

func TestAssignDefaultForeignKeyNamesDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "customer_id", Foreign: "customers(id)"}},
	}

	assigned := schemaprep.AssignDefaultForeignKeyNames(database, platform.Postgres)
	qt.Assert(t, assigned.Fields[0].ForeignKeyName, qt.Equals, "fk_orders_customer_id")
	qt.Assert(t, database.Fields[0].ForeignKeyName, qt.Equals, "")
	qt.Assert(t, schemaprep.AssignDefaultForeignKeyNames(assigned, platform.Postgres), qt.DeepEquals, assigned)
}

func TestAssignDefaultForeignKeyNamesDisambiguatesCollisions(t *testing.T) {
	t.Parallel()
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "owner_id", Foreign: "customers(id)"},
			{StructName: "Order", Name: "owner_id", Foreign: "accounts(id)"},
		},
	}

	assigned := schemaprep.AssignDefaultForeignKeyNames(database, platform.Postgres)
	qt.Assert(t, assigned.Fields[0].ForeignKeyName != assigned.Fields[1].ForeignKeyName, qt.IsTrue)
	qt.Assert(t, strings.HasPrefix(assigned.Fields[0].ForeignKeyName, "fk_orders_owner_id_"), qt.IsTrue)
	qt.Assert(t, strings.HasPrefix(assigned.Fields[1].ForeignKeyName, "fk_orders_owner_id_"), qt.IsTrue)
}
