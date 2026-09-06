package schemaprep_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

func TestAssignDefaultForeignKeyNamesDoesNotMutateInput(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "customer_id", Foreign: "customers(id)"}},
	}

	assigned := schemaprep.AssignDefaultForeignKeyNames(database, platform.Postgres)
	c.Assert(assigned.Fields[0].ForeignKeyName, qt.Equals, "fk_orders_customer_id")
	c.Assert(database.Fields[0].ForeignKeyName, qt.Equals, "")
	c.Assert(schemaprep.AssignDefaultForeignKeyNames(assigned, platform.Postgres), qt.DeepEquals, assigned)
}

func TestAssignDefaultForeignKeyNamesDisambiguatesCollisions(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "owner_id", Foreign: "customers(id)"},
			{StructName: "Order", Name: "owner_id", Foreign: "accounts(id)"},
		},
	}

	assigned := schemaprep.AssignDefaultForeignKeyNames(database, platform.Postgres)
	c.Assert(assigned.Fields[0].ForeignKeyName, qt.Not(qt.Equals), assigned.Fields[1].ForeignKeyName)
	c.Assert(strings.HasPrefix(assigned.Fields[0].ForeignKeyName, "fk_orders_owner_id_"), qt.IsTrue)
	c.Assert(strings.HasPrefix(assigned.Fields[1].ForeignKeyName, "fk_orders_owner_id_"), qt.IsTrue)
}

func TestAssignDefaultForeignKeyNamesPreservesFinalizedInlineField(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "BIGINT"},
			{
				StructName:            "User",
				Name:                  "organization_id",
				Type:                  "BIGINT",
				Foreign:               "organizations(id)",
				GeneratedFromEmbedded: true,
			},
		},
		EmbeddedFields: []schemamodel.EmbeddedField{{
			StructName:       "User",
			EmbeddedTypeName: "composite-source-0\x00Metadata",
			Mode:             "inline",
		}},
	}

	got := schemaprep.AssignDefaultForeignKeyNames(database, platform.Postgres)

	c.Assert(got.Fields, qt.HasLen, 2)
	c.Assert(got.Fields[1].Name, qt.Equals, "organization_id")
	c.Assert(got.Fields[1].ForeignKeyName, qt.Equals, "fk_users_organization_id")
}
