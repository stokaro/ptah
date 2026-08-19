package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestFinalize_RebuildsDerivedForeignKeyState(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "parent_id", Type: "INTEGER", Foreign: "users(id)"},
			{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Post", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
	}

	goschema.Finalize(database)
	goschema.Finalize(database)

	c.Assert(database.Dependencies["posts"], qt.DeepEquals, []string{"users"})
	c.Assert(database.SelfReferencingForeignKeys["users"], qt.DeepEquals, []goschema.SelfReferencingFK{{
		FieldName: "parent_id",
		Foreign:   "users(id)",
	}})

	database.Fields = []goschema.Field{
		{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
		{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
	}
	goschema.Finalize(database)

	c.Assert(database.Dependencies, qt.DeepEquals, map[string][]string{
		"posts": {},
		"users": {},
	})
	c.Assert(database.SelfReferencingForeignKeys, qt.DeepEquals, make(map[string][]goschema.SelfReferencingFK))
}

func TestFinalize_ExpandsEmbeddedRelationsIdempotently(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
		},
		EmbeddedFields: []goschema.EmbeddedField{{
			StructName:       "Post",
			EmbeddedTypeName: "User",
			Mode:             "relation",
			Field:            "user_id",
			Ref:              "users(id)",
		}},
	}

	goschema.Finalize(database)
	goschema.Finalize(database)

	c.Assert(database.Fields, qt.HasLen, 3)
	c.Assert(database.Dependencies["posts"], qt.DeepEquals, []string{"users"})
	c.Assert(database.Fields[2].Name, qt.Equals, "user_id")
	c.Assert(database.Fields[2].Foreign, qt.Equals, "users(id)")
}

func TestFinalize_ReplacesGeneratedEmbeddedFieldsAfterMutation(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Account", Name: "accounts"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Account", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
		},
		EmbeddedFields: []goschema.EmbeddedField{{
			StructName:       "Post",
			EmbeddedTypeName: "User",
			Mode:             "relation",
			Field:            "user_id",
			Ref:              "users(id)",
		}},
	}

	goschema.Finalize(database)
	database.EmbeddedFields = []goschema.EmbeddedField{{
		StructName:       "Post",
		EmbeddedTypeName: "Account",
		Mode:             "relation",
		Field:            "account_id",
		Ref:              "accounts(id)",
	}}
	goschema.Finalize(database)

	c.Assert(database.Fields, qt.HasLen, 4)
	c.Assert(database.Fields[3].Name, qt.Equals, "account_id")
	c.Assert(database.Fields[3].Foreign, qt.Equals, "accounts(id)")
	c.Assert(database.Dependencies["posts"], qt.DeepEquals, []string{"accounts"})
}

func TestFinalize_RecordsEmbeddedSelfReferenceOnce(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Node", Name: "nodes"}},
		Fields: []goschema.Field{{
			StructName: "Node",
			Name:       "id",
			Type:       "INTEGER",
			Primary:    true,
		}},
		EmbeddedFields: []goschema.EmbeddedField{{
			StructName:       "Node",
			EmbeddedTypeName: "Parent",
			Mode:             "relation",
			Field:            "parent_id",
			Ref:              "nodes(id)",
		}},
	}

	goschema.Finalize(database)

	c.Assert(database.SelfReferencingForeignKeys["nodes"], qt.DeepEquals, []goschema.SelfReferencingFK{{
		FieldName:      "parent_id",
		Foreign:        "nodes(id)",
		ForeignKeyName: "fk_node_parent_id",
	}})
}

func TestFinalize_ExpandsNestedEmbeddedModesAndStopsInlineCycles(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Tenant", Name: "tenants"},
			{StructName: "Order", Name: "orders"},
		},
		Fields: []goschema.Field{
			{StructName: "Tenant", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Ownership", Name: "label", Type: "TEXT"},
			{StructName: "Cycle", Name: "code", Type: "TEXT"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{StructName: "Order", EmbeddedTypeName: "Ownership", Mode: "inline", Prefix: "owner_"},
			{StructName: "Ownership", EmbeddedTypeName: "Tenant", Mode: "relation", Field: "tenant_id", Ref: "tenants(id)"},
			{StructName: "Ownership", EmbeddedTypeName: "Metadata", Mode: "json", Name: "metadata"},
			{StructName: "Ownership", EmbeddedTypeName: "Cycle", Mode: "inline"},
			{StructName: "Cycle", EmbeddedTypeName: "Ownership", Mode: "inline"},
		},
	}

	goschema.Finalize(database)

	c.Assert(database.Fields, qt.HasLen, 14)
	c.Assert(database.Fields[7].StructName, qt.Equals, "Order")
	c.Assert(database.Fields[7].Name, qt.Equals, "owner_label")
	c.Assert(database.Fields[8].Name, qt.Equals, "owner_tenant_id")
	c.Assert(database.Fields[8].Foreign, qt.Equals, "tenants(id)")
	c.Assert(database.Fields[8].ForeignKeyName, qt.Equals, "fk_order_owner_tenant_id")
	c.Assert(database.Fields[8].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mysql":   {"type": "INT"},
		"mariadb": {"type": "INT"},
	})
	c.Assert(database.Fields[9].Name, qt.Equals, "owner_metadata")
	c.Assert(database.Fields[9].Type, qt.Equals, "JSONB")
	c.Assert(database.Fields[10].Name, qt.Equals, "owner_code")
	c.Assert(database.Dependencies["orders"], qt.DeepEquals, []string{"tenants"})
}

func TestFinalize_PreservesUnnamedTableConstraints(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Node", Name: "nodes"}},
		Constraints: []goschema.Constraint{
			{
				StructName:     "Node",
				Type:           "FOREIGN KEY",
				Columns:        []string{"tenant_id", "parent_id"},
				ForeignTable:   "nodes",
				ForeignColumns: []string{"tenant_id", "id"},
			},
			{
				StructName:      "Node",
				Type:            "CHECK",
				CheckExpression: "tenant_id > 0",
			},
		},
	}

	goschema.Finalize(database)

	c.Assert(database.Constraints, qt.HasLen, 2)
	c.Assert(database.SelfReferencingForeignKeys, qt.DeepEquals, make(map[string][]goschema.SelfReferencingFK))
}

func TestFinalize_PreservesDistinctUnnamedForeignKeys(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "FirstParent", Name: "first_parents"},
			{StructName: "SecondParent", Name: "second_parents"},
			{StructName: "Child", Name: "children"},
		},
		Constraints: []goschema.Constraint{
			{StructName: "Child", Type: "FOREIGN KEY", Columns: []string{"parent_id"}, ForeignTable: "first_parents", ForeignColumns: []string{"id"}},
			{StructName: "Child", Type: "FOREIGN KEY", Columns: []string{"parent_id"}, ForeignTable: "second_parents", ForeignColumns: []string{"id"}},
		},
	}

	goschema.Finalize(database)

	c.Assert(database.Constraints, qt.HasLen, 2)
	c.Assert(database.Dependencies["children"], qt.DeepEquals, []string{"first_parents", "second_parents"})
}

func TestFinalize_DeduplicatesEquivalentNormalizedUnnamedForeignKeys(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Child", Schema: "app", Name: "children"},
			{StructName: "Parent", Schema: "app", Name: "parents"},
		},
		Constraints: []goschema.Constraint{
			{
				StructName:     "Child",
				Type:           "FOREIGN KEY",
				Columns:        []string{"parent_id"},
				ForeignTable:   "parents",
				ForeignColumns: []string{"id"},
			},
			{
				StructName:     "Child",
				Table:          "app.children",
				Type:           "FOREIGN KEY",
				Columns:        []string{"parent_id"},
				ForeignTable:   "app.parents",
				ForeignColumns: []string{"id"},
			},
		},
	}

	goschema.Finalize(database)

	c.Assert(database.Constraints, qt.DeepEquals, []goschema.Constraint{{
		StructName:     "Child",
		Table:          "app.children",
		Type:           "FOREIGN KEY",
		Columns:        []string{"parent_id"},
		ForeignTable:   "app.parents",
		ForeignColumns: []string{"id"},
	}})
	c.Assert(database.Dependencies["app.children"], qt.DeepEquals, []string{"app.parents"})
}

func TestFinalize_OrdersStronglyConnectedComponentsDeterministically(t *testing.T) {
	tests := []struct {
		name   string
		tables []goschema.Table
	}{
		{
			name: "dependent declared before cycle",
			tables: []goschema.Table{
				{StructName: "Leaf", Name: "leaf"},
				{StructName: "CycleB", Name: "cycle_b"},
				{StructName: "CycleA", Name: "cycle_a"},
				{StructName: "Root", Name: "root"},
			},
		},
		{
			name: "cycle members and acyclic nodes permuted",
			tables: []goschema.Table{
				{StructName: "CycleA", Name: "cycle_a"},
				{StructName: "Root", Name: "root"},
				{StructName: "Leaf", Name: "leaf"},
				{StructName: "CycleB", Name: "cycle_b"},
			},
		},
	}
	expected := []goschema.Table{
		{StructName: "Root", Name: "root"},
		{StructName: "CycleA", Name: "cycle_a"},
		{StructName: "CycleB", Name: "cycle_b"},
		{StructName: "Leaf", Name: "leaf"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &goschema.Database{
				Tables: test.tables,
				Fields: []goschema.Field{
					{StructName: "Root", Name: "id", Type: "INTEGER", Primary: true},
					{StructName: "CycleA", Name: "id", Type: "INTEGER", Primary: true},
					{StructName: "CycleA", Name: "root_id", Type: "INTEGER", Foreign: "root(id)"},
					{StructName: "CycleA", Name: "cycle_b_id", Type: "INTEGER", Foreign: "cycle_b(id)"},
					{StructName: "CycleB", Name: "id", Type: "INTEGER", Primary: true},
					{StructName: "CycleB", Name: "cycle_a_id", Type: "INTEGER", Foreign: "cycle_a(id)"},
					{StructName: "Leaf", Name: "id", Type: "INTEGER", Primary: true},
					{StructName: "Leaf", Name: "cycle_a_id", Type: "INTEGER", Foreign: "cycle_a(id)"},
				},
			}

			goschema.Finalize(database)

			c.Assert(database.Tables, qt.DeepEquals, expected)
		})
	}
}
