package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
)

func TestConvertDBSchemaToGoSchema_Extensions(t *testing.T) {
	tests := []struct {
		name     string
		dbSchema *types.DBSchema
		expected []goschema.Extension
	}{
		{
			name: "single extension without comment",
			dbSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{
						Name:    "pg_trgm",
						Version: "1.6",
						Schema:  "public",
					},
				},
			},
			expected: []goschema.Extension{
				{
					Name:        "pg_trgm",
					IfNotExists: true,
					Version:     "1.6",
					Comment:     "",
				},
			},
		},
		{
			name: "single extension with comment",
			dbSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{
						Name:    "postgis",
						Version: "3.0",
						Schema:  "public",
						Comment: new("Geographic data support"),
					},
				},
			},
			expected: []goschema.Extension{
				{
					Name:        "postgis",
					IfNotExists: true,
					Version:     "3.0",
					Comment:     "Geographic data support",
				},
			},
		},
		{
			name: "multiple extensions",
			dbSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{
						Name:    "pg_trgm",
						Version: "1.6",
						Schema:  "public",
					},
					{
						Name:    "btree_gin",
						Version: "1.3",
						Schema:  "public",
						Comment: new("Enable GIN indexes on btree types"),
					},
				},
			},
			expected: []goschema.Extension{
				{
					Name:        "pg_trgm",
					IfNotExists: true,
					Version:     "1.6",
					Comment:     "",
				},
				{
					Name:        "btree_gin",
					IfNotExists: true,
					Version:     "1.3",
					Comment:     "Enable GIN indexes on btree types",
				},
			},
		},
		{
			name: "no extensions",
			dbSchema: &types.DBSchema{
				Extensions: []types.DBExtension{},
			},
			expected: []goschema.Extension{},
		},
		{
			name: "nil extensions",
			dbSchema: &types.DBSchema{
				Extensions: nil,
			},
			expected: []goschema.Extension{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := dbschematogo.ConvertDBSchemaToGoSchema(tt.dbSchema)

			c.Assert(result.Extensions, qt.HasLen, len(tt.expected))
			for i, expectedExt := range tt.expected {
				actualExt := result.Extensions[i]
				c.Assert(actualExt.Name, qt.Equals, expectedExt.Name)
				c.Assert(actualExt.IfNotExists, qt.Equals, expectedExt.IfNotExists)
				c.Assert(actualExt.Version, qt.Equals, expectedExt.Version)
				c.Assert(actualExt.Comment, qt.Equals, expectedExt.Comment)
			}
		})
	}
}

func TestConvertDBSchemaToGoSchema_ExtensionsWithOtherElements(t *testing.T) {
	c := qt.New(t)

	// Test that extensions are properly converted alongside other schema elements
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "users",
				Columns: []types.DBColumn{
					{
						Name:     "id",
						DataType: "integer",
					},
				},
			},
		},
		Extensions: []types.DBExtension{
			{
				Name:    "pg_trgm",
				Version: "1.6",
				Schema:  "public",
			},
		},
		Enums: []types.DBEnum{
			{
				Name:   "status_type",
				Values: []string{"active", "inactive"},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	// Verify extensions are converted
	c.Assert(result.Extensions, qt.HasLen, 1)
	c.Assert(result.Extensions[0].Name, qt.Equals, "pg_trgm")
	c.Assert(result.Extensions[0].IfNotExists, qt.Equals, true)
	c.Assert(result.Extensions[0].Version, qt.Equals, "1.6")

	// Verify other elements are also converted
	c.Assert(result.Tables, qt.HasLen, 1)
	c.Assert(result.Tables[0].Name, qt.Equals, "users")
	c.Assert(result.Enums, qt.HasLen, 1)
	c.Assert(result.Enums[0].Name, qt.Equals, "status_type")
}

func TestConvertDBSchemaToGoSchema_GeneratedColumns(t *testing.T) {
	c := qt.New(t)
	expression := "lower(name)"
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "users",
				Columns: []types.DBColumn{
					{
						Name:                "slug",
						DataType:            "text",
						GeneratedExpression: &expression,
						GeneratedKind:       "STORED",
					},
				},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Fields, qt.HasLen, 1)
	c.Assert(result.Fields[0].GeneratedExpression, qt.Equals, "lower(name)")
	c.Assert(result.Fields[0].GeneratedKind, qt.Equals, "STORED")
}

func TestConvertDBSchemaToGoSchema_PostgresUserDefinedColumnUsesUDTName(t *testing.T) {
	c := qt.New(t)
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "products",
				Columns: []types.DBColumn{
					{
						Name:     "status",
						DataType: "USER-DEFINED",
						UDTName:  "enum_product_status",
					},
				},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Fields, qt.HasLen, 1)
	c.Assert(result.Fields[0].Type, qt.Equals, "enum_product_status")
}

func TestConvertDBSchemaToGoSchema_SchemaQualifiedObjectOwnersUseTableStructName(t *testing.T) {
	c := qt.New(t)
	checkClause := "tenant_id > 0"
	notNullCheck := "id IS NOT NULL"
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Schema: "tenant_a",
				Name:   "orders",
				Columns: []types.DBColumn{
					{Name: "id", DataType: "integer"},
				},
			},
		},
		Indexes: []types.DBIndex{
			{
				Schema:    "tenant_a",
				TableName: "orders",
				Name:      "idx_orders_id",
				Columns:   []string{"id"},
			},
			{
				Schema:    "tenant_a",
				TableName: "orders",
				Name:      "orders_id_unique",
				Columns:   []string{"id"},
				IsUnique:  true,
			},
		},
		Constraints: []types.DBConstraint{
			{
				Schema:      "tenant_a",
				TableName:   "orders",
				Name:        "orders_tenant_check",
				Type:        "CHECK",
				CheckClause: &checkClause,
			},
			{
				Schema:      "tenant_a",
				TableName:   "orders",
				Name:        "orders_id_not_null",
				Type:        "CHECK",
				CheckClause: &notNullCheck,
			},
			{
				Schema:    "tenant_a",
				TableName: "orders",
				Name:      "orders_id_unique",
				Type:      "UNIQUE",
				ColumnNames: []string{
					"id",
				},
			},
		},
		RLSPolicies: []types.DBRLSPolicy{
			{
				Name:            "orders_tenant_policy",
				Table:           "tenant_a.orders",
				PolicyFor:       "ALL",
				ToRoles:         "PUBLIC",
				UsingExpression: "tenant_id > 0",
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Tables, qt.HasLen, 1)
	c.Assert(result.Tables[0].StructName, qt.Equals, "Orders")
	c.Assert(result.Indexes, qt.HasLen, 1)
	c.Assert(result.Indexes[0].StructName, qt.Equals, "Orders")
	c.Assert(result.Indexes[0].Name, qt.Equals, "idx_orders_id")
	c.Assert(result.Constraints, qt.HasLen, 1)
	c.Assert(result.Constraints[0].StructName, qt.Equals, "Orders")
	c.Assert(result.Constraints[0].Name, qt.Equals, "orders_tenant_check")
	c.Assert(result.RLSPolicies, qt.HasLen, 1)
	c.Assert(result.RLSPolicies[0].StructName, qt.Equals, "Orders")
}

func TestConvertDBSchemaToGoSchema_DBDefaultExpression(t *testing.T) {
	c := qt.New(t)
	statusDefault := "'draft'::enum_product_status"
	nameDefault := "'unnamed'"
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "products",
				Columns: []types.DBColumn{
					{
						Name:          "status",
						DataType:      "USER-DEFINED",
						UDTName:       "enum_product_status",
						ColumnDefault: &statusDefault,
					},
					{
						Name:          "name",
						DataType:      "text",
						ColumnDefault: &nameDefault,
					},
				},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Fields, qt.HasLen, 2)
	c.Assert(result.Fields[0].Default, qt.Equals, "")
	c.Assert(result.Fields[0].DefaultExpr, qt.Equals, "'draft'::enum_product_status")
	c.Assert(result.Fields[1].Default, qt.Equals, "'unnamed'")
	c.Assert(result.Fields[1].DefaultExpr, qt.Equals, "")
}

func TestConvertDBSchemaToGoSchema_CompositeForeignKeyBecomesTableConstraint(t *testing.T) {
	c := qt.New(t)
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "orders",
				Columns: []types.DBColumn{
					{Name: "tenant_id", DataType: "integer"},
					{Name: "owner_id", DataType: "integer"},
				},
			},
		},
		Constraints: []types.DBConstraint{
			{
				Name:           "fk_orders_accounts",
				TableName:      "orders",
				Type:           "FOREIGN KEY",
				ColumnName:     "tenant_id",
				ColumnNames:    []string{"tenant_id", "owner_id"},
				ForeignTable:   new("accounts"),
				ForeignColumn:  new("tenant_id"),
				ForeignColumns: []string{"tenant_id", "id"},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Fields, qt.HasLen, 2)
	for _, field := range result.Fields {
		c.Assert(field.Foreign, qt.Equals, "")
		c.Assert(field.ForeignKeyName, qt.Equals, "")
	}
	c.Assert(result.Constraints, qt.DeepEquals, []goschema.Constraint{{
		StructName:     "Orders",
		Name:           "fk_orders_accounts",
		Type:           "FOREIGN KEY",
		Table:          "orders",
		Columns:        []string{"tenant_id", "owner_id"},
		ForeignTable:   "accounts",
		ForeignColumn:  "tenant_id",
		ForeignColumns: []string{"tenant_id", "id"},
	}})
}

func TestConvertDBSchemaToGoSchema_TableLevelConstraintsAndSizedTypes(t *testing.T) {
	c := qt.New(t)
	varcharLen := 255
	precision := 10
	scale := 2
	checkClause := "price > 0"
	nullsDistinct := false
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "order_items",
			Columns: []types.DBColumn{
				{Name: "tenant_id", DataType: "integer", IsPrimaryKey: true},
				{Name: "order_id", DataType: "integer", IsPrimaryKey: true},
				{Name: "sku", DataType: "character varying", CharacterMaxLength: &varcharLen},
				{Name: "price", DataType: "numeric", NumericPrecision: &precision, NumericScale: &scale},
			},
		}},
		Constraints: []types.DBConstraint{
			{
				Name:        "order_items_pkey",
				TableName:   "order_items",
				Type:        "PRIMARY KEY",
				ColumnNames: []string{"tenant_id", "order_id"},
			},
			{
				Name:           "order_items_sku_unique",
				TableName:      "order_items",
				Type:           "UNIQUE",
				ColumnNames:    []string{"tenant_id", "sku"},
				IncludeColumns: []string{"created_at"},
				NullsDistinct:  &nullsDistinct,
			},
			{
				Name:        "order_items_price_check",
				TableName:   "order_items",
				Type:        "CHECK",
				CheckClause: &checkClause,
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Tables, qt.HasLen, 1)
	c.Assert(result.Tables[0].PrimaryKey, qt.DeepEquals, []string{"tenant_id", "order_id"})
	c.Assert(result.Fields[0].Primary, qt.IsFalse)
	c.Assert(result.Fields[1].Primary, qt.IsFalse)
	c.Assert(result.Fields[2].Type, qt.Equals, "VARCHAR(255)")
	c.Assert(result.Fields[3].Type, qt.Equals, "NUMERIC(10,2)")
	c.Assert(result.Constraints, qt.DeepEquals, []goschema.Constraint{
		{
			StructName:     "OrderItems",
			Name:           "order_items_sku_unique",
			Type:           "UNIQUE",
			Table:          "order_items",
			Columns:        []string{"tenant_id", "sku"},
			IncludeColumns: []string{"created_at"},
			NullsDistinct:  &nullsDistinct,
		},
		{
			StructName:      "OrderItems",
			Name:            "order_items_price_check",
			Type:            "CHECK",
			Table:           "order_items",
			CheckExpression: "price > 0",
		},
	})
}

func TestConvertDBSchemaToGoSchema_ColumnCharsetCollate(t *testing.T) {
	c := qt.New(t)
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "users",
				Columns: []types.DBColumn{
					{
						Name:     "name",
						DataType: "varchar(255)",
						Charset:  "hebrew",
						Collate:  "hebrew_general_ci",
					},
				},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Fields, qt.HasLen, 1)
	c.Assert(result.Fields[0].Charset, qt.Equals, "hebrew")
	c.Assert(result.Fields[0].Collate, qt.Equals, "hebrew_general_ci")
}

func TestConvertDBSchemaToGoSchema_ExtensionDefaultValues(t *testing.T) {
	c := qt.New(t)

	// Test that extensions get proper default values
	dbSchema := &types.DBSchema{
		Extensions: []types.DBExtension{
			{
				Name:    "test_extension",
				Version: "1.0",
				Schema:  "public",
				// Comment is nil
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Extensions, qt.HasLen, 1)
	ext := result.Extensions[0]

	// Verify default values
	c.Assert(ext.Name, qt.Equals, "test_extension")
	c.Assert(ext.IfNotExists, qt.Equals, true) // Should default to true for safety
	c.Assert(ext.Version, qt.Equals, "1.0")
	c.Assert(ext.Comment, qt.Equals, "") // Should be empty string when nil
}
