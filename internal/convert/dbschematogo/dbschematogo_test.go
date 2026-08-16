package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
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
					Schema:      "public",
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
					Schema:      "public",
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
					Schema:      "public",
					IfNotExists: true,
					Version:     "1.6",
					Comment:     "",
				},
				{
					Name:        "btree_gin",
					Schema:      "public",
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
				c.Assert(actualExt.Schema, qt.Equals, expectedExt.Schema)
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

func TestConvertDBSchemaToGoSchema_Schemas(t *testing.T) {
	c := qt.New(t)
	dbSchema := &types.DBSchema{
		Schemas: []types.DBSchemaInfo{
			{Name: "auth", Comment: "Authentication objects"},
			{Name: "billing", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Schemas, qt.DeepEquals, []goschema.Schema{
		{Name: "auth", Comment: "Authentication objects"},
		{Name: "billing", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
	})
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

// A PostgreSQL array column reaches the converter as the bare category "ARRAY"
// -- information_schema carries no element type for one, and no length either
// -- so the reader asks the server directly and the converter has to prefer
// that answer (stokaro/ptah#1138).
//
// The rows are the three ways this can go wrong, and the last one is why the
// obvious narrow fix is not enough: reconstructing the type from UDTName yields
// "varchar[]" from a "varchar(100)[]" column, which parses and plans and is
// still the wrong type.
func TestConvertDBSchemaToGoSchema_PostgresArrayColumnUsesTheServerSpelling(t *testing.T) {
	tests := []struct {
		name     string
		column   types.DBColumn
		wantType string
	}{
		{
			name: "a sized element type keeps its length",
			column: types.DBColumn{
				Name:          "records",
				DataType:      "ARRAY",
				UDTName:       "_varchar",
				FormattedType: "character varying(100)[]",
			},
			wantType: "character varying(100)[]",
		},
		{
			name: "an unsized element type",
			column: types.DBColumn{
				Name:          "tags",
				DataType:      "ARRAY",
				UDTName:       "_text",
				FormattedType: "text[]",
			},
			wantType: "text[]",
		},
		{
			name: "an enum element type is spelled by its own name",
			column: types.DBColumn{
				Name:          "statuses",
				DataType:      "ARRAY",
				UDTName:       "_status",
				FormattedType: "status[]",
			},
			wantType: "status[]",
		},
		{
			name: "a column the server did not have to spell is untouched",
			column: types.DBColumn{
				Name:               "title",
				DataType:           "character varying",
				CharacterMaxLength: new(64),
			},
			wantType: "VARCHAR(64)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbSchema := &types.DBSchema{
				Tables: []types.DBTable{{Name: "logs", Columns: []types.DBColumn{test.column}}},
			}

			result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

			c.Assert(result.Fields, qt.HasLen, 1)
			c.Assert(result.Fields[0].Type, qt.Equals, test.wantType)
		})
	}
}

// A PostgreSQL domain is reported by information_schema under its BASE type,
// and the base is what decides which branch of the converter used to run first.
// A domain over a built-in base survived; a domain over a user-defined base was
// flattened to that base and the CHECK it carries went with it
// (stokaro/ptah#1138).
//
// The catalog rows are copied from PostgreSQL 17, one cluster, four columns:
//
//	column      data_type      udt_name   domain_name   format_type
//	c_domain    integer        int4       positive_int  positive_int
//	c_point3d   USER-DEFINED   cube       point3d       point3d
//	c_tags      ARRAY          _text      tags          tags
//	c_cube      USER-DEFINED   cube       (null)        (not read)
//
// The reader fills FormattedType for the first three and leaves it empty for
// the fourth, which is what keeps the last row a control rather than a
// duplicate: an ordinary user-defined column is not a domain and must still be
// named by UDTName.
func TestConvertDBSchemaToGoSchema_PostgresDomainColumnKeepsTheDomain(t *testing.T) {
	tests := []struct {
		name     string
		column   types.DBColumn
		wantType string
	}{
		{
			name: "a domain over a built-in base",
			column: types.DBColumn{
				Name:          "c_domain",
				DataType:      "integer",
				UDTName:       "int4",
				FormattedType: "positive_int",
			},
			wantType: "positive_int",
		},
		{
			name: "a domain over a user-defined base",
			column: types.DBColumn{
				Name:          "c_point3d",
				DataType:      "USER-DEFINED",
				UDTName:       "cube",
				FormattedType: "point3d",
			},
			wantType: "point3d",
		},
		{
			name: "a domain over an array",
			column: types.DBColumn{
				Name:          "c_tags",
				DataType:      "ARRAY",
				UDTName:       "_text",
				FormattedType: "tags",
			},
			wantType: "tags",
		},
		{
			name: "a user-defined column that is not a domain still uses UDTName",
			column: types.DBColumn{
				Name:     "c_cube",
				DataType: "USER-DEFINED",
				UDTName:  "cube",
			},
			wantType: "cube",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbSchema := &types.DBSchema{
				Tables: []types.DBTable{{Name: "scalars", Columns: []types.DBColumn{test.column}}},
			}

			result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

			c.Assert(result.Fields, qt.HasLen, 1)
			c.Assert(result.Fields[0].Type, qt.Equals, test.wantType)
		})
	}
}

// TestConvertDBSchemaToGoSchema_PostgresDomainColumnKeepsItsDomain pins the
// desired-state side of stokaro/ptah#1242.
//
// information_schema reports a domain column's BASE type in data_type and puts
// the domain in domain_name, so a conversion that trusts data_type rebuilds the
// column as `integer` and drops every constraint the domain carries. Measured
// on the pinned community binary v1.3.0 against PostgreSQL 17.10, the binary
// reports `type = sql("positive")` for such a column.
//
// The SERIAL row is the case where two rules meet. A domain column that also
// draws from an owned sequence satisfies the SERIAL detection -- data_type is
// the domain's base type and pg_get_serial_sequence answers -- and SERIAL only
// ever builds an integer column, so the shorthand would silently undo the
// domain. The domain wins, and the sequence default it was folding away is
// carried explicitly instead.
//
// The enum, composite and range rows are the shape where a domain does NOT put
// its base type in data_type. When the base type is itself user-defined the
// catalog reports data_type = 'USER-DEFINED' with udt_name naming the BASE type
// -- measured on PostgreSQL 17.10, `c d_enum` where `CREATE DOMAIN d_enum AS
// color` reads back as data_type 'USER-DEFINED', udt_name 'color', domain_name
// 'd_enum', format_type 'd_enum'. Answering udt_name there rebuilds the column
// as the bare enum and drops the domain's CHECK, which is the same loss the
// `positive` rows above pin, on the branch that reaches USER-DEFINED first.
// TestConvertDBSchemaToGoSchema_PostgresUserDefinedColumnUsesUDTName is the
// control: a USER-DEFINED column with no domain still answers with udt_name.
func TestConvertDBSchemaToGoSchema_PostgresDomainColumnKeepsItsDomain(t *testing.T) {
	nextval := "nextval('s'::regclass)"

	tests := []struct {
		name            string
		column          types.DBColumn
		wantType        string
		wantDefaultExpr string
	}{
		{
			name: "domain column keeps the domain, not its base type",
			column: types.DBColumn{
				Name:          "qty",
				DataType:      "integer",
				UDTName:       "int4",
				FormattedType: "positive",
				DomainName:    "positive",
			},
			wantType: "positive",
		},
		{
			name: "a domain outside the search path keeps its qualifier",
			column: types.DBColumn{
				Name:          "qty",
				DataType:      "integer",
				UDTName:       "int4",
				FormattedType: "doms.positive",
				DomainName:    "positive",
			},
			wantType: "doms.positive",
		},
		{
			name: "a domain column drawing from a sequence is not a SERIAL",
			column: types.DBColumn{
				Name:            "id",
				DataType:        "integer",
				UDTName:         "int4",
				FormattedType:   "positive",
				DomainName:      "positive",
				ColumnDefault:   &nextval,
				IsAutoIncrement: true,
			},
			wantType:        "positive",
			wantDefaultExpr: nextval,
		},
		{
			name: "a domain over an enum keeps the domain, not the enum",
			column: types.DBColumn{
				Name:          "c",
				DataType:      "USER-DEFINED",
				UDTName:       "color",
				FormattedType: "d_enum",
				DomainName:    "d_enum",
			},
			wantType: "d_enum",
		},
		{
			name: "a domain over a composite type keeps the domain",
			column: types.DBColumn{
				Name:          "a",
				DataType:      "USER-DEFINED",
				UDTName:       "addr",
				FormattedType: "d_comp",
				DomainName:    "d_comp",
			},
			wantType: "d_comp",
		},
		{
			name: "a domain over a range type keeps the domain",
			column: types.DBColumn{
				Name:          "r",
				DataType:      "USER-DEFINED",
				UDTName:       "myrange",
				FormattedType: "d_range",
				DomainName:    "d_range",
			},
			wantType: "d_range",
		},
		{
			name: "a domain over an enum outside the search path keeps its qualifier",
			column: types.DBColumn{
				Name:          "c",
				DataType:      "USER-DEFINED",
				UDTName:       "color",
				FormattedType: "doms.d_enum",
				DomainName:    "d_enum",
			},
			wantType: "doms.d_enum",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbSchema := &types.DBSchema{
				Tables: []types.DBTable{{Name: "t", Columns: []types.DBColumn{test.column}}},
			}

			result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

			c.Assert(result.Fields, qt.HasLen, 1)
			c.Assert(result.Fields[0].Type, qt.Equals, test.wantType)
			c.Assert(result.Fields[0].DefaultExpr, qt.Equals, test.wantDefaultExpr)
		})
	}
}

// TestConvertDBSchemaToGoSchema_SerialDetectionSurvivesTheDomainRule is the
// control for the SERIAL row above: a plain integer column with the same
// sequence default must still be written back as the SERIAL shorthand, with the
// default folded into it rather than restated.
func TestConvertDBSchemaToGoSchema_SerialDetectionSurvivesTheDomainRule(t *testing.T) {
	c := qt.New(t)
	nextval := "nextval('t_id_seq'::regclass)"

	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{{Name: "t", Columns: []types.DBColumn{{
			Name:            "id",
			DataType:        "integer",
			UDTName:         "int4",
			ColumnDefault:   &nextval,
			IsAutoIncrement: true,
		}}}},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Fields, qt.HasLen, 1)
	c.Assert(result.Fields[0].Type, qt.Equals, "SERIAL")
	c.Assert(result.Fields[0].DefaultExpr, qt.Equals, "")
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

func TestConvertDBSchemaToGoSchema_DuplicateTableNamesUseSchemaQualifiedStructNames(t *testing.T) {
	c := qt.New(t)
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Schema: "auth",
				Name:   "users",
				Columns: []types.DBColumn{
					{Name: "id", DataType: "integer"},
					{Name: "email", DataType: "text"},
				},
			},
			{
				Schema: "billing",
				Name:   "users",
				Columns: []types.DBColumn{
					{Name: "id", DataType: "integer"},
					{Name: "external_id", DataType: "text"},
				},
			},
		},
		Indexes: []types.DBIndex{
			{Schema: "auth", TableName: "users", Name: "users_email_key", Columns: []string{"email"}, IsUnique: true},
			{Schema: "billing", TableName: "users", Name: "users_external_id_key", Columns: []string{"external_id"}, IsUnique: true},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Tables, qt.DeepEquals, []goschema.Table{
		{StructName: "AuthUsers", Schema: "auth", Name: "users"},
		{StructName: "BillingUsers", Schema: "billing", Name: "users"},
	})
	c.Assert(result.Fields, qt.DeepEquals, []goschema.Field{
		{StructName: "AuthUsers", FieldName: "Id", Name: "id", Type: "integer", Nullable: false},
		{StructName: "AuthUsers", FieldName: "Email", Name: "email", Type: "text", Nullable: false},
		{StructName: "BillingUsers", FieldName: "Id", Name: "id", Type: "integer", Nullable: false},
		{StructName: "BillingUsers", FieldName: "ExternalId", Name: "external_id", Type: "text", Nullable: false},
	})
	c.Assert(result.Indexes, qt.DeepEquals, []goschema.Index{
		{StructName: "AuthUsers", Name: "users_email_key", TableName: "auth.users", Fields: []string{"email"}, Unique: true},
		{StructName: "BillingUsers", Name: "users_external_id_key", TableName: "billing.users", Fields: []string{"external_id"}, Unique: true},
	})
}

func TestConvertDBSchemaToGoSchema_PreservesIndexPartDirection(t *testing.T) {
	c := qt.New(t)
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{Schema: "dbo", Name: "users"},
		},
		Indexes: []types.DBIndex{
			{
				Schema:    "dbo",
				TableName: "users",
				Name:      "idx_users_lookup",
				Columns:   []string{"email", "status"},
				Parts: []types.DBIndexPart{
					{Name: "email", Desc: true},
					{Name: "status"},
				},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Indexes, qt.HasLen, 1)
	c.Assert(result.Indexes[0].Fields, qt.DeepEquals, []string{"email", "status"})
	c.Assert(result.Indexes[0].Parts, qt.DeepEquals, []goschema.IndexPart{
		{Name: "email", Desc: true},
		{Name: "status"},
	})
}

func TestConvertDBSchemaToGoSchema_PreservesIndexPartExpression(t *testing.T) {
	c := qt.New(t)
	// An expression key must arrive in the model as an expression. Dropping it
	// into Name makes the renderer quote it, and CREATE INDEX ... ("lower(name)")
	// is rejected by PostgreSQL with `column "lower(name)" does not exist`.
	// See #1242.
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{Schema: "public", Name: "users"},
		},
		Indexes: []types.DBIndex{
			{
				Schema:    "public",
				TableName: "users",
				Name:      "idx_users_lower_name",
				Columns:   []string{"tenant_id", "lower(name)"},
				Parts: []types.DBIndexPart{
					{Name: "tenant_id"},
					{Expr: "lower(name)"},
				},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Indexes, qt.HasLen, 1)
	c.Assert(result.Indexes[0].Parts, qt.DeepEquals, []goschema.IndexPart{
		{Name: "tenant_id"},
		{Expr: "lower(name)"},
	})
}

// TestConvertDBSchemaToGoSchema_PreservesImplicitExtensionRequirements pins the
// one edge in the model that no text carries.
//
// The reader resolves an index's operator classes and access method against
// pg_depend because PostgreSQL prints neither when the class is the default for
// its type: a GIN index over an integer column needs btree_gin and says so
// nowhere. Dropping the answer here loses it just as completely as never asking
// (stokaro/ptah#1286).
//
// The exclusion constraint is the second half. Its backing index is skipped
// above so the constraint renders once, and the requirement it carries has to
// arrive on the constraint or it goes with the index that was skipped.
func TestConvertDBSchemaToGoSchema_PreservesImplicitExtensionRequirements(t *testing.T) {
	c := qt.New(t)
	usingMethod := "gist"
	elements := "room WITH =, during WITH &&"
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{{Schema: "public", Name: "booking"}},
		Indexes: []types.DBIndex{
			{
				Schema:             "public",
				TableName:          "booking",
				Name:               "booking_room_gin",
				Columns:            []string{"room"},
				Method:             "gin",
				RequiresExtensions: []string{"btree_gin"},
			},
			{
				Schema:             "public",
				TableName:          "booking",
				Name:               "booking_room_during_excl",
				Columns:            []string{"room", "during"},
				Method:             "gist",
				RequiresExtensions: []string{"btree_gist"},
			},
		},
		Constraints: []types.DBConstraint{{
			Schema:             "public",
			TableName:          "booking",
			Name:               "booking_room_during_excl",
			Type:               "EXCLUDE",
			UsingMethod:        &usingMethod,
			ExcludeElements:    &elements,
			RequiresExtensions: []string{"btree_gist"},
		}},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Indexes, qt.HasLen, 1)
	c.Assert(result.Indexes[0].Name, qt.Equals, "booking_room_gin")
	c.Assert(result.Indexes[0].RequiresExtensions, qt.DeepEquals, []string{"btree_gin"})
	c.Assert(result.Constraints, qt.HasLen, 1)
	c.Assert(result.Constraints[0].RequiresExtensions, qt.DeepEquals, []string{"btree_gist"},
		qt.Commentf("the constraint-backed index is dropped here, so its requirement rides the constraint"))
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

func TestConvertDBSchemaToGoSchema_PostgresSequenceSemantics(t *testing.T) {
	c := qt.New(t)
	serialDefault := "nextval('items_id_seq'::regclass)"
	standaloneSequenceDefault := "nextval('shared_ids'::regclass)"
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "items",
				Columns: []types.DBColumn{
					{
						Name:            "id",
						DataType:        "bigint",
						ColumnDefault:   &serialDefault,
						IsAutoIncrement: true,
					},
					{
						Name:          "external_id",
						DataType:      "bigint",
						ColumnDefault: &standaloneSequenceDefault,
					},
					{
						Name:               "identity_id",
						DataType:           "bigint",
						IdentityGeneration: "ALWAYS",
					},
				},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Fields, qt.HasLen, 3)
	c.Assert(result.Fields[0].Type, qt.Equals, "BIGSERIAL")
	c.Assert(result.Fields[0].AutoInc, qt.IsTrue)
	c.Assert(result.Fields[0].DefaultExpr, qt.Equals, "")
	c.Assert(result.Fields[1].Type, qt.Equals, "bigint")
	c.Assert(result.Fields[1].AutoInc, qt.IsFalse)
	c.Assert(result.Fields[1].DefaultExpr, qt.Equals, standaloneSequenceDefault)
	c.Assert(result.Fields[2].Type, qt.Equals, "bigint")
	c.Assert(result.Fields[2].IdentityGeneration, qt.Equals, "ALWAYS")
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

func TestConvertDBSchemaToGoSchema_SQLiteTableOptions(t *testing.T) {
	c := qt.New(t)
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{{
			Name:         "users",
			Strict:       true,
			WithoutRowID: true,
			Columns: []types.DBColumn{{
				Name:         "id",
				DataType:     "TEXT",
				IsPrimaryKey: true,
			}},
		}},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Tables, qt.HasLen, 1)
	c.Assert(result.Tables[0].Strict, qt.IsTrue)
	c.Assert(result.Tables[0].WithoutRowID, qt.IsTrue)
}

func TestConvertDBSchemaToGoSchema_PreservesStructuralMemberIdentity(t *testing.T) {
	c := qt.New(t)
	foreignTable := "targets"
	foreignColumn := "id"
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "tenant.data",
				Columns: []types.DBColumn{{
					Name:     "payload.id",
					DataType: "INTEGER",
				}},
			},
			{
				Schema: "tenant.data",
				Name:   "payload",
				Columns: []types.DBColumn{{
					Name:     "id",
					DataType: "INTEGER",
				}},
			},
		},
		Indexes: []types.DBIndex{
			{Name: "payload.lookup", TableName: "tenant.data", Columns: []string{"payload.id"}},
			{Name: "lookup", Schema: "tenant.data", TableName: "payload", Columns: []string{"id"}},
		},
		Constraints: []types.DBConstraint{
			{
				Name:           "fk_literal",
				TableName:      "tenant.data",
				Type:           "FOREIGN KEY",
				ColumnName:     "payload.id",
				ColumnNames:    []string{"payload.id"},
				ForeignTable:   &foreignTable,
				ForeignColumn:  &foreignColumn,
				ForeignColumns: []string{"id"},
			},
			{
				Name:           "fk_qualified",
				Schema:         "tenant.data",
				TableName:      "payload",
				Type:           "FOREIGN KEY",
				ColumnName:     "id",
				ColumnNames:    []string{"id"},
				ForeignTable:   &foreignTable,
				ForeignColumn:  &foreignColumn,
				ForeignColumns: []string{"id"},
			},
			{
				Name:        "payload.lookup",
				TableName:   "tenant.data",
				Type:        "UNIQUE",
				ColumnName:  "payload.id",
				ColumnNames: []string{"payload.id"},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(result.Fields, qt.HasLen, 2)
	c.Assert(result.Fields[0].ForeignKeyName, qt.Equals, "fk_literal")
	c.Assert(result.Fields[1].ForeignKeyName, qt.Equals, "fk_qualified")
	c.Assert(result.Indexes, qt.HasLen, 1)
	c.Assert(result.Indexes[0].TableName, qt.Equals, `"tenant.data".payload`)
	c.Assert(result.Indexes[0].Name, qt.Equals, "lookup")
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
	c.Assert(ext.Schema, qt.Equals, "public")
	c.Assert(ext.IfNotExists, qt.Equals, true) // Should default to true for safety
	c.Assert(ext.Version, qt.Equals, "1.0")
	c.Assert(ext.Comment, qt.Equals, "") // Should be empty string when nil
}

// TestConvertDBSchemaToGoSchema_GrantsDescribeTheTargetTheSharedContractNames
// pins which field a described grant reads its target out of.
//
// A SCHEMA-typed row carries the schema in ObjectName with Schema empty, and a
// TABLE-typed row carries the schema in Schema. Reading them positionally
// produces a declaration naming the wrong object, and on ClickHouse that made
// every database-scoped grant compare unequal to the row it had just created.
func TestConvertDBSchemaToGoSchema_GrantsDescribeTheTargetTheSharedContractNames(t *testing.T) {
	tests := []struct {
		name         string
		grant        types.DBGrant
		wantOnSchema string
		wantOnTable  string
	}{
		{
			name:         "a schema grant",
			grant:        types.DBGrant{Role: "reader", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "shop"},
			wantOnSchema: "shop",
		},
		{
			name:        "a table grant",
			grant:       types.DBGrant{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
			wantOnTable: "shop.orders",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted := dbschematogo.ConvertDBSchemaToGoSchema(
				&types.DBSchema{Grants: []types.DBGrant{test.grant}},
			)

			c.Assert(converted.Grants, qt.HasLen, 1)
			c.Assert(converted.Grants[0].OnSchema, qt.Equals, test.wantOnSchema)
			c.Assert(converted.Grants[0].OnTable, qt.Equals, test.wantOnTable)
		})
	}
}

// TestConvertDBSchemaToGoSchema_PartialRevokeIsNotDescribedAsAGrant pins the
// one row shape that means the OPPOSITE of a grant.
//
// ClickHouse records `GRANT SELECT ON db.* TO r; REVOKE SELECT ON db.t FROM r`
// as two rows, the second with is_partial_revoke set. Describing that second
// row as a Grant would produce a document stating the role HOLDS a privilege
// the server records it as having lost, and applying that document would grant
// it for real. The broader grant stays, because dropping it too would make a
// comparison plan a GRANT that wipes the exception out.
func TestConvertDBSchemaToGoSchema_PartialRevokeIsNotDescribedAsAGrant(t *testing.T) {
	c := qt.New(t)

	converted := dbschematogo.ConvertDBSchemaToGoSchema(&types.DBSchema{
		Grants: []types.DBGrant{
			{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "shop"},
			{
				Role: "reader", Privilege: "SELECT", ObjectType: "TABLE",
				Schema: "shop", ObjectName: "orders", IsPartialRevoke: true,
			},
		},
	})

	c.Assert(converted.Grants, qt.HasLen, 1)
	c.Assert(converted.Grants[0].OnSchema, qt.Equals, "shop")
	c.Assert(converted.Grants[0].OnTable, qt.Equals, "")
}
