package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

func TestTableColumns_UnhappyPath(t *testing.T) {
	tests := []struct {
		name     string
		genTable schemamodel.Table
		dbTable  catalog.Table
		desired  *schemamodel.Database
		expected difftypes.TableDiff
	}{
		{
			name:     "no fields for struct",
			genTable: schemamodel.Table{StructName: "User", Name: "users"},
			dbTable: catalog.Table{
				Name:    "users",
				Columns: make([]catalog.Column, 0),
			},
			desired: &schemamodel.Database{
				Fields: []schemamodel.Field{
					{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true}, // Different struct
				},
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			expected: difftypes.TableDiff{
				TableName: "users",
			},
		},
		{
			name:     "empty database table",
			genTable: schemamodel.Table{StructName: "User", Name: "users"},
			dbTable: catalog.Table{
				Name:    "users",
				Columns: make([]catalog.Column, 0),
			},
			desired: &schemamodel.Database{
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
				},
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			expected: difftypes.TableDiff{
				TableName:    "users",
				ColumnsAdded: difftypes.ColumnChanges{{Name: "id"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.TableColumns(tt.genTable, tt.dbTable, tt.desired)

			c.Assert(result.TableName, qt.Equals, tt.expected.TableName)
			// Compared by name: these rows are about WHICH columns the
			// comparison reports, and the definitions each one carries are
			// covered where they are rendered (stokaro/ptah#2315).
			c.Assert(result.ColumnsAdded.Names(), qt.DeepEquals, tt.expected.ColumnsAdded.Names())
			c.Assert(result.ColumnsRemoved.Names(), qt.DeepEquals, tt.expected.ColumnsRemoved.Names())
		})
	}
}

func TestColumns_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		genCol   schemamodel.Field
		dbCol    catalog.Column
		expected difftypes.ColumnDiff
	}{
		{
			name: "type change",
			genCol: schemamodel.Field{
				Name: "name",
				Type: "VARCHAR(255)",
			},
			dbCol: catalog.Column{
				Name:     "name",
				DataType: "TEXT",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "name",
				Changes: map[string]string{
					"type": "text -> varchar",
				},
			},
		},
		{
			name: "nullable change",
			genCol: schemamodel.Field{
				Name:     "email",
				Type:     "VARCHAR(255)",
				Nullable: false,
			},
			dbCol: catalog.Column{
				Name:       "email",
				DataType:   "VARCHAR(255)",
				IsNullable: "YES",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "email",
				Changes: map[string]string{
					"nullable": "true -> false",
				},
			},
		},
		{
			name: "varchar narrowing preserves raw type",
			genCol: schemamodel.Field{
				Name: "name",
				Type: "VARCHAR(100)",
			},
			dbCol: catalog.Column{
				Name:     "name",
				DataType: "VARCHAR(255)",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "name",
				Changes: map[string]string{
					"type": "VARCHAR(255) -> VARCHAR(100)",
				},
			},
		},
		{
			name: "postgres varchar narrowing uses length metadata",
			genCol: schemamodel.Field{
				Name: "name",
				Type: "VARCHAR(100)",
			},
			dbCol: catalog.Column{
				Name:               "name",
				DataType:           "character varying",
				UDTName:            "varchar",
				CharacterMaxLength: func() *int { value := 255; return &value }(),
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "name",
				Changes: map[string]string{
					"type": "varchar(255) -> VARCHAR(100)",
				},
			},
		},
		{
			name: "integer narrowing preserves raw type",
			genCol: schemamodel.Field{
				Name: "count",
				Type: "integer",
			},
			dbCol: catalog.Column{
				Name:     "count",
				DataType: "bigint",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "count",
				Changes: map[string]string{
					"type": "bigint -> integer",
				},
			},
		},
		{
			name: "decimal narrowing preserves raw type",
			genCol: schemamodel.Field{
				Name: "price",
				Type: "NUMERIC(10,2)",
			},
			dbCol: catalog.Column{
				Name:     "price",
				DataType: "NUMERIC(12,2)",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "price",
				Changes: map[string]string{
					"type": "NUMERIC(12,2) -> NUMERIC(10,2)",
				},
			},
		},
		{
			name: "varchar widening preserves raw type",
			genCol: schemamodel.Field{
				Name: "name",
				Type: "VARCHAR(255)",
			},
			dbCol: catalog.Column{
				Name:     "name",
				DataType: "VARCHAR(100)",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "name",
				Changes: map[string]string{
					"type": "VARCHAR(100) -> VARCHAR(255)",
				},
			},
		},
		{
			name: "postgres varchar widening uses length metadata",
			genCol: schemamodel.Field{
				Name: "name",
				Type: "VARCHAR(255)",
			},
			dbCol: catalog.Column{
				Name:               "name",
				DataType:           "character varying",
				UDTName:            "varchar",
				CharacterMaxLength: func() *int { value := 100; return &value }(),
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "name",
				Changes: map[string]string{
					"type": "varchar(100) -> VARCHAR(255)",
				},
			},
		},
		{
			name: "integer widening preserves raw type",
			genCol: schemamodel.Field{
				Name: "count",
				Type: "bigint",
			},
			dbCol: catalog.Column{
				Name:     "count",
				DataType: "integer",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "count",
				Changes: map[string]string{
					"type": "integer -> bigint",
				},
			},
		},
		{
			name: "decimal widening preserves raw type",
			genCol: schemamodel.Field{
				Name: "price",
				Type: "NUMERIC(12,2)",
			},
			dbCol: catalog.Column{
				Name:     "price",
				DataType: "NUMERIC(10,2)",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "price",
				Changes: map[string]string{
					"type": "NUMERIC(10,2) -> NUMERIC(12,2)",
				},
			},
		},
		{
			name: "primary key change",
			genCol: schemamodel.Field{
				Name:    "id",
				Type:    "SERIAL",
				Primary: true,
			},
			dbCol: catalog.Column{
				Name:         "id",
				DataType:     "integer",
				IsPrimaryKey: false,
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "id",
				Changes: map[string]string{
					"primary_key": "false -> true",
				},
			},
		},
		{
			name: "unique constraint change",
			genCol: schemamodel.Field{
				Name:   "email",
				Type:   "VARCHAR(255)",
				Unique: true,
			},
			dbCol: catalog.Column{
				Name:     "email",
				DataType: "VARCHAR(255)",
				IsUnique: false,
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "email",
				Changes: map[string]string{
					"unique": "false -> true",
				},
			},
		},
		{
			name: "default value change",
			genCol: schemamodel.Field{
				Name:    "status",
				Type:    "VARCHAR(50)",
				Default: "'active'",
			},
			dbCol: catalog.Column{
				Name:          "status",
				DataType:      "VARCHAR(50)",
				ColumnDefault: new("'inactive'"),
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "status",
				Changes: map[string]string{
					"default": "'inactive' -> 'active'",
				},
			},
		},
		{
			name: "multiple changes",
			genCol: schemamodel.Field{
				Name:     "name",
				Type:     "TEXT",
				Nullable: false,
				Unique:   true,
			},
			dbCol: catalog.Column{
				Name:       "name",
				DataType:   "VARCHAR(100)",
				IsNullable: "YES",
				IsUnique:   false,
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "name",
				Changes: map[string]string{
					"type":     "varchar -> text",
					"nullable": "true -> false",
					"unique":   "false -> true",
				},
			},
		},
		{
			name: "generated expression change",
			genCol: schemamodel.Field{
				Name:                "slug",
				Type:                "TEXT",
				GeneratedExpression: "lower(name)",
				GeneratedKind:       "STORED",
			},
			dbCol: catalog.Column{
				Name:          "slug",
				DataType:      "TEXT",
				IsNullable:    "NO",
				GeneratedKind: "VIRTUAL",
				GeneratedExpression: func() *string {
					value := "upper(name)"
					return &value
				}(),
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "slug",
				Changes: map[string]string{
					"generated": "VIRTUAL upper(name) -> STORED lower(name)",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.Columns(tt.genCol, tt.dbCol)

			c.Assert(result.ColumnName, qt.Equals, tt.expected.ColumnName)
			c.Assert(result.Changes, qt.HasLen, len(tt.expected.Changes))
			for key, expectedValue := range tt.expected.Changes {
				c.Assert(result.Changes[key], qt.Equals, expectedValue)
			}
		})
	}
}

func TestColumns_UnhappyPath(t *testing.T) {
	tests := []struct {
		name     string
		genCol   schemamodel.Field
		dbCol    catalog.Column
		expected difftypes.ColumnDiff
	}{
		{
			name: "no changes",
			genCol: schemamodel.Field{
				Name:     "id",
				Type:     "SERIAL",
				Primary:  true,
				Nullable: false,
			},
			dbCol: catalog.Column{
				Name:         "id",
				DataType:     "integer",
				IsPrimaryKey: true,
				IsNullable:   "NO",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "id",
				Changes:    make(map[string]string),
			},
		},
		{
			name: "auto increment column ignores default",
			genCol: schemamodel.Field{
				Name:    "id",
				Type:    "SERIAL",
				Primary: true,
				Default: "",
			},
			dbCol: catalog.Column{
				Name:            "id",
				DataType:        "integer",
				IsPrimaryKey:    true,
				IsAutoIncrement: true,
				ColumnDefault:   new("nextval('users_id_seq'::regclass)"),
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "id",
				Changes:    make(map[string]string),
			},
		},
		{
			name: "primary key forces not null",
			genCol: schemamodel.Field{
				Name:     "id",
				Type:     "SERIAL",
				Primary:  true,
				Nullable: true, // This should be ignored for primary keys
			},
			dbCol: catalog.Column{
				Name:         "id",
				DataType:     "integer",
				IsPrimaryKey: true,
				IsNullable:   "NO",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "id",
				Changes:    make(map[string]string),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.Columns(tt.genCol, tt.dbCol)

			c.Assert(result.ColumnName, qt.Equals, tt.expected.ColumnName)
			c.Assert(result.Changes, qt.HasLen, len(tt.expected.Changes))
			for key, expectedValue := range tt.expected.Changes {
				c.Assert(result.Changes[key], qt.Equals, expectedValue)
			}
		})
	}
}

func TestEnums_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "enum added",
			desired: &schemamodel.Database{
				Enums: []schemamodel.Enum{
					{Name: "status_enum", Values: []string{"active", "inactive"}},
				},
			},
			database: &catalog.Database{
				Enums: make([]catalog.Enum, 0),
			},
			expected: &difftypes.SchemaDiff{
				EnumsAdded: difftypes.EnumChanges{{Name: "status_enum", Values: []string{"active", "inactive", "deprecated"}}},
			},
		},
		{
			name: "enum removed",
			desired: &schemamodel.Database{
				Enums: make([]schemamodel.Enum, 0),
			},
			database: &catalog.Database{
				Enums: []catalog.Enum{
					{Name: "old_enum", Values: []string{"value1", "value2"}},
				},
			},
			expected: &difftypes.SchemaDiff{
				EnumsRemoved: difftypes.EnumChanges{{Name: "old_enum"}},
			},
		},
		{
			name: "enum modified",
			desired: &schemamodel.Database{
				Enums: []schemamodel.Enum{
					{Name: "status_enum", Values: []string{"active", "inactive", "pending"}},
				},
			},
			database: &catalog.Database{
				Enums: []catalog.Enum{
					{Name: "status_enum", Values: []string{"active", "inactive"}},
				},
			},
			expected: &difftypes.SchemaDiff{
				EnumsModified: []difftypes.EnumDiff{
					{
						EnumName:      "status_enum",
						ValuesAdded:   []string{"pending"},
						ValuesRemoved: nil,
					},
				},
			},
		},
		{
			name: "multiple enum changes",
			desired: &schemamodel.Database{
				Enums: []schemamodel.Enum{
					{Name: "status_enum", Values: []string{"active", "inactive"}},
					{Name: "priority_enum", Values: []string{"low", "medium", "high"}},
				},
			},
			database: &catalog.Database{
				Enums: []catalog.Enum{
					{Name: "status_enum", Values: []string{"active", "inactive", "deprecated"}},
					{Name: "old_enum", Values: []string{"value1"}},
				},
			},
			expected: &difftypes.SchemaDiff{
				EnumsAdded:   difftypes.EnumChanges{{Name: "priority_enum", Values: []string{"low", "medium", "high"}}},
				EnumsRemoved: difftypes.EnumChanges{{Name: "old_enum"}},
				EnumsModified: []difftypes.EnumDiff{
					{
						EnumName:      "status_enum",
						ValuesAdded:   nil,
						ValuesRemoved: []string{"deprecated"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Enums(tt.desired, tt.database, diff)

			c.Assert(diff.EnumsAdded.Names(), qt.DeepEquals, tt.expected.EnumsAdded.Names())
			c.Assert(diff.EnumsRemoved.Names(), qt.DeepEquals, tt.expected.EnumsRemoved.Names())
			c.Assert(diff.EnumsModified, qt.HasLen, len(tt.expected.EnumsModified))

			for i, expectedEnumDiff := range tt.expected.EnumsModified {
				c.Assert(diff.EnumsModified[i].EnumName, qt.Equals, expectedEnumDiff.EnumName)
				c.Assert(diff.EnumsModified[i].ValuesAdded, qt.DeepEquals, expectedEnumDiff.ValuesAdded)
				c.Assert(diff.EnumsModified[i].ValuesRemoved, qt.DeepEquals, expectedEnumDiff.ValuesRemoved)
			}
		})
	}
}

func TestEnums_UnhappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "empty schemas",
			desired: &schemamodel.Database{
				Enums: make([]schemamodel.Enum, 0),
			},
			database: &catalog.Database{
				Enums: make([]catalog.Enum, 0),
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "nil enums",
			desired: &schemamodel.Database{
				Enums: nil,
			},
			database: &catalog.Database{
				Enums: nil,
			},
			expected: &difftypes.SchemaDiff{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Enums(tt.desired, tt.database, diff)

			c.Assert(diff.EnumsAdded.Names(), qt.DeepEquals, tt.expected.EnumsAdded.Names())
			c.Assert(diff.EnumsRemoved.Names(), qt.DeepEquals, tt.expected.EnumsRemoved.Names())
			c.Assert(diff.EnumsModified, qt.HasLen, len(tt.expected.EnumsModified))
		})
	}
}

func TestEnumValues_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		genEnum  schemamodel.Enum
		dbEnum   catalog.Enum
		expected difftypes.EnumDiff
	}{
		{
			name: "values added",
			genEnum: schemamodel.Enum{
				Name:   "status_enum",
				Values: []string{"active", "inactive", "pending", "archived"},
			},
			dbEnum: catalog.Enum{
				Name:   "status_enum",
				Values: []string{"active", "inactive"},
			},
			expected: difftypes.EnumDiff{
				EnumName:      "status_enum",
				ValuesAdded:   []string{"archived", "pending"},
				ValuesRemoved: nil,
			},
		},
		{
			name: "values removed",
			genEnum: schemamodel.Enum{
				Name:   "status_enum",
				Values: []string{"active", "inactive"},
			},
			dbEnum: catalog.Enum{
				Name:   "status_enum",
				Values: []string{"active", "inactive", "deprecated", "legacy"},
			},
			expected: difftypes.EnumDiff{
				EnumName:      "status_enum",
				ValuesAdded:   nil,
				ValuesRemoved: []string{"deprecated", "legacy"},
			},
		},
		{
			name: "mixed changes",
			genEnum: schemamodel.Enum{
				Name:   "priority_enum",
				Values: []string{"low", "medium", "high", "critical"},
			},
			dbEnum: catalog.Enum{
				Name:   "priority_enum",
				Values: []string{"low", "medium", "urgent"},
			},
			expected: difftypes.EnumDiff{
				EnumName:      "priority_enum",
				ValuesAdded:   []string{"critical", "high"},
				ValuesRemoved: []string{"urgent"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.EnumValues(tt.genEnum, tt.dbEnum)

			c.Assert(result.EnumName, qt.Equals, tt.expected.EnumName)
			c.Assert(result.ValuesAdded, qt.DeepEquals, tt.expected.ValuesAdded)
			c.Assert(result.ValuesRemoved, qt.DeepEquals, tt.expected.ValuesRemoved)
		})
	}
}

func TestEnumValues_UnhappyPath(t *testing.T) {
	tests := []struct {
		name     string
		genEnum  schemamodel.Enum
		dbEnum   catalog.Enum
		expected difftypes.EnumDiff
	}{
		{
			name: "no changes",
			genEnum: schemamodel.Enum{
				Name:   "status_enum",
				Values: []string{"active", "inactive"},
			},
			dbEnum: catalog.Enum{
				Name:   "status_enum",
				Values: []string{"active", "inactive"},
			},
			expected: difftypes.EnumDiff{
				EnumName:      "status_enum",
				ValuesAdded:   nil,
				ValuesRemoved: nil,
			},
		},
		{
			name: "empty enum values",
			genEnum: schemamodel.Enum{
				Name:   "empty_enum",
				Values: make([]string, 0),
			},
			dbEnum: catalog.Enum{
				Name:   "empty_enum",
				Values: make([]string, 0),
			},
			expected: difftypes.EnumDiff{
				EnumName:      "empty_enum",
				ValuesAdded:   nil,
				ValuesRemoved: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.EnumValues(tt.genEnum, tt.dbEnum)

			c.Assert(result.EnumName, qt.Equals, tt.expected.EnumName)
			c.Assert(result.ValuesAdded, qt.DeepEquals, tt.expected.ValuesAdded)
			c.Assert(result.ValuesRemoved, qt.DeepEquals, tt.expected.ValuesRemoved)
		})
	}
}

func TestIndexes_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "index added",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_user_email", TableName: "users"},
				},
			},
			database: &catalog.Database{
				Indexes: make([]catalog.Index, 0),
			},
			expected: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "idx_user_email", TableName: "users"},
				},
			},
		},
		{
			name: "index removed",
			desired: &schemamodel.Database{
				Indexes: make([]schemamodel.Index, 0),
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "old_index", TableName: "users", IsPrimary: false, IsUnique: false},
				},
			},
			expected: &difftypes.SchemaDiff{
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "old_index", TableName: "users"},
				},
			},
		},
		{
			name: "primary key index ignored",
			desired: &schemamodel.Database{
				Indexes: make([]schemamodel.Index, 0),
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "users_pkey", IsPrimary: true, IsUnique: false},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "unique constraint index ignored",
			desired: &schemamodel.Database{
				Indexes: make([]schemamodel.Index, 0),
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "users_email_key", TableName: "users", Columns: []string{"email"}, IsPrimary: false, IsUnique: true},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "multiple index changes",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_user_email", TableName: "users"},
					{Name: "idx_user_name", TableName: "users"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "idx_user_email", TableName: "users", IsPrimary: false, IsUnique: false},
					{Name: "old_index", TableName: "users", IsPrimary: false, IsUnique: false},
					{Name: "users_pkey", TableName: "users", IsPrimary: true, IsUnique: false}, // Should be ignored
				},
			},
			expected: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "idx_user_name", TableName: "users"},
				},
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "old_index", TableName: "users"},
				},
			},
		},
		{
			name: "index nulls distinct changed",
			desired: func() *schemamodel.Database {
				nullsDistinct := false
				return &schemamodel.Database{
					Indexes: []schemamodel.Index{
						{Name: "idx_users_c", StructName: "users", TableName: "users", Fields: []string{"c"}, Unique: true, NullsDistinct: &nullsDistinct},
					},
				}
			}(),
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "idx_users_c", TableName: "users", Columns: []string{"c"}, IsUnique: true},
				},
			},
			expected: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "idx_users_c", TableName: "users"},
				},
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "idx_users_c", TableName: "users"},
				},
			},
		},
		{
			name: "partial index condition changed",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_users_email_active", StructName: "users", TableName: "users", Fields: []string{"email"}, Condition: "deleted_at IS NULL"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{
						Name:      "idx_users_email_active",
						TableName: "users",
						Columns:   []string{"email"},
						Condition: "deleted_at IS NOT NULL",
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "idx_users_email_active", TableName: "users"},
				},
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "idx_users_email_active", TableName: "users"},
				},
			},
		},
		{
			name: "partial index condition outer parentheses match",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_users_email_active", StructName: "users", TableName: "users", Fields: []string{"email"}, Condition: "deleted_at IS NULL"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{
						Name:      "idx_users_email_active",
						TableName: "users",
						Columns:   []string{"email"},
						Condition: "(deleted_at IS NULL)",
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "partial index condition whitespace outside literals matches",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_users_email_active", StructName: "users", TableName: "users", Fields: []string{"email"}, Condition: "deleted_at   IS\nNULL"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{
						Name:      "idx_users_email_active",
						TableName: "users",
						Columns:   []string{"email"},
						Condition: "(deleted_at IS NULL)",
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "partial index condition whitespace inside string literal differs",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_users_email_active", StructName: "users", TableName: "users", Fields: []string{"email"}, Condition: "status = 'a  b'"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{
						Name:      "idx_users_email_active",
						TableName: "users",
						Columns:   []string{"email"},
						Condition: "status = 'a b'",
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "idx_users_email_active", TableName: "users"},
				},
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "idx_users_email_active", TableName: "users"},
				},
			},
		},
		{
			name: "partial index condition postgres IN rewrite does not drift",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_users_status", StructName: "users", TableName: "users", Fields: []string{"status"}, Condition: "status IN ('active','pending')"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{
						Name:      "idx_users_status",
						TableName: "users",
						Columns:   []string{"status"},
						Condition: "((status)::text = ANY ((ARRAY['active'::text, 'pending'::text])::text[]))",
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Indexes(tt.desired, tt.database, diff)

			c.Assert(diff.IndexesAdded, qt.DeepEquals, tt.expected.IndexesAdded)
			c.Assert(diff.IndexesRemoved, qt.DeepEquals, tt.expected.IndexesRemoved)
		})
	}
}

func TestIndexes_UnhappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "empty schemas",
			desired: &schemamodel.Database{
				Indexes: make([]schemamodel.Index, 0),
			},
			database: &catalog.Database{
				Indexes: make([]catalog.Index, 0),
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "nil indexes",
			desired: &schemamodel.Database{
				Indexes: nil,
			},
			database: &catalog.Database{
				Indexes: nil,
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "only system indexes in database",
			desired: &schemamodel.Database{
				Indexes: make([]schemamodel.Index, 0),
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "users_pkey", TableName: "users", Columns: []string{"id"}, IsPrimary: true, IsUnique: false},
					{Name: "users_email_key", TableName: "users", Columns: []string{"email"}, IsPrimary: false, IsUnique: true},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "explicitly defined unique indexes should be compared",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "tenants_slug_idx", TableName: "tenants"},
					{Name: "users_tenant_email_idx", TableName: "users"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "tenants_slug_idx", TableName: "tenants", Columns: []string{"slug"}, IsPrimary: false, IsUnique: true},
					{Name: "users_tenant_email_idx", TableName: "users", Columns: []string{"tenant_id", "email"}, IsPrimary: false, IsUnique: true},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "missing explicitly defined unique indexes should be added",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "tenants_slug_idx", TableName: "tenants"},
					{Name: "users_tenant_email_idx", TableName: "users"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					// Only constraint-based indexes exist, explicitly defined ones are missing
					{Name: "tenants_pkey", TableName: "tenants", Columns: []string{"id"}, IsPrimary: true, IsUnique: false},
					{Name: "users_email_key", TableName: "users", Columns: []string{"email"}, IsPrimary: false, IsUnique: true},
				},
			},
			expected: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "tenants_slug_idx", TableName: "tenants"},
					{Name: "users_tenant_email_idx", TableName: "users"},
				},
			},
		},
		{
			name: "constraint-based unique indexes should be ignored",
			desired: &schemamodel.Database{
				Indexes: make([]schemamodel.Index, 0),
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "users_email_key", TableName: "users", Columns: []string{"email"}, IsPrimary: false, IsUnique: true},
					{Name: "tenants_name_key", TableName: "tenants", Columns: []string{"name"}, IsPrimary: false, IsUnique: true},
					{Name: "products_sku_code_key", TableName: "products", Columns: []string{"sku", "code"}, IsPrimary: false, IsUnique: true},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "custom-named unique constraint backing indexes should be ignored",
			desired: &schemamodel.Database{
				Indexes: make([]schemamodel.Index, 0),
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
					{Name: "ptah_constraint_unique", TableName: "ptah_constraint_drift", Type: "UNIQUE", ColumnNames: []string{"sku", "region"}},
				},
				Indexes: []catalog.Index{
					{
						Name:      "ptah_constraint_unique",
						TableName: "ptah_constraint_drift",
						Columns:   []string{"sku", "region"},
						IsUnique:  true,
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "mixed constraint-based and explicitly defined unique indexes",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_users_custom_unique", TableName: "users"},
					{Name: "tenants_slug_idx", TableName: "tenants"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					// Constraint-based (should be ignored)
					{Name: "users_email_key", TableName: "users", Columns: []string{"email"}, IsPrimary: false, IsUnique: true},
					{Name: "tenants_name_key", TableName: "tenants", Columns: []string{"name"}, IsPrimary: false, IsUnique: true},
					// Explicitly defined (should be compared)
					{Name: "idx_users_custom_unique", TableName: "users", Columns: []string{"custom_field"}, IsPrimary: false, IsUnique: true},
					// Missing: tenants_slug_idx
				},
			},
			expected: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "tenants_slug_idx", TableName: "tenants"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Indexes(tt.desired, tt.database, diff)

			c.Assert(diff.IndexesAdded, qt.DeepEquals, tt.expected.IndexesAdded)
			c.Assert(diff.IndexesRemoved, qt.DeepEquals, tt.expected.IndexesRemoved)
		})
	}
}

// Note: The isConstraintBasedUniqueIndex function is tested indirectly through
// the integration tests and the main Indexes function tests, which provide
// comprehensive coverage of the constraint detection logic.

func TestColumns_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		genCol   schemamodel.Field
		dbCol    catalog.Column
		expected difftypes.ColumnDiff
	}{
		{
			name: "UDT name takes precedence over data type",
			genCol: schemamodel.Field{
				Name: "status",
				Type: "status_enum",
			},
			dbCol: catalog.Column{
				Name:     "status",
				DataType: "USER-DEFINED",
				UDTName:  "status_enum",
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "status",
				Changes:    make(map[string]string),
			},
		},
		{
			name: "SERIAL type detection for auto increment",
			genCol: schemamodel.Field{
				Name:    "id",
				Type:    "SERIAL",
				Primary: true,
				Default: "",
			},
			dbCol: catalog.Column{
				Name:            "id",
				DataType:        "integer",
				IsPrimaryKey:    true,
				IsAutoIncrement: false, // Not detected as auto increment
				ColumnDefault:   new("nextval('seq'::regclass)"),
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "id",
				Changes:    make(map[string]string), // Should ignore default due to SERIAL type
			},
		},
		{
			name: "null column default vs empty string",
			genCol: schemamodel.Field{
				Name:    "description",
				Type:    "TEXT",
				Default: "",
			},
			dbCol: catalog.Column{
				Name:          "description",
				DataType:      "TEXT",
				ColumnDefault: nil, // NULL default
			},
			expected: difftypes.ColumnDiff{
				ColumnName: "description",
				Changes:    make(map[string]string), // Both should normalize to empty
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.Columns(tt.genCol, tt.dbCol)

			c.Assert(result.ColumnName, qt.Equals, tt.expected.ColumnName)
			c.Assert(result.Changes, qt.HasLen, len(tt.expected.Changes))
			for key, expectedValue := range tt.expected.Changes {
				c.Assert(result.Changes[key], qt.Equals, expectedValue)
			}
		})
	}
}

func TestTableColumns_EdgeCases(t *testing.T) {
	c := qt.New(t)

	// Test with column modifications
	genTable := schemamodel.Table{StructName: "User", Name: "users"}
	dbTable := catalog.Table{
		Name: "users",
		Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsPrimaryKey: true},
			{Name: "name", DataType: "VARCHAR(100)", IsNullable: "YES"},
		},
	}

	desired := &schemamodel.Database{
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "name", Type: "VARCHAR(255)", Nullable: false}, // Type and nullable change
		},
		EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
	}

	result := compare.TableColumns(genTable, dbTable, desired)

	c.Assert(result.TableName, qt.Equals, "users")
	c.Assert(result.ColumnsModified, qt.HasLen, 1)
	c.Assert(result.ColumnsModified[0].ColumnName, qt.Equals, "name")
	c.Assert(result.ColumnsModified[0].Changes, qt.HasLen, 2) // nullable tightens and the varchar length widens 100 -> 255
	c.Assert(result.ColumnsModified[0].Changes["nullable"], qt.Equals, "true -> false")
	c.Assert(result.ColumnsModified[0].Changes["type"], qt.Equals, "VARCHAR(100) -> VARCHAR(255)")
}

func TestTablesAndColumns_SortingConsistency(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "zebra_table"},
			{StructName: "Post", Name: "alpha_table"},
		},
		Fields:         make([]schemamodel.Field, 0),
		EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
	}

	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "zebra_old_table"},
			{Name: "alpha_old_table"},
		},
	}

	diff := &difftypes.SchemaDiff{}
	compare.TablesAndColumns(desired, database, diff)

	// Check that results are sorted alphabetically
	c.Assert(diff.TablesAdded, qt.DeepEquals, []string{"alpha_table", "zebra_table"})
	c.Assert(diff.TablesRemoved, qt.DeepEquals, []string{"alpha_old_table", "zebra_old_table"})
}

func TestTablesAndColumns_UsesSchemaQualifiedTableIdentity(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "AuthUser", Name: "users", Schema: "auth"},
			{StructName: "BillingUser", Name: "users", Schema: "billing"},
		},
		Fields: []schemamodel.Field{
			{StructName: "AuthUser", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "BillingUser", Name: "id", Type: "INTEGER", Primary: true},
		},
		EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
	}
	database := &catalog.Database{
		Tables: []catalog.Table{
			{
				Name:   "users",
				Schema: "auth",
				Columns: []catalog.Column{
					{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true},
				},
			},
		},
	}

	diff := &difftypes.SchemaDiff{}
	compare.TablesAndColumns(desired, database, diff)

	c.Assert(diff.TablesAdded, qt.DeepEquals, []string{"billing.users"})
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}

func TestColumnByName_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		diffs        []difftypes.ColumnDiff
		columnName   string
		expectedDiff *difftypes.ColumnDiff
	}{
		{
			name: "find existing column",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "id",
					Changes: map[string]string{
						"type": "integer -> bigint",
					},
				},
				{
					ColumnName: "email",
					Changes: map[string]string{
						"type":     "varchar -> text",
						"nullable": "true -> false",
					},
				},
				{
					ColumnName: "name",
					Changes: map[string]string{
						"unique": "false -> true",
					},
				},
			},
			columnName: "email",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "email",
				Changes: map[string]string{
					"type":     "varchar -> text",
					"nullable": "true -> false",
				},
			},
		},
		{
			name: "find first column in slice",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "first_column",
					Changes: map[string]string{
						"type": "varchar -> text",
					},
				},
				{
					ColumnName: "second_column",
					Changes: map[string]string{
						"nullable": "true -> false",
					},
				},
			},
			columnName: "first_column",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "first_column",
				Changes: map[string]string{
					"type": "varchar -> text",
				},
			},
		},
		{
			name: "find last column in slice",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "first_column",
					Changes: map[string]string{
						"type": "varchar -> text",
					},
				},
				{
					ColumnName: "last_column",
					Changes: map[string]string{
						"nullable": "true -> false",
					},
				},
			},
			columnName: "last_column",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "last_column",
				Changes: map[string]string{
					"nullable": "true -> false",
				},
			},
		},
		{
			name: "find column with empty changes",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "unchanged_column",
					Changes:    make(map[string]string),
				},
			},
			columnName: "unchanged_column",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "unchanged_column",
				Changes:    make(map[string]string),
			},
		},
		{
			name: "find column with single change",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "status",
					Changes: map[string]string{
						"default": "'inactive' -> 'active'",
					},
				},
			},
			columnName: "status",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "status",
				Changes: map[string]string{
					"default": "'inactive' -> 'active'",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.SearchColumnByName(tt.diffs, tt.columnName)

			c.Assert(result, qt.IsNotNil)
			c.Assert(result.ColumnName, qt.Equals, tt.expectedDiff.ColumnName)
			c.Assert(result.Changes, qt.HasLen, len(tt.expectedDiff.Changes))
			for key, expectedValue := range tt.expectedDiff.Changes {
				c.Assert(result.Changes[key], qt.Equals, expectedValue)
			}
		})
	}
}

func TestColumnByName_UnhappyPath(t *testing.T) {
	tests := []struct {
		name       string
		diffs      []difftypes.ColumnDiff
		columnName string
	}{
		{
			name: "column not found",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "id",
					Changes: map[string]string{
						"type": "integer -> bigint",
					},
				},
				{
					ColumnName: "email",
					Changes: map[string]string{
						"nullable": "true -> false",
					},
				},
			},
			columnName: "nonexistent_column",
		},
		{
			name:       "empty slice",
			diffs:      make([]difftypes.ColumnDiff, 0),
			columnName: "any_column",
		},
		{
			name:       "nil slice",
			diffs:      nil,
			columnName: "any_column",
		},
		{
			name: "empty column name search",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "id",
					Changes: map[string]string{
						"type": "integer -> bigint",
					},
				},
			},
			columnName: "",
		},
		{
			name: "case sensitive search - wrong case",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "Email",
					Changes: map[string]string{
						"type": "varchar -> text",
					},
				},
			},
			columnName: "email", // lowercase, should not match "Email"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.SearchColumnByName(tt.diffs, tt.columnName)

			c.Assert(result, qt.IsNil)
		})
	}
}

func TestColumnByName_EdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		diffs        []difftypes.ColumnDiff
		columnName   string
		expectedDiff *difftypes.ColumnDiff
	}{
		{
			name: "duplicate column names - returns first match",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "duplicate_name",
					Changes: map[string]string{
						"type": "varchar -> text",
					},
				},
				{
					ColumnName: "duplicate_name",
					Changes: map[string]string{
						"nullable": "true -> false",
					},
				},
			},
			columnName: "duplicate_name",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "duplicate_name",
				Changes: map[string]string{
					"type": "varchar -> text",
				},
			},
		},
		{
			name: "column name with special characters",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "column_with_underscore",
					Changes: map[string]string{
						"type": "varchar -> text",
					},
				},
				{
					ColumnName: "column-with-dash",
					Changes: map[string]string{
						"nullable": "true -> false",
					},
				},
				{
					ColumnName: "column.with.dots",
					Changes: map[string]string{
						"unique": "false -> true",
					},
				},
			},
			columnName: "column-with-dash",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "column-with-dash",
				Changes: map[string]string{
					"nullable": "true -> false",
				},
			},
		},
		{
			name: "column name with numbers",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "column123",
					Changes: map[string]string{
						"type": "integer -> bigint",
					},
				},
				{
					ColumnName: "123column",
					Changes: map[string]string{
						"nullable": "true -> false",
					},
				},
			},
			columnName: "123column",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "123column",
				Changes: map[string]string{
					"nullable": "true -> false",
				},
			},
		},
		{
			name: "single character column name",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "a",
					Changes: map[string]string{
						"type": "char -> varchar",
					},
				},
			},
			columnName: "a",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "a",
				Changes: map[string]string{
					"type": "char -> varchar",
				},
			},
		},
		{
			name: "very long column name",
			diffs: []difftypes.ColumnDiff{
				{
					ColumnName: "this_is_a_very_long_column_name_that_might_be_used_in_some_databases_with_descriptive_naming_conventions",
					Changes: map[string]string{
						"type": "text -> longtext",
					},
				},
			},
			columnName: "this_is_a_very_long_column_name_that_might_be_used_in_some_databases_with_descriptive_naming_conventions",
			expectedDiff: &difftypes.ColumnDiff{
				ColumnName: "this_is_a_very_long_column_name_that_might_be_used_in_some_databases_with_descriptive_naming_conventions",
				Changes: map[string]string{
					"type": "text -> longtext",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.SearchColumnByName(tt.diffs, tt.columnName)

			if tt.expectedDiff == nil {
				c.Assert(result, qt.IsNil)
			} else {
				c.Assert(result, qt.IsNotNil)
				c.Assert(result.ColumnName, qt.Equals, tt.expectedDiff.ColumnName)
				c.Assert(result.Changes, qt.HasLen, len(tt.expectedDiff.Changes))
				for key, expectedValue := range tt.expectedDiff.Changes {
					c.Assert(result.Changes[key], qt.Equals, expectedValue)
				}
			}
		})
	}
}

func TestColumnByName_PointerBehavior(t *testing.T) {
	c := qt.New(t)

	// Test that the returned pointer references the original data
	originalDiffs := []difftypes.ColumnDiff{
		{
			ColumnName: "test_column",
			Changes: map[string]string{
				"type": "varchar -> text",
			},
		},
	}

	result := compare.SearchColumnByName(originalDiffs, "test_column")

	c.Assert(result, qt.IsNotNil)
	c.Assert(result.ColumnName, qt.Equals, "test_column")

	// Modify the returned pointer and verify it affects the original slice
	result.Changes["new_change"] = "old -> new"

	c.Assert(originalDiffs[0].Changes["new_change"], qt.Equals, "old -> new")
	c.Assert(originalDiffs[0].Changes, qt.HasLen, 2)
}

func TestTablesAndColumns_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "new table added",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "User", Name: "users"},
				},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
				},
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			database: &catalog.Database{
				Tables: make([]catalog.Table, 0),
			},
			expected: &difftypes.SchemaDiff{
				TablesAdded: []string{"users"},
			},
		},
		{
			name: "table removed",
			desired: &schemamodel.Database{
				Tables:         make([]schemamodel.Table, 0),
				Fields:         make([]schemamodel.Field, 0),
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			database: &catalog.Database{
				Tables: []catalog.Table{
					{Name: "old_table"},
				},
			},
			expected: &difftypes.SchemaDiff{
				TablesRemoved: []string{"old_table"},
			},
		},
		{
			name: "table modified - column added",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "User", Name: "users"},
				},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
					{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false},
				},
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			database: &catalog.Database{
				Tables: []catalog.Table{
					{
						Name: "users",
						Columns: []catalog.Column{
							{Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{
					{
						TableName:    "users",
						ColumnsAdded: difftypes.ColumnChanges{{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false}},
					},
				},
			},
		},
		{
			name: "multiple changes",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Post", Name: "posts"},
				},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
					{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
				},
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			database: &catalog.Database{
				Tables: []catalog.Table{
					{
						Name: "users",
						Columns: []catalog.Column{
							{Name: "id", DataType: "integer", IsPrimaryKey: true},
							{Name: "legacy_field", DataType: "varchar"},
						},
					},
					{Name: "old_table"},
				},
			},
			expected: &difftypes.SchemaDiff{
				TablesAdded:   []string{"posts"},
				TablesRemoved: []string{"old_table"},
				TablesModified: []difftypes.TableDiff{
					{
						TableName:      "users",
						ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy_field", Type: "varchar"}},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.TablesAndColumns(tt.desired, tt.database, diff)

			c.Assert(diff.TablesAdded, qt.DeepEquals, tt.expected.TablesAdded)
			c.Assert(diff.TablesRemoved, qt.DeepEquals, tt.expected.TablesRemoved)
			c.Assert(diff.TablesModified, qt.HasLen, len(tt.expected.TablesModified))

			for i, expectedTableDiff := range tt.expected.TablesModified {
				c.Assert(diff.TablesModified[i].TableName, qt.Equals, expectedTableDiff.TableName)
				c.Assert(diff.TablesModified[i].ColumnsAdded.Names(), qt.DeepEquals, expectedTableDiff.ColumnsAdded.Names())
				c.Assert(diff.TablesModified[i].ColumnsRemoved.Names(), qt.DeepEquals, expectedTableDiff.ColumnsRemoved.Names())
			}
		})
	}
}

func TestTablesAndColumns_UnhappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "empty schemas",
			desired: &schemamodel.Database{
				Tables:         make([]schemamodel.Table, 0),
				Fields:         make([]schemamodel.Field, 0),
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			database: &catalog.Database{
				Tables: make([]catalog.Table, 0),
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "nil embedded fields",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "User", Name: "users"},
				},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
				},
				EmbeddedFields: nil,
			},
			database: &catalog.Database{
				Tables: make([]catalog.Table, 0),
			},
			expected: &difftypes.SchemaDiff{
				TablesAdded: []string{"users"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.TablesAndColumns(tt.desired, tt.database, diff)

			c.Assert(diff.TablesAdded, qt.DeepEquals, tt.expected.TablesAdded)
			c.Assert(diff.TablesRemoved, qt.DeepEquals, tt.expected.TablesRemoved)
			c.Assert(diff.TablesModified, qt.HasLen, len(tt.expected.TablesModified))
		})
	}
}

func TestTableColumns_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		genTable schemamodel.Table
		dbTable  catalog.Table
		desired  *schemamodel.Database
		expected difftypes.TableDiff
	}{
		{
			name:     "column added",
			genTable: schemamodel.Table{StructName: "User", Name: "users"},
			dbTable: catalog.Table{
				Name: "users",
				Columns: []catalog.Column{
					{Name: "id", DataType: "integer", IsPrimaryKey: true},
				},
			},
			desired: &schemamodel.Database{
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
					{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false},
				},
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			expected: difftypes.TableDiff{
				TableName:    "users",
				ColumnsAdded: difftypes.ColumnChanges{{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false}},
			},
		},
		{
			name:     "column removed",
			genTable: schemamodel.Table{StructName: "User", Name: "users"},
			dbTable: catalog.Table{
				Name: "users",
				Columns: []catalog.Column{
					{Name: "id", DataType: "integer", IsPrimaryKey: true},
					{Name: "legacy_field", DataType: "varchar"},
				},
			},
			desired: &schemamodel.Database{
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
				},
				EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
			},
			expected: difftypes.TableDiff{
				TableName:      "users",
				ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy_field", Type: "varchar"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.TableColumns(tt.genTable, tt.dbTable, tt.desired)

			c.Assert(result.TableName, qt.Equals, tt.expected.TableName)
			// Compared by name: these rows are about WHICH columns the
			// comparison reports, and the definitions each one carries are
			// covered where they are rendered (stokaro/ptah#2315).
			c.Assert(result.ColumnsAdded.Names(), qt.DeepEquals, tt.expected.ColumnsAdded.Names())
			c.Assert(result.ColumnsRemoved.Names(), qt.DeepEquals, tt.expected.ColumnsRemoved.Names())
		})
	}
}

func TestTableColumns_WithEmbeddedFields(t *testing.T) {
	c := qt.New(t)

	genTable := schemamodel.Table{StructName: "User", Name: "users"}
	dbTable := catalog.Table{
		Name: "users",
		Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsPrimaryKey: true},
		},
	}

	desired := &schemamodel.Database{
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Timestamps", Name: "created_at", Type: "TIMESTAMP", Nullable: false},
			{StructName: "Timestamps", Name: "updated_at", Type: "TIMESTAMP", Nullable: false},
		},
		EmbeddedFields: []schemamodel.EmbeddedField{
			{
				StructName:       "User",
				Mode:             "inline",
				EmbeddedTypeName: "Timestamps",
			},
		},
	}

	result := compare.TableColumns(genTable, dbTable, desired)

	c.Assert(result.TableName, qt.Equals, "users")
	c.Assert(result.ColumnsAdded.Names(), qt.DeepEquals, []string{"created_at", "updated_at"})
}
