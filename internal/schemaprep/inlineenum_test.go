package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// TestLowerInlineEnums covers what each dialect without standalone enums
// builds for an enum column, and that a column naming a declared enum by type
// lowers like one carrying its values inline.
func TestLowerInlineEnums(t *testing.T) {
	enums := []schemamodel.Enum{{Name: "status", Values: []string{"a", "b"}}}
	tests := []struct {
		name      string
		dialect   string
		field     schemamodel.Field
		wantType  string
		wantCheck string
	}{
		{
			name: "mysql inline values", dialect: "mysql",
			field:    schemamodel.Field{Name: "s", Type: "ENUM", Enum: []string{"a", "it's"}},
			wantType: "enum('a','it''s')",
		},
		{
			name: "mariadb named enum", dialect: "mariadb",
			field:    schemamodel.Field{Name: "s", Type: "status"},
			wantType: "enum('a','b')",
		},
		{
			name: "sqlite beside its own check", dialect: "sqlite",
			field:    schemamodel.Field{Name: "s", Type: "ENUM", Enum: []string{"a"}, Check: "s <> ''"},
			wantType: "TEXT", wantCheck: "(s <> '') AND s IN ('a')",
		},
		{
			name: "sqlserver", dialect: "sqlserver",
			field:    schemamodel.Field{Name: "s]x", Type: "ENUM", Enum: []string{"a", "b"}},
			wantType: "NVARCHAR(255)", wantCheck: "[s]]x] IN ('a', 'b')",
		},
		{
			name: "oracle", dialect: "oracle",
			field:    schemamodel.Field{Name: "s", Type: "status"},
			wantType: "VARCHAR2(255)", wantCheck: "s IN ('a', 'b')",
		},
		{
			name: "a standalone-enum dialect", dialect: "postgres",
			field:    schemamodel.Field{Name: "s", Type: "status"},
			wantType: "status",
		},
		{
			name: "a column without values", dialect: "mysql",
			field:    schemamodel.Field{Name: "s", Type: "VARCHAR(8)"},
			wantType: "VARCHAR(8)",
		},
		{
			name: "values beside another type", dialect: "mysql",
			field:    schemamodel.Field{Name: "s", Type: "VARCHAR(8)", Enum: []string{"a"}},
			wantType: "VARCHAR(8)",
		},
		{
			name: "a lowered column", dialect: "sqlite",
			field:    schemamodel.Field{Name: "s", Type: "TEXT", Enum: []string{"a"}, Check: "s IN ('a')"},
			wantType: "TEXT", wantCheck: "s IN ('a')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fields := []schemamodel.Field{test.field}

			lowered := schemaprep.LowerInlineEnums(fields, enums, test.dialect)

			c.Assert(lowered, qt.HasLen, 1)
			c.Assert(lowered[0].Type, qt.Equals, test.wantType)
			c.Assert(lowered[0].Check, qt.Equals, test.wantCheck)
			c.Assert(fields[0], qt.DeepEquals, test.field)
		})
	}
}

// TestInlineEnumValues covers which columns an inline-enum dialect builds as
// enums, and from which values. The renderer and LowerInlineEnums both ask it,
// so a column the one builds is the column the other describes.
func TestInlineEnumValues(t *testing.T) {
	enums := []schemamodel.Enum{{Name: "status", Values: []string{"a", "b"}}}
	tests := []struct {
		name    string
		dialect string
		field   schemamodel.Field
		want    []string
	}{
		{
			name: "values on the column", dialect: "mysql",
			field: schemamodel.Field{Name: "s", Type: "ENUM", Enum: []string{"draft", "live"}},
			want:  []string{"draft", "live"},
		},
		{
			name: "a declared enum named by type", dialect: "sqlite",
			field: schemamodel.Field{Name: "s", Type: "status"},
			want:  []string{"a", "b"},
		},
		{
			name: "values on the column beside a declared enum", dialect: "oracle",
			field: schemamodel.Field{Name: "s", Type: "status", Enum: []string{"c"}},
			want:  []string{"c"},
		},
		{
			name: "a standalone-enum dialect", dialect: "postgres",
			field: schemamodel.Field{Name: "s", Type: "ENUM", Enum: []string{"draft", "live"}},
			want:  nil,
		},
		{
			name: "a column without values", dialect: "mariadb",
			field: schemamodel.Field{Name: "s", Type: "VARCHAR(8)"},
			want:  nil,
		},
		{
			name: "values beside another type", dialect: "mysql",
			field: schemamodel.Field{Name: "s", Type: "VARCHAR(8)", Enum: []string{"a"}},
			want:  nil,
		},
		{
			name: "a lowered column", dialect: "mysql",
			field: schemamodel.Field{Name: "s", Type: "enum('draft','live')", Enum: []string{"draft", "live"}},
			want:  nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values := schemaprep.InlineEnumValues(test.field, enums, test.dialect)

			c.Assert(values, qt.DeepEquals, test.want)
		})
	}
}

// TestEnumColumnValues covers which columns are enum columns on any target,
// with which values, and whether the column names a declared enum. A column
// with values and no declaration is the shape a target without inline enums
// refuses.
func TestEnumColumnValues(t *testing.T) {
	enums := []schemamodel.Enum{{Name: "status", Values: []string{"a", "b"}}}
	tests := []struct {
		name         string
		field        schemamodel.Field
		wantValues   []string
		wantDeclared bool
	}{
		{
			name:       "values on a column typed ENUM",
			field:      schemamodel.Field{Name: "s", Type: "ENUM", Enum: []string{"draft", "live"}},
			wantValues: []string{"draft", "live"},
		},
		{
			name:       "values on a column typed enum in lower case",
			field:      schemamodel.Field{Name: "s", Type: "enum", Enum: []string{"draft"}},
			wantValues: []string{"draft"},
		},
		{
			name:         "a declared enum named by type",
			field:        schemamodel.Field{Name: "s", Type: "status"},
			wantValues:   []string{"a", "b"},
			wantDeclared: true,
		},
		{
			name:         "values on the column beside a declared enum",
			field:        schemamodel.Field{Name: "s", Type: "status", Enum: []string{"c"}},
			wantValues:   []string{"c"},
			wantDeclared: true,
		},
		{
			name:  "a column typed ENUM without values",
			field: schemamodel.Field{Name: "s", Type: "ENUM"},
		},
		{
			name:  "values beside another type",
			field: schemamodel.Field{Name: "s", Type: "VARCHAR(8)", Enum: []string{"a"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, declared := schemaprep.EnumColumnValues(test.field, enums)

			c.Assert(values, qt.DeepEquals, test.wantValues)
			c.Assert(declared, qt.Equals, test.wantDeclared)
		})
	}
}
