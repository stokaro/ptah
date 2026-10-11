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
