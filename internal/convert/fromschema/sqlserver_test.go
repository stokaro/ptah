package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/convert/fromschema"
)

func TestFromField_SQLServerTypeConversions(t *testing.T) {
	tests := []struct {
		name     string
		field    goschema.Field
		expected string
	}{
		{
			name:     "serial",
			field:    goschema.Field{Name: "id", Type: "SERIAL", AutoInc: true},
			expected: "INT",
		},
		{
			name:     "bigserial",
			field:    goschema.Field{Name: "id", Type: "BIGSERIAL", AutoInc: true},
			expected: "BIGINT",
		},
		{
			name:     "text",
			field:    goschema.Field{Name: "body", Type: "TEXT"},
			expected: "NVARCHAR(MAX)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			column := fromschema.FromField(tt.field, nil, platform.SQLServer)

			c.Assert(column.Type, qt.Equals, tt.expected)
		})
	}
}

func TestFromField_SQLServerEnumUsesTextWithCheck(t *testing.T) {
	c := qt.New(t)

	field := goschema.Field{Name: "status", Type: "enum_status"}
	enums := []goschema.Enum{{Name: "enum_status", Values: []string{"active", "blocked"}}}

	column := fromschema.FromField(field, enums, platform.SQLServer)

	c.Assert(column.Type, qt.Equals, "NVARCHAR(255)")
	c.Assert(column.Check, qt.Equals, "[status] IN ('active', 'blocked')")
}

func TestFromDatabase_SQLServerIncludesViewsAndTriggers(t *testing.T) {
	c := qt.New(t)

	database := goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "dbo", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true, AutoInc: true},
		},
		Views: []goschema.View{{
			Name: "dbo.active_users",
			Body: "SELECT [id] FROM [dbo].[users]",
		}},
		Triggers: []goschema.Trigger{{
			Name:  "dbo.tr_users_touch",
			Table: "dbo.users",
			Event: "UPDATE",
			Body:  "AS SELECT 1",
		}},
	}

	statements := fromschema.FromDatabase(database, platform.SQLServer)
	sql, err := renderer.RenderSQL(platform.SQLServer, statements)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE VIEW [dbo].[active_users] AS")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER [dbo].[tr_users_touch] ON [dbo].[users] AFTER UPDATE AS SELECT 1;")
}
