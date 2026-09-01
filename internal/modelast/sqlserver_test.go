package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/modelast"
)

func TestFromField_SQLServerTypeConversions(t *testing.T) {
	tests := []struct {
		name     string
		field    schemamodel.Field
		expected string
	}{
		{
			name:     "serial",
			field:    schemamodel.Field{Name: "id", Type: "SERIAL", AutoInc: true},
			expected: "INT",
		},
		{
			name:     "bigserial",
			field:    schemamodel.Field{Name: "id", Type: "BIGSERIAL", AutoInc: true},
			expected: "BIGINT",
		},
		{
			name:     "text",
			field:    schemamodel.Field{Name: "body", Type: "TEXT"},
			expected: "NVARCHAR(MAX)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			column := modelast.FromField(tt.field, nil, platform.SQLServer)

			c.Assert(column.Type, qt.Equals, tt.expected)
		})
	}
}

func TestFromField_SQLServerEnumUsesTextWithCheck(t *testing.T) {
	c := qt.New(t)

	field := schemamodel.Field{Name: "status", Type: "enum_status"}
	enums := []schemamodel.Enum{{Name: "enum_status", Values: []string{"active", "blocked"}}}

	column := modelast.FromField(field, enums, platform.SQLServer)

	c.Assert(column.Type, qt.Equals, "NVARCHAR(255)")
	c.Assert(column.Check, qt.Equals, "[status] IN ('active', 'blocked')")
}

func TestCollectDatabase_SQLServerIncludesViewsAndTriggers(t *testing.T) {
	c := qt.New(t)

	database := schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Schema: "dbo", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true, AutoInc: true},
		},
		Views: []schemamodel.View{{
			Name: "dbo.active_users",
			Body: "SELECT [id] FROM [dbo].[users]",
		}},
		Triggers: []schemamodel.Trigger{{
			Name:  "dbo.tr_users_touch",
			Table: "dbo.users",
			Event: "UPDATE",
			Body:  "AS SELECT 1",
		}},
	}

	statements := modelast.CollectDatabase(database, platform.SQLServer)
	sql, err := renderer.RenderSQL(platform.SQLServer, statements)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE VIEW [dbo].[active_users] AS")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER [dbo].[tr_users_touch] ON [dbo].[users] AFTER UPDATE AS SELECT 1;")
}
