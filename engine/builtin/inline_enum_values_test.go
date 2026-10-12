package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// valuedEnumSchema is the census column-enum fixture: an ENUM column that
// carries its values and names no declared enum.
func valuedEnumSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", FieldName: "S", Name: "s", Type: "ENUM", Nullable: true, Enum: []string{"draft", "live"}},
		},
	}
}

// declaredEnumSchema declares the same values as an enum the column names by
// type, which is how a Go annotation or a YAML schema writes the column.
func declaredEnumSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Enums:  []schemamodel.Enum{{Name: "enum_t_s", Values: []string{"draft", "live"}}},
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", FieldName: "S", Name: "s", Type: "enum_t_s", Nullable: true},
		},
	}
}

// TestGetOrderedCreateStatements_AValuedEnumColumnIsBuiltInline pins the
// column each dialect without standalone enums builds from the values the
// column carries. The comparison lowers the column to the same model
// (schemaprep.LowerInlineEnums), so a render that wrote the bare type ENUM
// built a column MySQL, MariaDB, SQL Server and Oracle refuse, and on SQLite a
// TEXT column without the CHECK the comparison expects.
func TestGetOrderedCreateStatements_AValuedEnumColumnIsBuiltInline(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{
			dialect: platform.MySQL,
			want:    "-- MYSQL TABLE: t --\nCREATE TABLE `t` (\n  `id` BIGINT PRIMARY KEY,\n  `s` ENUM('draft', 'live')\n);\n\n",
		},
		{
			dialect: platform.MariaDB,
			want:    "-- MARIADB TABLE: t --\nCREATE TABLE `t` (\n  `id` BIGINT PRIMARY KEY,\n  `s` ENUM('draft', 'live')\n);\n\n",
		},
		{
			dialect: platform.SQLite,
			want:    "CREATE TABLE \"t\" (\n  \"id\" BIGINT NOT NULL PRIMARY KEY,\n  \"s\" TEXT CHECK (s IN ('draft', 'live'))\n);\n",
		},
		{
			dialect: platform.SQLServer,
			want:    "CREATE TABLE [t] (\n  [id] BIGINT PRIMARY KEY,\n  [s] NVARCHAR(255) CONSTRAINT [t_s_check] CHECK ([s] IN ('draft', 'live'))\n);\n",
		},
		{
			dialect: platform.Oracle,
			want:    "CREATE TABLE t (\n  id NUMBER(19) NOT NULL PRIMARY KEY,\n  s VARCHAR2(255) CONSTRAINT t_s_check CHECK (s IN ('draft', 'live'))\n);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(valuedEnumSchema(), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

// TestGetOrderedCreateStatements_AValuedEnumColumnMatchesADeclaredEnum is the
// control on the same dialects: a column carrying its values and a column
// naming a declared enum with those values build one table.
func TestGetOrderedCreateStatements_AValuedEnumColumnMatchesADeclaredEnum(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			valued, err := builtin.GetOrderedCreateStatements(valuedEnumSchema(), dialect)
			c.Assert(err, qt.IsNil)
			declared, err := builtin.GetOrderedCreateStatements(declaredEnumSchema(), dialect)
			c.Assert(err, qt.IsNil)

			c.Assert(valued, qt.DeepEquals, declared)
		})
	}
}
