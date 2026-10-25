package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
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
// (schemaprep.LowerInlineEnums), so the bare type ENUM, which each of these
// servers refuses, or SQLite's TEXT without the CHECK, would not converge.
// Spanner names the CHECK the way the comparison expects, because the server
// names an unnamed one CK_<table>_<hash>_<n>.
func TestGetOrderedCreateStatements_AValuedEnumColumnIsBuiltInline(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{
			dialect: platform.MySQL,
			want:    "CREATE TABLE `t` (\n  `id` BIGINT PRIMARY KEY,\n  `s` ENUM('draft', 'live')\n);\n\n",
		},
		{
			dialect: platform.MariaDB,
			want:    "CREATE TABLE `t` (\n  `id` BIGINT PRIMARY KEY,\n  `s` ENUM('draft', 'live')\n);\n\n",
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
		{
			dialect: platform.ClickHouse,
			want:    "CREATE TABLE t (\n  id Int64,\n  s Nullable(Enum8('draft' = 1, 'live' = 2))\n) ENGINE = MergeTree ORDER BY (id);\n\n",
		},
		{
			dialect: platform.Spanner,
			want: "CREATE TABLE \"t\" (\n  \"id\" BIGINT PRIMARY KEY NOT NULL,\n" +
				"  \"s\" TEXT CONSTRAINT \"t_s_check\" CHECK (s IN ('draft', 'live'))\n);\n\n",
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
	for _, dialect := range []string{
		platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle,
		platform.ClickHouse, platform.Spanner,
	} {
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

// TestGetOrderedCreateStatements_AValuedEnumColumnWithNoInlineFormIsRefused_FailurePath
// covers the PostgreSQL family, which cannot build the column from its values.
// Rendered as it stands it is the bare type ENUM, which each server refuses at
// apply time, so the render refuses it first and says what to declare instead.
func TestGetOrderedCreateStatements_AValuedEnumColumnWithNoInlineFormIsRefused_FailurePath(t *testing.T) {
	const prefix = `column "s" of table "t" is typed ENUM with the values \("draft", "live"\) and names no declared enum, which `
	const namedEnum = `declare a named enum with these values \(//ptah:schema:enum in Go, an enums entry in YAML, ` +
		`an enum block in HCL\) and type the column with its name`
	tests := []struct {
		dialect string
		wantErr string
	}{
		{dialect: platform.Postgres, wantErr: prefix + `postgres cannot create: ` + namedEnum},
		{dialect: platform.CockroachDB, wantErr: prefix + `cockroachdb cannot create: ` + namedEnum},
		{dialect: platform.YugabyteDB, wantErr: prefix + `yugabytedb cannot create: ` + namedEnum},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(valuedEnumSchema(), test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestGetOrderedCreateStatements_ADeclaredEnumIsNotRefused is the control on
// the PostgreSQL family: the same values declared as a named enum render the
// type and the column that names it.
func TestGetOrderedCreateStatements_ADeclaredEnumIsNotRefused(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(declaredEnumSchema(), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, `CREATE TYPE "enum_t_s" AS ENUM ('draft', 'live');`)
		})
	}
}

// TestValidateSchema_AValuedEnumColumnWithNoInlineFormIsRefused_FailurePath
// pins that validation, which the comparison runs on the declared side before
// planning, refuses the column the render refuses.
func TestValidateSchema_AValuedEnumColumnWithNoInlineFormIsRefused_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := builtin.ValidateSchema(valuedEnumSchema(), platform.Postgres)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `column "s" of table "t" is typed ENUM .* which postgres cannot create: declare a named enum .*`)
}
