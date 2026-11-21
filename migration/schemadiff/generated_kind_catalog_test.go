package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

// A generated column declared without a kind is created with the target's
// default kind, and on ClickHouse one declared STORED or VIRTUAL is created
// MATERIALIZED or ALIAS. A catalog converted from the declaration, as a schema
// file compared with another file is, states the kind as declared, and the
// comparison reads both sides as the kind the target creates, so the
// declaration compares equal to its own catalog. Read on the declared side
// alone, each such column plans a change of kind.
func TestCompareGeneratedKind_MatchesTheCatalogConvertedFromIt(t *testing.T) {
	tests := []struct {
		dialect string
		engine  string
		typ     string
		kind    string
	}{
		{dialect: platform.ClickHouse, engine: "MergeTree", typ: "Int64", kind: "STORED"},
		{dialect: platform.ClickHouse, engine: "MergeTree", typ: "Int64", kind: "VIRTUAL"},
		{dialect: platform.ClickHouse, engine: "MergeTree", typ: "Int64", kind: ""},
		{dialect: platform.Postgres, typ: "BIGINT", kind: ""},
		{dialect: platform.MySQL, typ: "BIGINT", kind: ""},
		{dialect: platform.SQLServer, typ: "BIGINT", kind: "STORED"},
		{dialect: platform.SQLServer, typ: "BIGINT", kind: "VIRTUAL"},
		{dialect: platform.SQLServer, typ: "BIGINT", kind: ""},
	}
	for _, test := range tests {
		t.Run(test.dialect+" kind "+test.kind, func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			desired := schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t", Engine: test.engine}},
				Fields: []schemamodel.Field{
					{StructName: "T", FieldName: "ID", Name: "id", Type: test.typ, Primary: true},
					{StructName: "T", FieldName: "A", Name: "a", Type: test.typ},
					{StructName: "T", FieldName: "M", Name: "m", Type: test.typ, GeneratedExpression: "a * 2", GeneratedKind: test.kind},
				},
			}
			current, err := goschematodb.ToDBSchema(c.Context(), &desired, test.dialect, runtime)
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &desired, current,
				catalog.ServerInfo{Dialect: test.dialect}, nil, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// A catalog a server reported states the kind it read. PostgreSQL 18 builds a
// VIRTUAL column, and a declaration that names no kind, or STORED, is a column
// of another kind; one that names VIRTUAL is the same column. A read that names
// no kind is left with none: filling it with the target's default would compare
// a column the reader cannot classify equal to the declaration whatever the
// server built. SQL Server reports PERSISTED for a stored computed column and
// VIRTUAL for one computed on read; a declaration that names no kind, or
// STORED, is created PERSISTED there.
func TestCompareGeneratedKind_ReadsAServersKindAsReported(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		schema   string
		read     string
		declared string
		modified int
	}{
		{name: "a virtual column declared with no kind", dialect: platform.Postgres, schema: "public", read: "VIRTUAL", declared: "", modified: 1},
		{name: "a virtual column declared stored", dialect: platform.Postgres, schema: "public", read: "VIRTUAL", declared: "STORED", modified: 1},
		{name: "a virtual column declared virtual", dialect: platform.Postgres, schema: "public", read: "VIRTUAL", declared: "VIRTUAL", modified: 0},
		{name: "a stored column declared with no kind", dialect: platform.Postgres, schema: "public", read: "STORED", declared: "", modified: 0},
		{name: "a column read with no kind declared with none", dialect: platform.Postgres, schema: "public", read: "", declared: "", modified: 1},
		{name: "a column read with no kind declared stored", dialect: platform.Postgres, schema: "public", read: "", declared: "STORED", modified: 1},
		{name: "a sql server column computed on read declared with no kind", dialect: platform.SQLServer, schema: "dbo", read: "VIRTUAL", declared: "", modified: 1},
		{name: "a sql server column computed on read declared stored", dialect: platform.SQLServer, schema: "dbo", read: "VIRTUAL", declared: "STORED", modified: 1},
		{name: "a sql server column computed on read declared virtual", dialect: platform.SQLServer, schema: "dbo", read: "VIRTUAL", declared: "VIRTUAL", modified: 0},
		{name: "a persisted sql server column declared with no kind", dialect: platform.SQLServer, schema: "dbo", read: "PERSISTED", declared: "", modified: 0},
		{name: "a persisted sql server column declared stored", dialect: platform.SQLServer, schema: "dbo", read: "PERSISTED", declared: "STORED", modified: 0},
		{name: "a persisted sql server column declared persisted", dialect: platform.SQLServer, schema: "dbo", read: "PERSISTED", declared: "PERSISTED", modified: 0},
		{name: "a persisted sql server column declared virtual", dialect: platform.SQLServer, schema: "dbo", read: "PERSISTED", declared: "VIRTUAL", modified: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Fields: []schemamodel.Field{
					{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
					{StructName: "T", FieldName: "A", Name: "a", Type: "BIGINT", Nullable: true},
					{StructName: "T", FieldName: "M", Name: "m", Type: "BIGINT", Nullable: true, GeneratedExpression: "a * 2", GeneratedKind: test.declared},
				},
			}
			expression := "a * 2"
			read := &catalog.Database{Tables: []catalog.Table{{Name: "t", Schema: test.schema, Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "a", DataType: "bigint", UDTName: "int8", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "m", DataType: "bigint", UDTName: "int8", IsNullable: "YES", OrdinalPosition: 3, GeneratedExpression: &expression, GeneratedKind: test.read},
			}}}}

			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &desired, read,
				catalog.ServerInfo{Dialect: test.dialect}, nil, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, test.modified)
		})
	}
}

// SQL Server derives a computed column's type and nullability from its
// expression, and its DDL states neither, so on a column generated on both
// sides neither is compared: measured on SQL Server 2022, `CASE WHEN a > 0
// THEN 'p' ELSE 'n' END` declared NVARCHAR(1) and nullable is a VARCHAR(1)
// NOT NULL column, and the ALTER COLUMN a comparison plans for it is refused
// with Msg 4928. An ordinary column on SQL Server, and a generated column on
// PostgreSQL, whose DDL states the type, are still compared. The live column
// is VARCHAR(1) NOT NULL, and each row declares another type, another
// nullability, or both.
func TestCompareGeneratedColumn_LeavesADerivedTypeUncompared(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		schema     string
		kind       string
		expression string
		typ        string
		nullable   bool
		modified   int
	}{
		{name: "a sql server computed column of another type and nullability", dialect: platform.SQLServer, schema: "dbo",
			kind: "VIRTUAL", expression: "a * 2", typ: "NVARCHAR(1)", nullable: true, modified: 0},
		{name: "a sql server computed column of another type", dialect: platform.SQLServer, schema: "dbo",
			kind: "PERSISTED", expression: "a * 2", typ: "INT", modified: 0},
		{name: "a sql server computed column of another nullability", dialect: platform.SQLServer, schema: "dbo",
			kind: "VIRTUAL", expression: "a * 2", typ: "VARCHAR(1)", nullable: true, modified: 0},
		{name: "a sql server ordinary column of another type", dialect: platform.SQLServer, schema: "dbo", typ: "INT", modified: 1},
		{name: "a sql server ordinary column of another nullability", dialect: platform.SQLServer, schema: "dbo",
			typ: "VARCHAR(1)", nullable: true, modified: 1},
		{name: "a postgres generated column", dialect: platform.Postgres, schema: "public",
			kind: "STORED", expression: "a * 2", typ: "NVARCHAR(1)", nullable: true, modified: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Fields: []schemamodel.Field{
					{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
					{StructName: "T", FieldName: "A", Name: "a", Type: "BIGINT", Nullable: true},
					{StructName: "T", FieldName: "M", Name: "m", Type: test.typ, Nullable: test.nullable,
						GeneratedExpression: test.expression, GeneratedKind: test.kind},
				},
			}
			read := &catalog.Database{Tables: []catalog.Table{{Name: "t", Schema: test.schema, Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "a", DataType: "bigint", UDTName: "int8", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "m", DataType: "VARCHAR", ColumnType: "VARCHAR(1)", IsNullable: "NO", OrdinalPosition: 3,
					GeneratedExpression: nonEmpty(test.expression), GeneratedKind: test.kind},
			}}}}

			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &desired, read,
				catalog.ServerInfo{Dialect: test.dialect}, nil, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, test.modified)
		})
	}
}

// nonEmpty is the address of expression, or nil for an empty one, as a reader
// reports an ordinary column.
func nonEmpty(expression string) *string {
	if expression == "" {
		return nil
	}
	return &expression
}
