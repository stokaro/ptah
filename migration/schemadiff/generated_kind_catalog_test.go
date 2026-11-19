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
// server built. SQL Server reports no kind for a column that is not PERSISTED,
// and a declaration that names no kind is created PERSISTED there, so the two
// differ.
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
		{name: "a sql server column that is not persisted declared with no kind", dialect: platform.SQLServer, schema: "dbo", read: "", declared: "", modified: 1},
		{name: "a persisted sql server column declared with no kind", dialect: platform.SQLServer, schema: "dbo", read: "PERSISTED", declared: "", modified: 0},
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
