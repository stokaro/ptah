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
