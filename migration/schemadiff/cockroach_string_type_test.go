package schemadiff_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// CockroachDB names its string and byte types STRING and BYTES and its catalog
// reports them as text and bytea, so a column declared in CockroachDB's own
// spelling is compared as the catalog reports it. A sized STRING(n) is a text
// column with a width, which the reader spells STRING(n), and a width the two
// sides do not share is still a change (stokaro/ptah#4059).

// stringColumn is a desired schema whose table t holds one column s of the
// declared type.
func stringColumn(declared string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "bigint", Primary: true},
			{StructName: "T", Name: "s", Type: declared, Nullable: true},
		},
	}
}

// builtStringColumn is a read of t whose column s is built as CockroachDB's
// catalog reports it, with the spelling the reader gives a sized string.
func builtStringColumn(built catalog.Column) *catalog.Database {
	built.Name = "s"
	built.IsNullable = "YES"
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "t",
		Type: "BASE TABLE",
		Columns: []catalog.Column{
			{Name: "id", UDTName: "int8", IsPrimaryKey: true},
			built,
		},
	}}}
}

// sizedString is a STRING(n) column as the reader reports it.
func sizedString(width int) catalog.Column {
	return catalog.Column{DataType: "text", UDTName: "text", FormattedType: fmt.Sprintf("STRING(%d)", width), CharacterMaxLength: &width}
}

// TestCompareWithDatabaseInfo_CockroachStringType_HappyPath compares a column
// declared in CockroachDB's spelling against the type the server built for it.
func TestCompareWithDatabaseInfo_CockroachStringType_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		built    catalog.Column
	}{
		{name: "STRING", declared: "STRING", built: catalog.Column{DataType: "text", UDTName: "text"}},
		{name: "lower case", declared: "string", built: catalog.Column{DataType: "text", UDTName: "text"}},
		{name: "STRING[]", declared: "STRING[]", built: catalog.Column{DataType: "ARRAY", UDTName: "_text", FormattedType: "text[]"}},
		{name: "BYTES", declared: "BYTES", built: catalog.Column{DataType: "bytea", UDTName: "bytea"}},
		{name: "STRING(10)", declared: "STRING(10)", built: sizedString(10)},
		{name: "text stays text", declared: "text", built: catalog.Column{DataType: "text", UDTName: "text"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				stringColumn(test.declared), builtStringColumn(test.built),
				catalog.ServerInfo{Dialect: "cockroachdb", Schema: "public"}, nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_CockroachStringType_FailurePath keeps a type the
// declaration and the column do not share a change: a width that differs, a
// width on one side only, another type, and PostgreSQL, which has no STRING.
func TestCompareWithDatabaseInfo_CockroachStringType_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		declared string
		built    catalog.Column
	}{
		{name: "a wider STRING", dialect: "cockroachdb", declared: "STRING(20)", built: sizedString(10)},
		{name: "an unbounded STRING against a width", dialect: "cockroachdb", declared: "STRING", built: sizedString(10)},
		{name: "a width against an unbounded column", dialect: "cockroachdb", declared: "STRING(10)", built: catalog.Column{DataType: "text", UDTName: "text"}},
		{name: "BYTES against text", dialect: "cockroachdb", declared: "BYTES", built: catalog.Column{DataType: "text", UDTName: "text"}},
		{name: "STRING[] against a scalar", dialect: "cockroachdb", declared: "STRING[]", built: catalog.Column{DataType: "bytea", UDTName: "bytea"}},
		{name: "postgres has no STRING", dialect: "postgres", declared: "STRING", built: catalog.Column{DataType: "text", UDTName: "text"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				stringColumn(test.declared), builtStringColumn(test.built),
				catalog.ServerInfo{Dialect: test.dialect, Schema: "public"}, nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Not(qt.Equals), "")
		})
	}
}
