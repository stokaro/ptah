package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// CockroachDB builds a column declared INT or INTEGER with the width the
// session's default_int_size names, 8 unless the session sets it. A declared
// `integer` is compared as that width, so it matches the INT8 the same
// statement builds, and a width the declaration or the table does not share is
// still a change (stokaro/ptah#3922).

// integerColumn is a desired schema whose table t holds one column n of the
// declared type.
func integerColumn(declared string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "bigint", Primary: true},
			{StructName: "T", Name: "n", Type: declared, Nullable: true},
		},
	}
}

// builtIntegerColumn is a read of t whose column n the server built as the
// named type, as CockroachDB's catalog spells it.
func builtIntegerColumn(built string) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "t",
		Type: "BASE TABLE",
		Columns: []catalog.Column{
			{Name: "id", UDTName: "int8", IsPrimaryKey: true},
			{Name: "n", UDTName: built, IsNullable: "YES"},
		},
	}}}
}

// TestCompareWithDatabaseInfo_CockroachIntegerWidth_HappyPath compares a
// declared integer against the width the server gives it.
func TestCompareWithDatabaseInfo_CockroachIntegerWidth_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		dialect        string
		defaultIntSize int
		declared       string
		built          string
	}{
		{name: "integer at the default width", dialect: "cockroachdb", defaultIntSize: 8, declared: "integer", built: "int8"},
		{name: "int at the default width", dialect: "cockroachdb", defaultIntSize: 8, declared: "int", built: "int8"},
		{name: "upper case", dialect: "cockroachdb", defaultIntSize: 8, declared: "INTEGER", built: "int8"},
		{name: "a width no connection read", dialect: "cockroachdb", declared: "int", built: "int8"},
		{name: "a session at four bytes", dialect: "cockroachdb", defaultIntSize: 4, declared: "integer", built: "int4"},
		{name: "a declared width", dialect: "cockroachdb", defaultIntSize: 8, declared: "int4", built: "int4"},
		{name: "postgres keeps four bytes", dialect: "postgres", declared: "integer", built: "int4"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				integerColumn(test.declared), builtIntegerColumn(test.built),
				catalog.ServerInfo{Dialect: test.dialect, Schema: "public", DefaultIntSize: test.defaultIntSize}, nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_CockroachIntegerWidth_FailurePath keeps a width
// the declaration and the table do not share a change: a session at one width
// against a table built at the other, a declared width, and PostgreSQL, where
// INTEGER is four bytes whatever the session says.
func TestCompareWithDatabaseInfo_CockroachIntegerWidth_FailurePath(t *testing.T) {
	tests := []struct {
		name           string
		dialect        string
		defaultIntSize int
		declared       string
		built          string
		want           string
	}{
		{
			name: "a table built at four bytes", dialect: "cockroachdb", defaultIntSize: 8,
			declared: "integer", built: "int4", want: "int4 -> int8",
		},
		{
			name: "a session at four bytes", dialect: "cockroachdb", defaultIntSize: 4,
			declared: "int", built: "int8", want: "int8 -> int4",
		},
		{
			name: "a declared width", dialect: "cockroachdb", defaultIntSize: 8,
			declared: "int4", built: "int8", want: "int8 -> int4",
		},
		{
			name: "postgres", dialect: "postgres", defaultIntSize: 8,
			declared: "integer", built: "int8", want: "int8 -> integer",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				integerColumn(test.declared), builtIntegerColumn(test.built),
				catalog.ServerInfo{Dialect: test.dialect, Schema: "public", DefaultIntSize: test.defaultIntSize}, nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals, map[string]string{"type": test.want})
		})
	}
}
