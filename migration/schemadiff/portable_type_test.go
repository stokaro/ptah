package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

// heldSQLServerColumn is a read of t.c on SQL Server, whose reader marks every type as
// the text the catalog holds.
func heldSQLServerColumn(columnType string) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "t", Type: "BASE TABLE",
		Columns: []catalog.Column{{Name: "c", DataType: columnType, ColumnType: columnType, IsNullable: "YES", TypeIsDeclaredText: true}},
	}}}
}

// comparedSQLServerType compares declared with a read of a column of type held.
func comparedSQLServerType(c *qt.C, declared schemamodel.Field, held string) *difftypes.SchemaDiff {
	c.Helper()
	declared.StructName, declared.Name, declared.Nullable = "T", "c", true
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "T", Name: "t"}}, Fields: []schemamodel.Field{declared}}
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, heldSQLServerColumn(held), "sqlserver", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	return diff
}

// TestCompareSQLServerType_ANameAndTheTypeItWritesAreOneType holds the column Ptah builds
// from a portable name, and the column the server builds from a name it also takes as its
// own. A row that plans here plans a statement that changes nothing, on every run.
func TestCompareSQLServerType_ANameAndTheTypeItWritesAreOneType(t *testing.T) {
	tests := []struct {
		name     string
		held     string
		declared string
	}{
		{"timestamp", "DATETIME2", "timestamp"},
		{"timestamp in upper case", "DATETIME2", "TIMESTAMP"},
		{"timestamp without a zone", "DATETIME2", "timestamp without time zone"},
		{"timestamp with digits it does not write", "DATETIME2", "timestamp(3)"},
		{"timestamptz", "DATETIMEOFFSET", "timestamptz"},
		{"timestamp with a zone", "DATETIMEOFFSET", "timestamp with time zone"},
		{"boolean", "BIT", "boolean"},
		{"bool", "BIT", "bool"},
		{"text", "NVARCHAR(MAX)", "text"},
		{"text in upper case", "NVARCHAR(MAX)", "TEXT"},
		{"citext", "NVARCHAR(MAX)", "citext"},
		{"bytea", "VARBINARY(MAX)", "bytea"},
		{"blob", "VARBINARY(MAX)", "blob"},
		{"double precision", "FLOAT", "double precision"},
		{"integer", "INT", "integer"},
		{"int4", "INT", "int4"},
		{"char", "NCHAR(1)", "char"},
		{"char with a length", "NCHAR(5)", "char(5)"},
		{"character with a length", "NCHAR(5)", "character(5)"},
		{"varchar in lower case", "NVARCHAR(MAX)", "varchar"},
		{"varchar in upper case", "NVARCHAR(MAX)", "VARCHAR"},
		{"varchar with a length", "NVARCHAR(50)", "varchar(50)"},
		{"varchar MAX", "NVARCHAR(MAX)", "varchar(max)"},
		{"character varying", "NVARCHAR(50)", "character varying(50)"},
		{"the server's text", "TEXT", "text"},
		{"the server's char", "CHAR(1)", "char"},
		{"the server's char with a length", "CHAR(5)", "character(5)"},
		{"the server's varchar", "VARCHAR(1)", "varchar"},
		{"the server's varchar with a length", "VARCHAR(50)", "character varying(50)"},
		{"the server's varchar MAX", "VARCHAR(MAX)", "varchar(max)"},
		{"the server's timestamp", "TIMESTAMP", "timestamp"},
		{"a name the renderer leaves alone", "NVARCHAR(50)", "nvarchar(50)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := comparedSQLServerType(c, schemamodel.Field{Type: test.declared}, test.held)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompareSQLServerType_ARealDifferenceIsAChange holds the guard the other way: a column
// that is neither what the renderer writes nor what the server builds from the word. Each
// is rated by what the renderer writes, as the statement is.
func TestCompareSQLServerType_ARealDifferenceIsAChange(t *testing.T) {
	tests := []struct {
		name        string
		held        string
		declared    string
		wantNarrows bool
	}{
		{"fewer digits under timestamp", "DATETIME2(3)", "timestamp", false},
		{"an older type under timestamp", "DATETIME", "timestamp", false},
		{"an offset under timestamp", "DATETIMEOFFSET", "timestamp", true},
		{"fewer digits under timestamptz", "DATETIMEOFFSET(3)", "timestamptz", false},
		{"no offset under timestamptz", "DATETIME2", "timestamptz", false},
		{"a length under text", "NVARCHAR(50)", "text", false},
		{"a byte string under text", "VARCHAR(MAX)", "text", false},
		{"a byte string under citext", "VARCHAR(MAX)", "citext", false},
		{"a Unicode LOB under text", "NTEXT", "text", false},
		{"a number under text", "INT", "text", false},
		{"an integer under boolean", "INT", "boolean", false},
		{"a tinyint under boolean", "TINYINT", "boolean", false},
		{"a length under bytea", "VARBINARY(50)", "bytea", false},
		{"a fixed length under bytea", "BINARY(16)", "bytea", false},
		{"a real under double precision", "REAL", "double precision", false},
		{"a bigint under integer", "BIGINT", "integer", true},
		{"a smallint under integer", "SMALLINT", "integer", false},
		{"a longer length under char", "NCHAR(10)", "char(5)", true},
		{"a shorter length under char", "NCHAR(5)", "char(10)", false},
		{"a longer length under varchar", "NVARCHAR(50)", "varchar(20)", true},
		{"MAX under varchar", "NVARCHAR(MAX)", "varchar(50)", true},
		{"one character under lowercase varchar", "NVARCHAR(1)", "varchar", false},
		{"one character under the exact VARCHAR", "NVARCHAR(1)", "VARCHAR", false},
		{"a longer length of the server's varchar", "VARCHAR(50)", "varchar(20)", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := comparedSQLServerType(c, schemamodel.Field{Type: test.declared}, test.held)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.Changes["type"], qt.Not(qt.Equals), "")
			c.Assert(column.TypeNarrows, qt.Equals, test.wantNarrows)
		})
	}
}

// TestCompareSQLServerType_ATypeNamedForTheTargetIsComparedAsNamed holds the two forms the
// renderer writes as they stand. Read through the portable mapping, `sql("TEXT")` would
// match the NVARCHAR(MAX) it is not.
func TestCompareSQLServerType_ATypeNamedForTheTargetIsComparedAsNamed(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
		held  string
	}{
		{"sql text over the Unicode type", schemamodel.Field{Type: "TEXT", TypeRawSQL: true}, "NVARCHAR(MAX)"},
		{"sql datetime over the portable timestamp", schemamodel.Field{Type: "TIMESTAMP", TypeRawSQL: true}, "DATETIME2"},
		{"a catalog's text over the Unicode type", schemamodel.Field{Type: "text", TypeIsDeclaredText: true}, "NVARCHAR(MAX)"},
		{"sql char over the Unicode type", schemamodel.Field{Type: "CHAR(5)", TypeRawSQL: true}, "NCHAR(5)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := comparedSQLServerType(c, test.field, test.held)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Not(qt.Equals), "")
		})
	}
}

// sqlServerFile is a schema file declaring t.c as the field.
func sqlServerFile(field schemamodel.Field) *schemamodel.Database {
	field.StructName, field.Name, field.Nullable = "T", "c", true
	return &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "T", Name: "t"}}, Fields: []schemamodel.Field{field}}
}

// TestCompareSQLServerType_TwoFilesCompareAsTheyWrite compares two schema files, where the
// current side is a declaration rather than a read. A row that plans here plans a statement
// that writes what the first file already wrote.
func TestCompareSQLServerType_TwoFilesCompareAsTheyWrite(t *testing.T) {
	tests := []struct {
		name    string
		from    schemamodel.Field
		to      schemamodel.Field
		wantLen int
	}{
		{name: "a name against itself", from: schemamodel.Field{Type: "boolean"}, to: schemamodel.Field{Type: "boolean"}},
		{name: "text against itself", from: schemamodel.Field{Type: "text"}, to: schemamodel.Field{Type: "text"}},
		{
			name: "a type named with sql against itself",
			from: schemamodel.Field{Type: "TEXT", TypeRawSQL: true}, to: schemamodel.Field{Type: "TEXT", TypeRawSQL: true},
		},
		{name: "a name against the type it writes", from: schemamodel.Field{Type: "boolean"}, to: schemamodel.Field{Type: "bit"}},
		{name: "the type a name writes against the name", from: schemamodel.Field{Type: "bit"}, to: schemamodel.Field{Type: "boolean"}},
		{name: "a timestamp against the type it writes", from: schemamodel.Field{Type: "timestamp"}, to: schemamodel.Field{Type: "datetime2"}},
		{name: "a real change between two names", from: schemamodel.Field{Type: "timestamp"}, to: schemamodel.Field{Type: "timestamptz"}, wantLen: 1},
		{name: "a real change to a length", from: schemamodel.Field{Type: "text"}, to: schemamodel.Field{Type: "varchar(50)"}, wantLen: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			current, err := goschematodb.ToDBSchema(c.Context(), sqlServerFile(test.from), "sqlserver", runtime)
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDialect(c.Context(), sqlServerFile(test.to), current, "sqlserver", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, test.wantLen)
		})
	}
}
