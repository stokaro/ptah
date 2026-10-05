package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestColumnsWithDialect_YDBTypes_HappyPath pins the type pairs that are one
// column on YDB. Each catalog type on the right is what local-ydb read back for
// the declaration on the left, on a line with the 64-bit date types (26.2.1.14)
// or without them (25.1.4.7): a declared length is not kept, and a table built
// on either line answers the same declaration.
func TestColumnsWithDialect_YDBTypes_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		catalog  string
	}{
		{name: "a varchar length YDB does not keep", declared: "VARCHAR(255)", catalog: "Utf8"},
		{name: "text", declared: "TEXT", catalog: "Utf8"},
		{name: "an instant built on 26.2", declared: "TIMESTAMP", catalog: "Timestamp64"},
		{name: "an instant built on 25.1", declared: "TIMESTAMP", catalog: "Timestamp"},
		{name: "a date built on 25.1", declared: "DATE", catalog: "Date"},
		{name: "jsonb", declared: "JSONB", catalog: "JsonDocument"},
		{name: "a decimal", declared: "DECIMAL(10,2)", catalog: "Decimal(10,2)"},
		{name: "a bigint", declared: "BIGINT", catalog: "Int64"},
		{name: "a native spelling in another case", declared: "uint64", catalog: "Uint64"},
		{name: "two declarations of one type", declared: "VARCHAR(255)", catalog: "TEXT"},
		{name: "a declared instant on both sides", declared: "TIMESTAMP", catalog: "DATETIME"},
		// A vector is bytes in a String column, which reads back as a plain
		// String; its dimension is the vector index's to keep.
		{name: "a vector built as bytes", declared: "vector(1536)", catalog: "String"},
		{name: "a vector without a dimension", declared: "VECTOR", catalog: "String"},
		{name: "two vector declarations of different dimensions", declared: "vector(4)", catalog: "vector(3)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "c", Type: test.declared, Nullable: true},
				catalog.Column{Name: "c", DataType: test.catalog, ColumnType: test.catalog, IsNullable: "YES"},
				platform.YDB,
			)

			c.Assert(diff.Changes, qt.HasLen, 0)
		})
	}
}

// TestColumnsWithDialect_YDBTypes_FailurePath pins the pairs that are a change:
// a different precision, a narrow type where the declaration names the wide
// one, and a different family. YDB changes none of them in place, so the
// planner refuses each, and the comparison has to report it for that refusal
// to happen.
func TestColumnsWithDialect_YDBTypes_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		catalog  string
		want     string
	}{
		{name: "a decimal precision", declared: "DECIMAL(12,2)", catalog: "Decimal(10,2)", want: "Decimal(10,2) -> Decimal(12,2)"},
		{name: "the narrow type where the wide one is declared", declared: "Timestamp64", catalog: "Timestamp", want: "Timestamp -> Timestamp64"},
		{name: "a wider integer", declared: "BIGINT", catalog: "Int32", want: "Int32 -> Int64"},
		{name: "bytes where text is declared", declared: "TEXT", catalog: "String", want: "String -> Utf8"},
		{name: "a declared 64-bit integer against YDB's 8-bit one", declared: "INT8", catalog: "Int8", want: "Int8 -> Int64"},
		{name: "two declarations of different types", declared: "BIGINT", catalog: "INTEGER", want: "Int32 -> Int64"},
		{name: "a vector where text was built", declared: "vector(3)", catalog: "Utf8", want: "Utf8 -> String"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "c", Type: test.declared, Nullable: true},
				catalog.Column{Name: "c", DataType: test.catalog, ColumnType: test.catalog, IsNullable: "YES"},
				platform.YDB,
			)

			c.Assert(diff.Changes["type"], qt.Equals, test.want)
		})
	}
}

// TestColumnsWithDialect_YDBKeyIsNotNull pins the key rule both ways. The YDB
// renderer writes NOT NULL on every key column, so a key declared without
// not_null matches the column Ptah built; a key the server reports nullable,
// built outside Ptah, is a difference, which the planner then refuses because
// YDB has no SET NOT NULL.
func TestColumnsWithDialect_YDBKeyIsNotNull(t *testing.T) {
	tests := []struct {
		name       string
		dbNullable string
		want       map[string]string
	}{
		{name: "a key built by Ptah", dbNullable: "NO", want: make(map[string]string)},
		{name: "a nullable key built elsewhere", dbNullable: "YES", want: map[string]string{"nullable": "true -> false"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "id", Type: "BIGINT", Primary: true, Nullable: true},
				catalog.Column{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: test.dbNullable, IsPrimaryKey: true},
				platform.YDB,
			)

			c.Assert(diff.Changes, qt.DeepEquals, test.want)
		})
	}
}

// TestColumnsWithDialect_YDBSerialIsNotNull pins the Serial rule both ways. The
// YDB renderer writes NOT NULL on every Serial column, key or not, so one
// declared without not_null matches the column Ptah built; a nullable column
// the server reports is still a difference.
func TestColumnsWithDialect_YDBSerialIsNotNull(t *testing.T) {
	tests := []struct {
		name       string
		field      schemamodel.Field
		dbNullable string
		want       map[string]string
	}{
		{name: "a serial type built by Ptah", field: schemamodel.Field{Name: "n", Type: "BIGSERIAL", Nullable: true},
			dbNullable: "NO", want: make(map[string]string)},
		{name: "an auto-increment built by Ptah", field: schemamodel.Field{Name: "n", Type: "BIGINT", AutoInc: true, Nullable: true},
			dbNullable: "NO", want: make(map[string]string)},
		{name: "a serial the server reports nullable", field: schemamodel.Field{Name: "n", Type: "BIGSERIAL", Nullable: true},
			dbNullable: "YES", want: map[string]string{"nullable": "true -> false"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				test.field,
				catalog.Column{Name: "n", DataType: "BigSerial", ColumnType: "BigSerial", IsNullable: test.dbNullable, IsAutoIncrement: true},
				platform.YDB,
			)

			c.Assert(diff.Changes, qt.DeepEquals, test.want)
		})
	}
}

// TestColumnsWithDialect_YDBIncrementingIntegerIsASerial pins how an
// incrementing integer compares. The renderer writes a declared auto_increment
// BIGINT as BigSerial, and the other side may report the same column as a
// BigSerial, as an incrementing Int64, or, in a file-to-file comparison, as
// the declaration it was converted from. Each is one column.
func TestColumnsWithDialect_YDBIncrementingIntegerIsASerial(t *testing.T) {
	tests := []struct {
		name    string
		field   schemamodel.Field
		catalog catalog.Column
		want    map[string]string
	}{
		{name: "a serial against an incrementing integer",
			field:   schemamodel.Field{Name: "n", Type: "BIGSERIAL"},
			catalog: catalog.Column{Name: "n", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsAutoIncrement: true},
			want:    make(map[string]string)},
		{name: "an auto-increment against its own declaration",
			field:   schemamodel.Field{Name: "n", Type: "BIGINT", AutoInc: true},
			catalog: catalog.Column{Name: "n", DataType: "BIGINT", ColumnType: "BIGINT", IsNullable: "NO", IsAutoIncrement: true},
			want:    make(map[string]string)},
		{name: "a plain integer against an incrementing one",
			field:   schemamodel.Field{Name: "n", Type: "BIGINT"},
			catalog: catalog.Column{Name: "n", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsAutoIncrement: true},
			want:    map[string]string{"type": "BigSerial -> Int64"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(test.field, test.catalog, platform.YDB)

			c.Assert(diff.Changes, qt.DeepEquals, test.want)
		})
	}
}

// ydbIndex is the catalog side of one YDB index "i" on table "t".
func ydbIndex(method string, unique bool, columns, cover []string) catalog.Index {
	return catalog.Index{Name: "i", TableName: "t", Method: method, IsUnique: unique, Columns: columns, IncludeColumns: cover}
}

// ydbDeclaredIndex is the desired side of one index "i" on table "t".
func ydbDeclaredIndex(method string, unique bool, columns, cover []string) schemamodel.Index {
	return schemamodel.Index{Name: "i", TableName: "t", Type: method, Unique: unique, Fields: columns, IncludeColumns: cover}
}

// TestIndexesWithDialect_YDBRebuildsAChangedIndex pins the properties YDB fixes
// when it builds an index: its columns, its covered columns, its uniqueness
// and whether it is maintained synchronously. A difference in any of them is a
// DROP INDEX and an ADD INDEX of the same name.
func TestIndexesWithDialect_YDBRebuildsAChangedIndex(t *testing.T) {
	tests := []struct {
		name     string
		desired  schemamodel.Index
		database catalog.Index
	}{
		{name: "made asynchronous", desired: ydbDeclaredIndex("async", false, []string{"a"}, nil),
			database: ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)},
		{name: "made unique", desired: ydbDeclaredIndex("", true, []string{"a"}, nil),
			database: ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)},
		{name: "a key column added", desired: ydbDeclaredIndex("", false, []string{"a", "b"}, nil),
			database: ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)},
		{name: "a covered column added", desired: ydbDeclaredIndex("", false, []string{"a"}, []string{"c"}),
			database: ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)},
		{name: "an access method nothing reads", desired: ydbDeclaredIndex("hash", false, []string{"a"}, nil),
			database: ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(
				&schemamodel.Database{Indexes: []schemamodel.Index{test.desired}},
				&catalog.Database{Indexes: []catalog.Index{test.database}},
				diff, platform.YDB,
			)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "i", TableName: "t"}})
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "i", TableName: "t"}})
		})
	}
}

// TestIndexesWithDialect_YDBKeepsAnUnchangedIndex is the control: the same
// index spelled the way a declaration writes it and the way the catalog does
// is one index, and a declared BTREE is the synchronous index YDB builds.
func TestIndexesWithDialect_YDBKeepsAnUnchangedIndex(t *testing.T) {
	tests := []struct {
		name     string
		desired  schemamodel.Index
		database catalog.Index
	}{
		{name: "a plain index", desired: ydbDeclaredIndex("", false, []string{"a"}, nil),
			database: ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)},
		{name: "a declared btree", desired: ydbDeclaredIndex("btree", false, []string{"a"}, nil),
			database: ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)},
		{name: "an asynchronous covering index", desired: ydbDeclaredIndex("async", false, []string{"a"}, []string{"c"}),
			database: ydbIndex("GLOBAL ASYNC", false, []string{"a"}, []string{"c"})},
		{name: "a unique index", desired: ydbDeclaredIndex("", true, []string{"a"}, nil),
			database: ydbIndex("GLOBAL SYNC", true, []string{"a"}, nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(
				&schemamodel.Database{Indexes: []schemamodel.Index{test.desired}},
				&catalog.Database{Indexes: []catalog.Index{test.database}},
				diff, platform.YDB,
			)

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

// TestColumnsWithDialect_YDBDefaults_HappyPath pins the declared defaults that
// are the default a YDB catalog reports. The reader reports a stored default
// as the literal ydbtype writes for it, and the declaration is written through
// the same function in the column's type, so spellings of one value agree.
func TestColumnsWithDialect_YDBDefaults_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		declared    string
		declaration string
		catalog     string
		stored      *string
	}{
		{name: "a text value", declared: "VARCHAR(20)", declaration: "active", catalog: "Utf8", stored: new("'active'u")},
		{name: "a quoted SQL literal", declared: "TEXT", declaration: "'it''s'", catalog: "Utf8", stored: new(`'it\'s'u`)},
		{name: "an integer with a sign", declared: "SMALLINT", declaration: "+5", catalog: "Int16", stored: new("5s")},
		{name: "a boolean from a digit", declared: "BOOLEAN", declaration: "1", catalog: "Bool", stored: new("true")},
		{name: "an instant on a line with the wide types", declared: "TIMESTAMP",
			declaration: "2026-01-02 03:04:05", catalog: "Timestamp64", stored: new("Timestamp64('2026-01-02T03:04:05Z')")},
		{name: "an instant on a line without them", declared: "TIMESTAMP",
			declaration: "2026-01-02 03:04:05", catalog: "Timestamp", stored: new("Timestamp('2026-01-02T03:04:05Z')")},
		{name: "an interval in hours", declared: "INTERVAL", declaration: "PT26H", catalog: "Interval64",
			stored: new("Interval64('P1DT2H')")},
		{name: "a decimal with a trailing zero", declared: "DECIMAL(10,2)", declaration: "12.50",
			catalog: "Decimal(10,2)", stored: new("Decimal('12.5', 10, 2)")},
		{name: "a NULL default writes none", declared: "TEXT", declaration: "NULL", catalog: "Utf8", stored: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "c", Type: test.declared, Nullable: true, Default: test.declaration},
				catalog.Column{Name: "c", DataType: test.catalog, ColumnType: test.catalog, IsNullable: "YES",
					ColumnDefault: test.stored},
				platform.YDB,
			)

			c.Assert(diff.Changes, qt.HasLen, 0)
		})
	}
}

// TestColumnsWithDialect_YDBDefaults_FailurePath pins the pairs that differ.
func TestColumnsWithDialect_YDBDefaults_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		stored      *string
		want        string
	}{
		{name: "another value", declaration: "inactive", stored: new("'active'u"), want: "'active'u -> inactive"},
		{name: "a default the catalog does not have", declaration: "active", stored: nil, want: " -> active"},
		{name: "a default the declaration drops", declaration: "", stored: new("'active'u"), want: "'active'u -> "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "c", Type: "TEXT", Nullable: true, Default: test.declaration},
				catalog.Column{Name: "c", DataType: "Utf8", ColumnType: "Utf8", IsNullable: "YES",
					ColumnDefault: test.stored},
				platform.YDB,
			)

			c.Assert(diff.Changes, qt.DeepEquals, map[string]string{"default": test.want})
		})
	}
}

// A Serial column takes its value from its sequence, so the catalog reports no
// default for it and a declaration naming none is not a change.
func TestColumnsWithDialect_YDBSerialHasNoDefault(t *testing.T) {
	c := qt.New(t)

	diff := compare.ColumnsWithDialect(
		schemamodel.Field{Name: "id", Type: "BIGINT", AutoInc: true, Primary: true},
		catalog.Column{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO",
			IsPrimaryKey: true, IsAutoIncrement: true},
		platform.YDB,
	)

	c.Assert(diff.Changes, qt.HasLen, 0)
}
