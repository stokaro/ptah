package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// TestCompareTypeCapacity_ALowerCapacityNarrows plans a type the catalog holds against a
// declaration, in the catalog's spelling, and reads what the comparison answers about it.
// Each destructive row planned the same statement rated safe. Each widening stands for the
// false rating in the other direction, which stops a plan as well.
func TestCompareTypeCapacity_ALowerCapacityNarrows(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		held        string
		declared    string
		wantNarrows bool
	}{
		{"PostgreSQL double to real", "postgres", "double precision", "real", true},
		{"PostgreSQL real to double", "postgres", "real", "double precision", false},
		{"PostgreSQL a fraction to an integer", "postgres", "numeric(10,2)", "integer", true},
		{"PostgreSQL an integer to digits", "postgres", "integer", "numeric(12,0)", false},
		{"PostgreSQL an integer past a float", "postgres", "bigint", "double precision", true},
		{"PostgreSQL varying bits down", "postgres", "bit varying(8)", "bit varying(4)", true},
		{"PostgreSQL an array of doubles", "postgres", "double precision[]", "real[]", true},
		{"CockroachDB STRING down", "cockroachdb", "STRING(100)", "string(5)", true},
		{"CockroachDB STRING up", "cockroachdb", "STRING(5)", "string(100)", false},
		{"CockroachDB float down", "cockroachdb", "float8", "float4", true},
		{"YugabyteDB double to real", "yugabytedb", "double precision", "real", true},
		{"Spanner double to real", "spanner", "double precision", "real", true},
		{"MySQL longtext to text", "mysql", "longtext", "text", true},
		{"MySQL text to longtext", "mysql", "text", "longtext", false},
		{"MySQL mediumtext to text", "mysql", "mediumtext", "text", true},
		{"MySQL longblob to blob", "mysql", "longblob", "blob", true},
		{"MySQL blob to tinyblob", "mysql", "blob", "tinyblob", true},
		{"MySQL double to float", "mysql", "double", "float", true},
		{"MySQL float to double", "mysql", "float", "double", false},
		{"MySQL a sign lost", "mysql", "int", "int unsigned", true},
		{"MySQL half the range lost", "mysql", "int unsigned", "int", true},
		{"MySQL unsigned to wider", "mysql", "int unsigned", "bigint", false},
		{"MySQL a member lost", "mysql", "enum('a','b')", "enum('a')", true},
		{"MySQL a member added", "mysql", "enum('a')", "enum('a','b')", false},
		{"MariaDB longtext to text", "mariadb", "longtext", "text", true},
		{"MariaDB double to float", "mariadb", "double", "float", true},
		{"SQL Server MAX to a length", "sqlserver", "VARBINARY(MAX)", "varbinary(50)", true},
		{"SQL Server a length to MAX", "sqlserver", "VARBINARY(50)", "varbinary(max)", false},
		{"SQL Server money to smallmoney", "sqlserver", "MONEY", "smallmoney", true},
		{"SQL Server float to real", "sqlserver", "FLOAT", "real", true},
		{"SQL Server real to float", "sqlserver", "REAL", "float", false},
		{"Oracle double to single", "oracle", "BINARY_DOUBLE", "BINARY_FLOAT", true},
		{"Oracle a portable real", "oracle", "BINARY_DOUBLE", "real", true},
		{"Oracle a portable double", "oracle", "BINARY_FLOAT", "double precision", false},
		{"Oracle RAW down", "oracle", "RAW(10)", "RAW(5)", true},
		{"Oracle BLOB to RAW", "oracle", "BLOB", "RAW(50)", true},
		{"Oracle FLOAT down", "oracle", "FLOAT(10)", "FLOAT(5)", true},
		{"ClickHouse Float64 to Float32", "clickhouse", "Float64", "Float32", true},
		{"ClickHouse Float32 to Float64", "clickhouse", "Float32", "Float64", false},
		{"ClickHouse String to FixedString", "clickhouse", "String", "FixedString(2)", true},
		{"ClickHouse FixedString to String", "clickhouse", "FixedString(2)", "String", false},
		{"ClickHouse Decimal64 to Decimal32", "clickhouse", "Decimal64(2)", "Decimal32(2)", true},
		{"ClickHouse Decimal32 to Decimal64", "clickhouse", "Decimal32(2)", "Decimal64(2)", false},
		{"ClickHouse nullable floats", "clickhouse", "Nullable(Float64)", "Nullable(Float32)", true},
		{"ClickHouse an integer past a float", "clickhouse", "Int64", "Float64", true},
		{"ClickHouse a member lost", "clickhouse", "Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 1)", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.Changes["type"], qt.Not(qt.Equals), "")
			c.Assert(column.TypeNarrows, qt.Equals, test.wantNarrows)
			c.Assert(column.PreviousType, qt.Equals, test.held)
		})
	}
}

// TestCompareTypeCapacity_OneTierInTwoSpellings holds a length MySQL builds a tier for to
// that tier. MySQL reads the tier back, so a plan here runs on every run.
func TestCompareTypeCapacity_OneTierInTwoSpellings(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		held     string
		declared string
	}{
		{"MySQL tinytext", "mysql", "tinytext", "text(60)"},
		{"MySQL mediumtext", "mysql", "mediumtext", "text(70000)"},
		{"MySQL tinyblob", "mysql", "tinyblob", "blob(100)"},
		{"MySQL blob", "mysql", "blob", "blob(65535)"},
		{"MariaDB mediumtext", "mariadb", "mediumtext", "text(70000)"},
		{"MariaDB tinyblob", "mariadb", "tinyblob", "blob(100)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompareTypeCapacity_ALengthATierHasNoRoomFor reads `text(65535)` as the mediumtext
// the server builds for it, since 65535 characters may take four bytes each. Asked by
// name alone, the comparison reported nothing.
func TestCompareTypeCapacity_ALengthATierHasNoRoomFor(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration("text(65535)"),
		heldTemporalColumn("text"), "mysql", builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	column := diff.TablesModified[0].ColumnsModified[0]
	c.Assert(column.Changes["type"], qt.Not(qt.Equals), "")
	c.Assert(column.TypeNarrows, qt.IsFalse)
}
