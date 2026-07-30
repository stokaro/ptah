package protobufrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

// typeCase pins one row of the documented type-mapping table. want is the whole
// generated declaration, so the repeated modifier is covered by the same string
// and no branching is needed inside the test body.
type typeCase struct {
	columnType string
	want       string
}

func assertColumnType(c *qt.C, tc typeCase) {
	c.Helper()
	text := mustRenderText(c, oneTable(column("v", tc.columnType)), baseOptions())
	c.Assert(section(text, "message Thing {"), qt.Equals, "message Thing {\n  "+tc.want+"\n}")
}

func runTypeCases(t *testing.T, cases []typeCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.columnType, func(t *testing.T) {
			assertColumnType(qt.New(t), tc)
		})
	}
}

func TestTypeMapIntegers(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"SMALLINT", "int32 v = 1;"},
		{"SMALLSERIAL", "int32 v = 1;"},
		{"SERIAL2", "int32 v = 1;"},
		{"INT2", "int32 v = 1;"},
		{"TINYINT", "int32 v = 1;"},
		{"YEAR", "int32 v = 1;"},
		{"INT", "int32 v = 1;"},
		{"INTEGER", "int32 v = 1;"},
		{"INT4", "int32 v = 1;"},
		{"SERIAL", "int32 v = 1;"},
		{"SERIAL4", "int32 v = 1;"},
		{"MEDIUMINT", "int32 v = 1;"},
		{"BIGINT", "int64 v = 1;"},
		{"BIGSERIAL", "int64 v = 1;"},
		{"SERIAL8", "int64 v = 1;"},
		{"INT8", "int64 v = 1;"},
	})
}

func TestTypeMapUnsignedIsDetectedOnTheRawType(t *testing.T) {
	// NormalizeType strips the UNSIGNED modifier, so the mapping has to look at
	// the raw spelling; the lowercase MySQL form must work too.
	runTypeCases(t, []typeCase{
		{"INT UNSIGNED", "uint32 v = 1;"},
		{"int unsigned", "uint32 v = 1;"},
		{"SMALLINT UNSIGNED", "uint32 v = 1;"},
		{"BIGINT UNSIGNED", "uint64 v = 1;"},
		{"bigint unsigned", "uint64 v = 1;"},
		// UNSIGNED is meaningless on a non-integer and must not rewrite it.
		{"VARCHAR(20) UNSIGNED", "string v = 1;"},
	})
}

func TestTypeMapBooleansAndFloats(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"BOOL", "bool v = 1;"},
		{"BOOLEAN", "bool v = 1;"},
		{"REAL", "float v = 1;"},
		{"FLOAT4", "float v = 1;"},
		{"DOUBLE", "double v = 1;"},
		{"DOUBLE PRECISION", "double v = 1;"},
		{"FLOAT", "double v = 1;"},
		{"FLOAT8", "double v = 1;"},
	})
}

func TestTypeMapExactNumericsBecomeStrings(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"DECIMAL(10,2)", "string v = 1;"},
		{"NUMERIC", "string v = 1;"},
		{"MONEY", "string v = 1;"},
	})
}

func TestTypeMapStringSpellings(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"VARCHAR(255)", "string v = 1;"},
		{"CHARACTER VARYING(255)", "string v = 1;"},
		{"CHAR(2)", "string v = 1;"},
		{"CHARACTER(2)", "string v = 1;"},
		{"BPCHAR", "string v = 1;"},
		{"NCHAR(2)", "string v = 1;"},
		{"NVARCHAR(64)", "string v = 1;"},
		{"TEXT", "string v = 1;"},
		{"LONGTEXT", "string v = 1;"},
		{"MEDIUMTEXT", "string v = 1;"},
		{"TINYTEXT", "string v = 1;"},
		{"CLOB", "string v = 1;"},
		{"CITEXT", "string v = 1;"},
		{"UUID", "string v = 1;"},
		{"INET", "string v = 1;"},
		{"CIDR", "string v = 1;"},
		{"MACADDR", "string v = 1;"},
		{"MACADDR8", "string v = 1;"},
	})
}

func TestTypeMapBinaries(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"BYTEA", "bytes v = 1;"},
		{"BLOB", "bytes v = 1;"},
		{"LONGBLOB", "bytes v = 1;"},
		{"MEDIUMBLOB", "bytes v = 1;"},
		{"TINYBLOB", "bytes v = 1;"},
		{"BINARY(16)", "bytes v = 1;"},
		{"VARBINARY(16)", "bytes v = 1;"},
		{"BIT", "bytes v = 1;"},
	})
}

func TestTypeMapWellKnownTypes(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"JSON", "google.protobuf.Value v = 1;"},
		{"JSONB", "google.protobuf.Value v = 1;"},
		// Timestamp is reserved for types with explicit time-zone semantics.
		{"TIMESTAMPTZ", "google.protobuf.Timestamp v = 1;"},
		{"TIMESTAMP WITH TIME ZONE", "google.protobuf.Timestamp v = 1;"},
	})
}

func TestTypeMapTemporalsWithoutTimeZoneStayStrings(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"TIMESTAMP", "string v = 1;"},
		{"DATETIME", "string v = 1;"},
		{"TIMESTAMP WITHOUT TIME ZONE", "string v = 1;"},
		{"DATE", "string v = 1;"},
		{"TIME", "string v = 1;"},
		{"TIMETZ", "string v = 1;"},
		{"TIME WITH TIME ZONE", "string v = 1;"},
		{"TIME WITHOUT TIME ZONE", "string v = 1;"},
	})
}

func TestTypeMapUnknownTypeFallsBackToString(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"WIDGET", "string v = 1;"},
		{"geometry(Point,4326)", "string v = 1;"},
	})
}

func TestTypeMapArraysBecomeRepeated(t *testing.T) {
	runTypeCases(t, []typeCase{
		{"TEXT[]", "repeated string v = 1;"},
		{"INTEGER[]", "repeated int32 v = 1;"},
		{"BIGINT[]", "repeated int64 v = 1;"},
		{"JSONB[]", "repeated google.protobuf.Value v = 1;"},
		{"TIMESTAMPTZ[]", "repeated google.protobuf.Timestamp v = 1;"},
		{"WIDGET[]", "repeated string v = 1;"},
	})
}

func TestTypeMapImportsOnlyWhatItUses(t *testing.T) {
	c := qt.New(t)

	plain := mustRenderText(c, oneTable(column("v", "TEXT")), baseOptions())
	c.Assert(plain, qt.Not(qt.Contains), "import ")

	structOnly := mustRenderText(c, oneTable(column("v", "JSONB")), baseOptions())
	c.Assert(structOnly, qt.Contains, "import \"google/protobuf/struct.proto\";\n")
	c.Assert(structOnly, qt.Not(qt.Contains), "timestamp.proto")

	both := mustRenderText(c, oneTable(column("a", "JSONB"), column("b", "TIMESTAMPTZ")), baseOptions())
	c.Assert(both, qt.Contains,
		"import \"google/protobuf/struct.proto\";\nimport \"google/protobuf/timestamp.proto\";\n")
}

// diagnosticCase pins one lossy or unrecognized mapping to its warning.
type diagnosticCase struct {
	columnType string
	want       string
}

func assertMappingDiagnostic(c *qt.C, dc diagnosticCase) {
	c.Helper()
	res := mustRender(c, oneTable(column("v", dc.columnType)), baseOptions())
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains), "warning things.v: "+dc.want)
}

func TestTypeMapLossyProjectionsAreReported(t *testing.T) {
	cases := []diagnosticCase{
		{"DECIMAL(10,2)", "exact numeric mapped to string; Protobuf has no decimal type and float/double would lose precision"},
		{"NUMERIC", "exact numeric mapped to string; Protobuf has no decimal type and float/double would lose precision"},
		{"MONEY", "exact numeric mapped to string; Protobuf has no decimal type and float/double would lose precision"},
		{"TIMESTAMP", "timezone-ambiguous timestamp mapped to string; google.protobuf.Timestamp is only used for types with explicit time-zone semantics"},
		{"DATETIME", "timezone-ambiguous timestamp mapped to string; google.protobuf.Timestamp is only used for types with explicit time-zone semantics"},
		{"DATE", "date/time mapped to string; Protobuf has no wire-native equivalent"},
		{"TIME", "date/time mapped to string; Protobuf has no wire-native equivalent"},
		{"TIMETZ", "date/time mapped to string; Protobuf has no wire-native equivalent"},
		{"WIDGET", `column type "WIDGET" is not recognized and was exported as string`},
		{"WIDGET[]", `column type "WIDGET[]" is not recognized and was exported as string`},
	}
	for _, dc := range cases {
		t.Run(dc.columnType, func(t *testing.T) {
			assertMappingDiagnostic(qt.New(t), dc)
		})
	}
}

func TestTypeMapFaithfulProjectionsAreSilent(t *testing.T) {
	c := qt.New(t)

	res := mustRender(c, oneTable(
		column("a", "BIGINT"),
		column("b", "TEXT"),
		column("c", "BOOLEAN"),
		column("d", "BYTEA"),
		column("e", "JSONB"),
		column("f", "TIMESTAMPTZ"),
	), baseOptions())

	// Only the bootstrap warning; none of these mappings is lossy.
	c.Assert(res.Diagnostics, qt.HasLen, 1)
	c.Assert(res.Diagnostics[0].Path, qt.Equals, testOutPath)
}

func TestTypeMapNullableArrayReportsTheNullVersusEmptyLoss(t *testing.T) {
	c := qt.New(t)

	res := mustRender(c, oneTable(goschema.Field{Name: "tags", Type: "TEXT[]", Nullable: true}), baseOptions())
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		"warning things.tags: nullable array column exported as repeated; "+
			"protobuf cannot distinguish SQL NULL from an empty list")

	// A NOT NULL array carries no such ambiguity and must stay silent.
	notNull := mustRender(c, oneTable(column("tags", "TEXT[]")), baseOptions())
	c.Assert(notNull.Diagnostics, qt.HasLen, 1)
}

func TestTypeMapNeverEmitsPresenceModifiers(t *testing.T) {
	c := qt.New(t)

	// Editions default features.field_presence to EXPLICIT, and both "optional"
	// and "required" are hard parse errors there. SQL NOT NULL is simply lossy.
	text := mustRenderText(c, oneTable(
		goschema.Field{Name: "a", Type: "TEXT", Nullable: true},
		goschema.Field{Name: "b", Type: "TEXT", Nullable: false},
	), baseOptions())

	c.Assert(text, qt.Not(qt.Contains), "optional ")
	c.Assert(text, qt.Not(qt.Contains), "required ")
	c.Assert(section(text, "message Thing {"), qt.Equals,
		"message Thing {\n  string a = 1;\n  string b = 2;\n}")
}
