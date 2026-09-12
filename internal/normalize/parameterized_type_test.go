package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

// TestType_ParametersAreNotTypeNames pins the rule that separates a type's name
// from its argument list.
//
// Every family arm in Type recognizes a type by a substring, so a token that
// names a type somewhere else is dangerous inside a parameter list. Oracle
// writes a vector's element format in one -- VECTOR(512, INT8) -- and reading
// the whole string answered "integer" for the column. Measured against Oracle
// Free 23.26.3.0.0, varying only the declared side while the database column
// stayed a VECTOR, the comparator's desired type was "integer" for every
// spelling carrying INT8 and the declared vector for every other element
// format, so the dimension, the spacing and the storage form had nothing to do
// with it.
//
// The alias rows are the control. INT8, INT4, FLOAT8 and BIGSERIAL are real
// type names on their own and still fold to the family they belong to, so the
// rule cannot be satisfied by folding nothing.
func TestType_ParametersAreNotTypeNames(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"vector of int8", "VECTOR(512, INT8)", "vector(512, int8)"},
		{"vector of int8 without a space", "VECTOR(512,INT8)", "vector(512,int8)"},
		{"vector of int8 with a small dimension", "VECTOR(8, INT8)", "vector(8, int8)"},
		{"vector of int8 with no dimension", "VECTOR(*, INT8)", "vector(*, int8)"},
		{"vector of int8 stored dense", "VECTOR(512, INT8, DENSE)", "vector(512, int8, dense)"},
		{"vector of int4", "VECTOR(512, INT4)", "vector(512, int4)"},
		{"vector of float8", "VECTOR(512, FLOAT8)", "vector(512, float8)"},
		{"parameter spelled serial", "VECTOR(3, SERIAL)", "vector(3, serial)"},
		{"parameter spelled text", "VECTOR(3, TEXT)", "vector(3, text)"},
		{"vector of float32", "VECTOR(512, FLOAT32)", "vector(512, float32)"},
		{"vector of float32 stored sparse", "VECTOR(512, FLOAT32, SPARSE)", "vector(512, float32, sparse)"},
		{"vector of binary", "VECTOR(512, BINARY)", "vector(512, binary)"},

		{"int8 names a type on its own", "INT8", "integer"},
		{"int4 names a type on its own", "INT4", "integer"},
		{"float8 names a type on its own", "FLOAT8", "double precision"},
		{"bigserial names a type on its own", "BIGSERIAL", "integer"},
		{"a sized varchar keeps folding", "VARCHAR(255)", "varchar"},
		{"a sized tinyint keeps folding", "TINYINT(1)", "boolean"},
		{"a sized numeric keeps folding", "NUMERIC(10,2)", "decimal"},
		{"a sized timestamp keeps folding", "TIMESTAMP(6) WITH TIME ZONE", "timestamp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(normalize.Type(tt.input), qt.Equals, tt.expected)
		})
	}
}
