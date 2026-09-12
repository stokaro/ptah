package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestColumnsWithDialect_ParameterizedTypeKeepsItsParameters drives the
// comparator over a declared type whose parameter list carries a token that
// names a type elsewhere.
//
// The reported change is where the normalized view of each side shows: for a
// dialect other than SQLite the comparator prints what it decided on rather
// than the raw spellings. Measured against Oracle Free 23.26.3.0.0, with the
// database column left a VECTOR and only the declaration varied, every
// declaration carrying INT8 was answered "integer" -- so the element format
// decided the whole column's type, and the dimension, the spacing and the
// storage form did not.
//
// The FLOAT32 and BINARY rows are the control: those element formats name no
// type the normalizer folds, so their declarations already survived and the
// rule cannot be satisfied by dropping the normalization.
func TestColumnsWithDialect_ParameterizedTypeKeepsItsParameters(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{"int8 elements", "VECTOR(512, INT8)", "vector -> vector(512, int8)"},
		{"int8 elements without a space", "VECTOR(512,INT8)", "vector -> vector(512,int8)"},
		{"int8 elements, small dimension", "VECTOR(8, INT8)", "vector -> vector(8, int8)"},
		{"int8 elements, no dimension", "VECTOR(*, INT8)", "vector -> vector(*, int8)"},
		{"int8 elements stored dense", "VECTOR(512, INT8, DENSE)", "vector -> vector(512, int8, dense)"},
		{"float32 elements", "VECTOR(512, FLOAT32)", "vector -> vector(512, float32)"},
		{"float32 elements stored sparse", "VECTOR(512, FLOAT32, SPARSE)", "vector -> vector(512, float32, sparse)"},
		{"binary elements", "VECTOR(512, BINARY)", "vector -> vector(512, binary)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "embedding", Type: tt.declared},
				catalog.Column{Name: "embedding", DataType: "VECTOR"},
				"oracle",
			)

			c.Assert(diff.Changes["type"], qt.Equals, tt.want)
		})
	}
}

// TestColumnsWithDialect_ParameterizedTypeIsNotItsParameter is the destructive
// half of the same rule.
//
// Folding a declaration onto a token from its parameter list does not only
// misname the desired type: it makes the declaration equal to a column of that
// token's own type, and an equal type is a change the comparator never
// reports. A bigint column that has to become a vector then stays a bigint,
// through every plan and every apply, with nothing in the output to read.
func TestColumnsWithDialect_ParameterizedTypeIsNotItsParameter(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		dbType   string
		want     string
	}{
		{"bigint spelled int8", "VECTOR(512, INT8)", "int8", "integer -> vector(512, int8)"},
		{"bigint spelled out", "VECTOR(512, INT8)", "bigint", "integer -> vector(512, int8)"},
		{"integer spelled int4", "VECTOR(512, INT4)", "int4", "integer -> vector(512, int4)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "embedding", Type: tt.declared},
				catalog.Column{Name: "embedding", DataType: tt.dbType},
				"postgres",
			)

			c.Assert(diff.Changes["type"], qt.Equals, tt.want)
		})
	}
}
