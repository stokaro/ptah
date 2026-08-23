package types_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
)

// RawType answers one question two comparators both ask, which is why it lives
// on the column rather than beside either of them -- and why it needs a test of
// its own. A mutant inside a shared helper moves both callers together, so a
// comparison between them cannot see it: measured, on a version that dropped a
// numeric's scale and left every differential row green (stokaro/ptah#1662).
func TestRawType(t *testing.T) {
	tests := []struct {
		name   string
		column types.DBColumn
		want   string
	}{
		{
			// What a PostgreSQL read reports for `code varchar(50)`: the type
			// name without its width, and the width in a field of its own.
			name: "a varchar keeps the width the catalog holds separately",
			column: types.DBColumn{
				DataType: "character varying", CharacterMaxLength: new(50),
			},
			want: "character varying(50)",
		},
		{
			name: "a varchar with no width stays bare",
			column: types.DBColumn{
				DataType: "character varying",
			},
			want: "character varying",
		},
		{
			name: "a numeric keeps its precision and its scale",
			column: types.DBColumn{
				DataType: "numeric", NumericPrecision: new(10), NumericScale: new(2),
			},
			want: "numeric(10,2)",
		},
		{
			name: "a numeric with a precision and no scale keeps the precision",
			column: types.DBColumn{
				DataType: "numeric", NumericPrecision: new(10),
			},
			want: "numeric(10)",
		},
		{
			// A type that already carries its size is left alone, or the size
			// would be appended twice.
			name: "a spelling that already carries its size is untouched",
			column: types.DBColumn{
				DataType: "character varying", ColumnType: "varchar(100)", CharacterMaxLength: new(100),
			},
			want: "varchar(100)",
		},
		{
			// The order the fields are consulted in, which stokaro/ptah#1138
			// settled: FormattedType is the only one that survives an array or
			// a domain, and both sides read it.
			name: "the server's own spelling wins",
			column: types.DBColumn{
				DataType: "ARRAY", UDTName: "_varchar", ColumnType: "varchar",
				FormattedType: "character varying(100)[]",
			},
			want: "character varying(100)[]",
		},
		{
			name: "then the column type",
			column: types.DBColumn{
				DataType: "enum", UDTName: "status", ColumnType: "enum('a','b')",
			},
			want: "enum('a','b')",
		},
		{
			name: "then the user-defined type name",
			column: types.DBColumn{
				DataType: "USER-DEFINED", UDTName: "status",
			},
			want: "status",
		},
		{
			name:   "and the plain data type last",
			column: types.DBColumn{DataType: "integer"},
			want:   "integer",
		},
		{
			// A type with no size family gets nothing appended even when the
			// catalog fills the fields, which it does for an integer.
			name: "a type with no size family is left alone",
			column: types.DBColumn{
				DataType: "integer", NumericPrecision: new(32), NumericScale: new(0),
			},
			want: "integer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.column.RawType(), qt.Equals, test.want)
		})
	}
}
