package catalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

// TestColumn_RawType_WritesATimePrecision writes a precision the catalog reports behind
// the first word of a temporal type, and leaves out the one a column holds when it
// declares none.
func TestColumn_RawType_WritesATimePrecision(t *testing.T) {
	three, six, zero := 3, 6, 0
	tests := []struct {
		name   string
		column catalog.Column
		want   string
	}{
		{name: "a name of one word", column: catalog.Column{UDTName: "timestamp", DatetimePrecision: &three}, want: "timestamp(3)"},
		{
			name:   "a name of several words",
			column: catalog.Column{DataType: "timestamp with time zone", DatetimePrecision: &three},
			want:   "timestamp(3) with time zone",
		},
		{name: "a time", column: catalog.Column{DataType: "time without time zone", DatetimePrecision: &three}, want: "time(3) without time zone"},
		{name: "an interval", column: catalog.Column{DataType: "interval", DatetimePrecision: &three}, want: "interval(3)"},
		{name: "no fractional digits", column: catalog.Column{UDTName: "timestamptz", DatetimePrecision: &zero}, want: "timestamptz(0)"},
		{name: "the default", column: catalog.Column{UDTName: "timestamp", DatetimePrecision: &six}, want: "timestamp"},
		{name: "no precision", column: catalog.Column{UDTName: "timestamp"}, want: "timestamp"},
		{name: "another type", column: catalog.Column{UDTName: "date", DatetimePrecision: &zero}, want: "date"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.column.RawType(), qt.Equals, test.want)
		})
	}
}
