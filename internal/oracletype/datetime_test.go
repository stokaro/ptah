package oracletype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/oracletype"
)

// TestIsDatetime answers from the type Map creates, so a declared spelling that
// lands on DATE or TIMESTAMP is a moment and one that lands anywhere else is
// not, however close the words are.
func TestIsDatetime(t *testing.T) {
	tests := []struct {
		declared string
		want     bool
	}{
		{declared: "TIMESTAMP", want: true},
		{declared: "timestamp(6)", want: true},
		{declared: "TIMESTAMPTZ", want: true},
		{declared: "TIMESTAMP WITH TIME ZONE", want: true},
		{declared: "DATETIME", want: true},
		{declared: "DATE", want: true},
		{declared: "VARCHAR(32)", want: false},
		{declared: "TEXT", want: false},
		{declared: "BOOLEAN", want: false},
		{declared: "INTEGER", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.declared, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracletype.IsDatetime(tt.declared), qt.Equals, tt.want)
		})
	}
}
