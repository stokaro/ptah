package revisiontable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/revisiontable"
)

// A repeatable's version, and any version that is not a number where the
// directory runs under positions, is one the numeric table cannot keep.
func TestNumericTableRefuses_Refused(t *testing.T) {
	tests := []struct {
		name       string
		version    string
		repeatable bool
		positioned bool
	}{
		{name: "a repeatable", version: "R", repeatable: true},
		{name: "a repeatable that spells a number", version: "2R", repeatable: true, positioned: true},
		{name: "a version under positions", version: "v1", positioned: true},
		{name: "zero under positions", version: "00", positioned: true},
		{name: "a signed number under positions", version: "+1", positioned: true},
		{name: "a number too large for the column", version: "99999999999999999999", positioned: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisiontable.NumericTableRefuses(test.version, test.repeatable, test.positioned), qt.IsTrue)
		})
	}
}

// A decimal number above zero is kept, leading zeros included, and outside
// positions only a repeatable is judged.
func TestNumericTableRefuses_Kept(t *testing.T) {
	tests := []struct {
		name       string
		version    string
		repeatable bool
		positioned bool
	}{
		{name: "a number", version: "1"},
		{name: "a number under positions", version: "2", positioned: true},
		{name: "leading zeros under positions", version: "001", positioned: true},
		{name: "a repeatable keyed by a number", version: "3", repeatable: true},
		{name: "a version outside positions", version: "v1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisiontable.NumericTableRefuses(test.version, test.repeatable, test.positioned), qt.IsFalse)
		})
	}
}
