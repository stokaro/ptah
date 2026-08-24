package oracletype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/oracletype"
)

// TestMap_DropsAnExplicitZeroScale pins that a declared scale of zero is not
// part of the type.
//
// `NUMBER(10,0)` and `NUMBER(10)` are one Oracle type -- the default scale IS
// zero -- and `user_tab_columns` reports the second. A declaration writing the
// first therefore never matched its own column: measured on Oracle Free 23,
// applying one document twice planned
// `ALTER TABLE items MODIFY (id number(10,0))` on the second run and on every
// run after it (stokaro/ptah#2057).
func TestMap_DropsAnExplicitZeroScale(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{name: "a zero scale is dropped", declared: "number(10,0)", want: "NUMBER(10)"},
		{name: "and on the standard spellings too", declared: "decimal(10,0)", want: "NUMBER(10)"},
		{name: "numeric as well", declared: "NUMERIC(5,0)", want: "NUMBER(5)"},
		{name: "spaces do not hide it", declared: "number( 10 , 0 )", want: "NUMBER(10)"},
		// The controls. A scale that is not zero is part of the type and the
		// catalog reports it, so nothing may be dropped there.
		{name: "a real scale survives", declared: "number(10,2)", want: "NUMBER(10,2)"},
		{name: "a precision alone is unchanged", declared: "number(10)", want: "NUMBER(10)"},
		{name: "a bare NUMBER is unchanged", declared: "number", want: "NUMBER"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(oracletype.Map(test.declared), qt.Equals, test.want)
		})
	}
}
