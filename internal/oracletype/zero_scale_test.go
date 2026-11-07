package oracletype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/oracletype"
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
		// NUMBER(*) is a bare NUMBER, which holds a fraction NUMBER(*,0) does not.
		{name: "a star keeps its zero scale", declared: "number(*,0)", want: "NUMBER(*,0)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(oracletype.Map(test.declared), qt.Equals, test.want)
		})
	}
}

// TestServerBuilt holds a declared name to the column the server builds from it, read back
// from user_tab_columns on 23.26, and to nothing else. A name with no such column, or a
// column another name builds, is the comparison's to ask about.
func TestServerBuilt(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		catalog  string
		want     bool
	}{
		{name: "INTEGER", declared: "INTEGER", catalog: "NUMBER(*,0)", want: true},
		{name: "a lower-case name", declared: "integer", catalog: "NUMBER(*,0)", want: true},
		{name: "INT", declared: "INT", catalog: "NUMBER(*,0)", want: true},
		{name: "SMALLINT", declared: "SMALLINT", catalog: "NUMBER(*,0)", want: true},
		{name: "DECIMAL", declared: "DECIMAL", catalog: "NUMBER(*,0)", want: true},
		{name: "NUMERIC", declared: "NUMERIC", catalog: "NUMBER(*,0)", want: true},
		{name: "DOUBLE PRECISION", declared: "DOUBLE PRECISION", catalog: "FLOAT(126)", want: true},
		{name: "REAL", declared: "REAL", catalog: "FLOAT(63)", want: true},
		{name: "BOOLEAN", declared: "BOOLEAN", catalog: "BOOLEAN", want: true},
		{name: "another size under INTEGER", declared: "INTEGER", catalog: "NUMBER(19)", want: false},
		{name: "another float under DOUBLE PRECISION", declared: "DOUBLE PRECISION", catalog: "FLOAT(63)", want: false},
		{name: "a declared size is not the word", declared: "DECIMAL(10)", catalog: "NUMBER(*,0)", want: false},
		{name: "a name Oracle spells itself", declared: "NUMBER", catalog: "NUMBER(*,0)", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracletype.ServerBuilt(test.declared, test.catalog), qt.Equals, test.want)
		})
	}
}
