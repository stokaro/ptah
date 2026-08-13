package mysql

// White-box testing required: normalizeRoutineType is unexported and has no
// exported caller that can be driven without a live server. ReadSchema is the
// only public entry, and reaching this function through it means scripting
// information_schema.ROUTINES and information_schema.PARAMETERS through the
// dbtest driver, which would pin the queries rather than the normalization the
// two engines actually disagree about. The behavior under test is a pure string
// rule, so it is tested as one.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestNormalizeRoutineType_StripsTheLegacyIntegerDisplayWidth pins the rule that
// makes one declaration converge on both engines this reader serves.
//
// Measured on the identical declaration `f1(a int) RETURNS int`, applied by
// Ptah and read back from information_schema:
//
//	MySQL 26.7.0     DTD_IDENTIFIER = int
//	MariaDB 10.11.18 DTD_IDENTIFIER = int(11)
//
// Before this rule the same schema reported `Schema is synced` on MySQL and a
// permanent `-- Modify function f1: parameters, returns` on MariaDB -- the two
// engines disagreeing with each other, not the operator disagreeing with
// either.
//
// The width is the only thing dropped. varchar(20) and decimal(10,2) carry
// meaning in the parentheses, and an unsigned suffix survives, so a mutant that
// simply truncated at the first "(" would fail the rows below.
func TestNormalizeRoutineType_StripsTheLegacyIntegerDisplayWidth(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "mariadb int width", in: "int(11)", want: "int"},
		{name: "mysql int already bare", in: "int", want: "int"},
		{name: "bigint width", in: "bigint(20)", want: "bigint"},
		{name: "tinyint width", in: "tinyint(1)", want: "tinyint"},
		{name: "smallint width", in: "smallint(6)", want: "smallint"},
		{name: "mediumint width", in: "mediumint(9)", want: "mediumint"},
		{name: "unsigned suffix survives", in: "int(10) unsigned", want: "int unsigned"},
		{name: "varchar length is meaning, not width", in: "varchar(20)", want: "varchar(20)"},
		{name: "decimal precision is meaning", in: "decimal(10,2)", want: "decimal(10,2)"},
		{name: "text has no parentheses", in: "text", want: "text"},
		{name: "enum members are meaning", in: "enum('a','b')", want: "enum('a','b')"},
		{name: "surrounding space is trimmed", in: "  int(11)  ", want: "int"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Check(normalizeRoutineType(test.in), qt.Equals, test.want)
		})
	}
}
