package chkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/chkey"
)

// A quoted not is the name, alone or nested, in any case and either quote, in
// an expression, a view body, the arguments of an engine and a row policy
// filter; measured on ClickHouse 24.10 and 26.10, each of these breaks SHOW
// CREATE once stored, reads back as another expression, or fails the
// statement.
func TestQuotedNot_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, expression, want string }{
		{name: "the column alone", expression: "`not`", want: "`not`"},
		{name: "in a tuple", expression: "(id, `not`)", want: "`not`"},
		{name: "in upper case", expression: "tuple(`NOT`)", want: "`NOT`"},
		{name: "a call argument", expression: "intHash32(`Not`)", want: "`Not`"},
		{name: "double quotes", expression: `(id, "not")`, want: `"not"`},
		{name: "beside an operator", expression: "`not` + 1", want: "`not`"},
		{name: "a view body", expression: "SELECT id FROM t WHERE `not` > 0", want: "`not`"},
		{name: "an alias in a view body", expression: "SELECT id AS `not`, `not` + 1 AS y FROM t", want: "`not`"},
		{name: "a table alias in a view body", expression: "SELECT x.id FROM t AS \"NOT\"", want: `"NOT"`},
		{name: "engine arguments", expression: "SummingMergeTree((`not`))", want: "`not`"},
		{name: "a double-quoted engine argument", expression: `ReplacingMergeTree("not")`, want: `"not"`},
		{name: "a row policy filter", expression: "`not` > 0", want: "`not`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, found := chkey.QuotedNot(test.expression)
			c.Assert(found, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The operator, the function, a string and a longer name are not the column.
func TestQuotedNot_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, expression string }{
		{name: "the operator", expression: "not x"},
		{name: "the function", expression: "`not`(x)"},
		{name: "a string", expression: "concat(s, 'not')"},
		{name: "a longer name", expression: "(`not_x`, `nott`)"},
		{name: "a view body that names other columns", expression: "SELECT id, 'not' AS s FROM t WHERE NOT (id > 0)"},
		{name: "no name", expression: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, found := chkey.QuotedNot(test.expression)
			c.Assert(found, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestRefuseQuotedNot(t *testing.T) {
	c := qt.New(t)
	err := chkey.RefuseQuotedNot(`the ORDER BY of table "t"`, "(id, `NOT`)")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "unsupported feature: ClickHouse: the ORDER BY of table \"t\" names `NOT`; "+
		"releases before 26.11 store that name as the bare word not, the NOT operator, so the statement or every later "+
		"read of the schema fails, or the definition means another expression; rename it")
	c.Assert(chkey.RefuseQuotedNot(`the ORDER BY of table "t"`, "(id, `and`)"), qt.IsNil)
}
