package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A ClickHouse server prints keywords in upper case and keeps a name as
// written, and a name in another case is another column there. So a keyword
// in another case is the same condition, and on a dialect that folds names a
// name in another case is too.
func TestCheckConditionsAlike_AnotherCaseOfTheSameWordIsTheSameCondition(t *testing.T) {
	for _, test := range []struct {
		name, dialect, declared, held string
	}{
		{name: "keywords", dialect: platform.ClickHouse, declared: "v > 0 and s is not null", held: "(v > 0) AND (s IS NOT NULL)"},
		{name: "LIKE", dialect: platform.ClickHouse, declared: "s like 'a%'", held: "s LIKE 'a%'"},
		{name: "IN over a set", dialect: platform.ClickHouse, declared: "s in ('b', 'a')", held: "s IN ('a', 'b')"},
		{name: "a literal", dialect: platform.ClickHouse, declared: "b = TRUE OR v IS NULL", held: "(b = true) OR (v IS NULL)"},
		{name: "a name PostgreSQL folds", dialect: platform.Postgres, declared: "V > 0", held: "(v > 0)"},
	} {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(compare.CheckConditionsAlike(test.declared, test.held, test.dialect), qt.IsTrue)
		})
	}
}

// A name in another case is another column on ClickHouse, so the condition
// over it is another condition; a string is compared as written everywhere.
func TestCheckConditionsAlike_AnotherCaseOfANameIsAnotherCondition(t *testing.T) {
	for _, test := range []struct {
		name, dialect, declared, held string
	}{
		{name: "a column", dialect: platform.ClickHouse, declared: "V > 0", held: "v > 0"},
		{name: "a quoted column", dialect: platform.ClickHouse, declared: "`V` > 0", held: "v > 0"},
		{name: "a column inside a function", dialect: platform.ClickHouse, declared: "length(S) > 0", held: "length(s) > 0"},
		{name: "a column beside a keyword", dialect: platform.ClickHouse, declared: "V IS NOT NULL", held: "v IS NOT NULL"},
		{name: "a string", dialect: platform.ClickHouse, declared: "s = 'A'", held: "s = 'a'"},
		{name: "a string", dialect: platform.Postgres, declared: "s = 'A'", held: "(s = 'a'::text)"},
	} {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(compare.CheckConditionsAlike(test.declared, test.held, test.dialect), qt.IsFalse)
		})
	}
}
