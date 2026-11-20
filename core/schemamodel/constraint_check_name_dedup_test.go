package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// Two CHECKs of one name and table with different conditions are two
// declarations: deduplication keeps both, so the validation of CHECK names can
// refuse them as the server does. Folded by name, the second condition would be
// dropped without a word.
func TestDeduplicateConstraints_KeepsNamedChecksWithDifferentConditions(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Constraints: []schemamodel.Constraint{
			{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: "a > 0"},
			{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: "a < 9"},
		},
	}

	schemamodel.Deduplicate(database)

	c.Assert(database.Constraints, qt.HasLen, 2)
}

// The same CHECK declared twice, as two sources of one schema can, is one
// constraint.
func TestDeduplicateConstraints_FoldsANamedCheckDeclaredTwice(t *testing.T) {
	c := qt.New(t)
	check := schemamodel.Constraint{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: "a > 0"}
	database := &schemamodel.Database{
		Tables:      []schemamodel.Table{{StructName: "T", Name: "t"}},
		Constraints: []schemamodel.Constraint{check, check},
	}

	schemamodel.Deduplicate(database)

	c.Assert(database.Constraints, qt.DeepEquals, []schemamodel.Constraint{check})
}

// The same CHECK written with other spacing, or wrapped in parentheses, in two
// sources is one constraint, as every dialect reads the two spellings alike.
// Keeping both would make the CHECK name validation refuse a schema that
// declares one condition.
func TestDeduplicateConstraints_FoldsANamedCheckWrittenTwoWays(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{name: "whitespace beside punctuation", first: "a>0", second: "a  >  0"},
		{name: "outer parentheses", first: "a > 0", second: "(a > 0)"},
		{name: "leading and trailing whitespace", first: "a > 0", second: "  a > 0\n"},
		{name: "a call", first: "length(a)>0", second: "length ( a ) > 0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Constraints: []schemamodel.Constraint{
					{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: test.first},
					{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: test.second},
				},
			}

			schemamodel.Deduplicate(database)

			c.Assert(database.Constraints, qt.HasLen, 1)
		})
	}
}

// Two conditions that any dialect reads differently stay two CHECKs, so the
// CHECK name validation refuses the pair instead of one condition being
// dropped without a word. Each pair is one the key must never merge: a quoted
// identifier or a string differing in case or in a space, a backslash string,
// a dollar-quoted string, a cast, a character set introducer, a word split by
// whitespace, and a comment opener.
func TestDeduplicateConstraints_KeepsChecksThatAnyDialectReadsDifferently(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{name: "quoted identifier case", first: `"A" > 0`, second: `"a" > 0`},
		{name: "quoted identifier space", first: `"my col" > 0`, second: `"mycol" > 0`},
		{name: "double-quoted string space", first: `s <> "a b"`, second: `s <> "ab"`},
		{name: "double-quoted string case", first: `s = "A"`, second: `s = "a"`},
		{name: "string case", first: `s = 'A'`, second: `s = 'a'`},
		{name: "string space", first: `s = 'a b'`, second: `s = 'ab'`},
		{name: "backslash string case", first: `s <> 'it\'s A'`, second: `s <> 'it\'s a'`},
		{name: "escape string case", first: `s = E'it\'s A'`, second: `s = E'it\'s a'`},
		{name: "dollar-quoted string case", first: `s = $$A$$`, second: `s = $$a$$`},
		{name: "cast type", first: `x = 1::int`, second: `x = 1::bigint`},
		{name: "cast to a domain", first: `c = 'abc'::text`, second: `c = 'abc'::citext`},
		{name: "character set introducer", first: `_utf8mb4'x' = s`, second: `'x' = s`},
		{name: "word split by whitespace", first: `NOT a > 0`, second: `nota > 0`},
		{name: "identifier case", first: `A > 0`, second: `a > 0`},
		{name: "comment opener", first: `a - -1 > 0`, second: `a--1 > 0`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Constraints: []schemamodel.Constraint{
					{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: test.first},
					{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: test.second},
				},
			}

			schemamodel.Deduplicate(database)

			c.Assert(database.Constraints, qt.HasLen, 2)
		})
	}
}
