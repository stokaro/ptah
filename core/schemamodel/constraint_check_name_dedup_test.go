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

// The same CHECK written with other spacing, or a keyword in another case, in
// two sources is one constraint, as the comparison of a schema with a database
// reads the two spellings. Keeping both would make the CHECK name validation
// refuse a schema that declares one condition.
func TestDeduplicateConstraints_FoldsANamedCheckWrittenTwoWays(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{name: "whitespace", first: "a>0", second: "a  >  0"},
		{name: "outer parentheses", first: "a > 0", second: "(a > 0)"},
		{name: "keyword case", first: "a > 0 AND b < 9", second: "a > 0 and b < 9"},
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

// A difference inside a string is a different condition, so the pair is kept
// for the validation to refuse.
func TestDeduplicateConstraints_KeepsChecksThatDifferInAString(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Constraints: []schemamodel.Constraint{
			{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: "s <> 'A'"},
			{StructName: "T", Table: "t", Name: "dd", Type: "CHECK", CheckExpression: "s <> 'a'"},
		},
	}

	schemamodel.Deduplicate(database)

	c.Assert(database.Constraints, qt.HasLen, 2)
}
