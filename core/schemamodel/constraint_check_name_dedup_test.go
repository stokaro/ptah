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
