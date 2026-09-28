package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestConstraintAdditionsFor_CarriesNotValid builds the addition of a CHECK the
// declaration allows NOT VALID, and the addition says so (stokaro/ptah#3853).
func TestConstraintAdditionsFor_CarriesNotValid(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Constraints: []schemamodel.Constraint{{
			StructName: "T", Table: "t", Name: "t_n_positive", Type: "CHECK", CheckExpression: "n > 0", NotValid: true,
		}},
	}

	additions := difftypes.ConstraintAdditionsFor(desired, "t_n_positive")

	c.Assert(additions, qt.HasLen, 1)
	c.Assert(additions[0].NotValid, qt.IsTrue)
}

// TestSchemaDiff_AValidationIsAChange counts a validation as a change, so a
// diff holding one alone is not reported synced.
func TestSchemaDiff_AValidationIsAChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsValidated: []difftypes.ConstraintValidation{{TableName: "t", Name: "t_ck"}}}

	c.Assert(diff.HasChanges(), qt.IsTrue)
}
