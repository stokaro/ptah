package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// A removed or misspelled common kind cannot silently discard a constraint.
func TestRender_RefusesUnknownCommonConstraint(t *testing.T) {
	for _, kind := range []string{"EXCLUDE", "UNQIUE", " UNIQUE ", ""} {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)
			database := constraintMethodSchema(schemamodel.Constraint{Type: kind, Columns: []string{"a"}})
			statements, err := builtin.GetOrderedCreateStatements(database, "postgres")
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*constraint "c_x" on "c" has unsupported common kind .*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}
