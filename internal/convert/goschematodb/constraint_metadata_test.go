package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/convert/goschematodb"
)

func TestConstraintConversionOwnsOptionalValuesAndDependencies(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{Constraints: []schemamodel.Constraint{{
		Table: "items", Name: "key", Type: "UNIQUE", Columns: []string{"id"},
		NullsDistinct: new(false), Requires: []objectidentity.ID{pgextension.Subject(pgextension.Ref("dependency"))},
	}}}
	current, err := goschematodb.ToDBSchema(t.Context(), desired, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(current.Constraints, qt.HasLen, 1)
	c.Assert(current.Constraints[0].Requires, qt.DeepEquals, []objectidentity.ID{pgextension.Subject(pgextension.Ref("dependency"))})
	c.Assert(*current.Constraints[0].NullsDistinct, qt.IsFalse)
	current.Constraints[0].Requires[0].Name.Source = "changed"
	*current.Constraints[0].NullsDistinct = true
	c.Assert(desired.Constraints[0].Requires, qt.DeepEquals, []objectidentity.ID{pgextension.Subject(pgextension.Ref("dependency"))})
	c.Assert(*desired.Constraints[0].NullsDistinct, qt.IsFalse)
}
