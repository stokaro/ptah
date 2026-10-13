package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Unnamed constraints with different owner settings remain distinct.
func TestDeduplicate_KeepsConstraintsWithDifferentFacets(t *testing.T) {
	c := qt.New(t)
	key := func(listed ...string) schemamodel.Constraint {
		return schemamodel.Constraint{
			StructName: "Child", Table: "children", Type: "FOREIGN KEY",
			Columns: []string{"a", "b"}, ForeignTable: "parents", ForeignColumns: []string{"a", "b"},
			OnDelete: "SET NULL", Facets: must.Must(schemaext.NewFacets(&keySetting{value: listed[0]})),
		}
	}
	database := &schemamodel.Database{Constraints: []schemamodel.Constraint{key("a"), key("b")}}

	schemamodel.Deduplicate(database)

	c.Assert(database.Constraints, qt.HasLen, 2)
}

type keySetting struct{ value string }

func (*keySetting) Kind() schemaext.Kind     { return "example.org/key-setting" }
func (v *keySetting) Clone() schemaext.Value { return &keySetting{value: v.value} }
func (v *keySetting) Equal(other schemaext.Value) bool {
	typed, ok := other.(*keySetting)
	return ok && typed.value == v.value
}
