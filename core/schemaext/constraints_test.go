package schemaext_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

const constraintValueKind schemaext.Kind = "example.org/constraint"

type constraintValue struct {
	expressions   []string
	wrongIdentity bool
}

func (*constraintValue) Kind() schemaext.Kind { return constraintValueKind }
func (v *constraintValue) Clone() schemaext.Value {
	return &constraintValue{expressions: slices.Clone(v.expressions), wrongIdentity: v.wrongIdentity}
}
func (v *constraintValue) Equal(other schemaext.Value) bool {
	o, ok := other.(*constraintValue)
	return ok && v.wrongIdentity == o.wrongIdentity && slices.Equal(v.expressions, o.expressions)
}
func (v *constraintValue) DefinedConstraint(ref objectidentity.ID) schemaext.DefinedConstraint {
	if v.wrongIdentity {
		ref.Name.Source = "different"
	}
	return schemaext.DefinedConstraint{Ref: ref, Indexed: true, Expressions: v.expressions}
}
func constraintObject(parent string, wrong bool) schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(constraintValueKind), Parent: objectidentity.Part{Source: parent, Normalized: parent}, Name: objectidentity.Part{Source: "c", Normalized: "c"}}, Value: &constraintValue{expressions: []string{"f(a)", "g(b)"}, wrongIdentity: wrong}}
}
func TestDefinedConstraintsHaveIndependentExpressions(t *testing.T) {
	c := qt.New(t)
	object := constraintObject("table", false)
	objects, err := schemaext.NewObjects(object)
	c.Assert(err, qt.IsNil)
	first := schemaext.DefinedConstraints(objects)
	c.Assert(first, qt.DeepEquals, []schemaext.DefinedConstraint{{Ref: object.Ref, Indexed: true, Expressions: []string{"f(a)", "g(b)"}}})
	first[0].Expressions[0] = "changed"
	c.Assert(schemaext.DefinedConstraints(objects)[0].Expressions, qt.DeepEquals, []string{"f(a)", "g(b)"})
}
func TestDefinedConstraintsRejectInvalidProjections(t *testing.T) {
	tests := []struct {
		name, parent string
		wrong        bool
	}{
		{name: "missing parent"},
		{name: "changed identity", parent: "table", wrong: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := schemaext.NewObjects(constraintObject(test.parent, test.wrong))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(schemaext.DefinedConstraints(got), qt.IsNil)
		})
	}
}

func TestDefinedConstraintsKeepIdentityOrder(t *testing.T) {
	c := qt.New(t)
	first, second := constraintObject("first", false), constraintObject("second", false)
	objects, err := schemaext.NewObjects(second, first)
	c.Assert(err, qt.IsNil)
	got := schemaext.DefinedConstraints(objects)
	c.Assert(got, qt.HasLen, 2)
	c.Assert(got[0].Ref, qt.DeepEquals, first.Ref)
	c.Assert(got[1].Ref, qt.DeepEquals, second.Ref)
	c.Assert(schemaext.DefinedConstraints(schemaext.Objects{}), qt.IsNil)
}
