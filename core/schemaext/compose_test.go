package schemaext_test

import (
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
)

var errComposition = errors.New("conflicting declaration")

// composed models independent parts with an intentionally mutating owner, to
// prove a refused composition cannot mutate either captured input.
type composed struct {
	Parts   []string
	Failure bool
	Result  schemaext.Value
}

func (*composed) Kind() schemaext.Kind { return "example.org/composed" }
func (v *composed) Clone() schemaext.Value {
	copied := *v
	copied.Parts = slices.Clone(v.Parts)
	return &copied
}
func (v *composed) Equal(other schemaext.Value) bool {
	w, ok := other.(*composed)
	return ok && v.Failure == w.Failure && slices.Equal(v.Parts, w.Parts)
}
func (v *composed) ComposeDeclaration(other schemaext.Value) (schemaext.Value, error) {
	w := other.(*composed)
	result := slices.Concat(v.Parts, w.Parts)
	v.Parts[0] = "mutated left"
	w.Parts[0] = "mutated right"
	if v.Failure {
		return nil, errComposition
	}
	if v.Result != nil {
		return v.Result, nil
	}
	return &composed{Parts: result}, nil
}

func TestDeclare_CompositionPreservesInputs(t *testing.T) {
	c := qt.New(t)
	left := must.Must(schemaext.NewObjects(declared(&composed{Parts: []string{"read"}}, "postgres")))
	right := must.Must(schemaext.NewObjects(declared(&composed{Parts: []string{"write"}}, "postgres")))
	beforeLeft := must.Must(schemaext.NewObjects(must.Must(left.All())...))
	beforeRight := must.Must(schemaext.NewObjects(must.Must(right.All())...))
	want := must.Must(schemaext.NewObjects(declared(&composed{Parts: []string{"read", "write"}}, "postgres")))

	direct, err := left.Declare(declared(&composed{Parts: []string{"write"}}, "postgres"))
	c.Assert(err, qt.IsNil)
	c.Assert(direct.Equal(want), qt.IsTrue)
	merged, err := left.MergeDeclarations(right)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Equal(want), qt.IsTrue)
	c.Assert(left.Equal(beforeLeft), qt.IsTrue)
	c.Assert(right.Equal(beforeRight), qt.IsTrue)
}

func TestDeclare_CompositionFailureIsAtomic(t *testing.T) {
	tests := []struct {
		name    string
		value   *composed
		targets []string
		want    error
	}{
		{name: "owner refusal", value: &composed{Parts: []string{"read"}, Failure: true}, want: errComposition},
		{name: "different kind", value: &composed{Parts: []string{"read"}, Result: &declaredOnce{}}, want: schemaext.ErrInvalidValue},
		{name: "typed nil", value: &composed{Parts: []string{"read"}, Result: (*declaredOnce)(nil)}, want: schemaext.ErrInvalidValue},
		{name: "different scope", value: &composed{Parts: []string{"read"}}, targets: []string{"postgres"}, want: schemaext.ErrDuplicate},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			left := must.Must(schemaext.NewObjects(declared(test.value)))
			right := must.Must(schemaext.NewObjects(declared(&composed{Parts: []string{"write"}}, test.targets...)))
			beforeLeft := must.Must(schemaext.NewObjects(must.Must(left.All())...))
			beforeRight := must.Must(schemaext.NewObjects(must.Must(right.All())...))
			direct, err := left.Declare(declared(&composed{Parts: []string{"write"}}, test.targets...))
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(direct.IsZero(), qt.IsTrue)
			merged, err := left.MergeDeclarations(right)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(merged.IsZero(), qt.IsTrue)
			c.Assert(left.Equal(beforeLeft), qt.IsTrue)
			c.Assert(right.Equal(beforeRight), qt.IsTrue)
		})
	}
}
