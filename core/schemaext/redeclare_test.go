package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// idempotent is a model whose owner states it is idempotent to declare.
type idempotent struct{ Version string }

func (*idempotent) Kind() schemaext.Kind     { return "example.org/idempotent" }
func (v *idempotent) Clone() schemaext.Value { return &idempotent{Version: v.Version} }
func (*idempotent) Redeclarable() bool       { return true }
func (v *idempotent) Equal(other schemaext.Value) bool {
	w, ok := other.(*idempotent)
	return ok && *v == *w
}

// declaredOnce is a model whose owner states nothing about a second
// declaration.
type declaredOnce struct{}

func (*declaredOnce) Kind() schemaext.Kind             { return "example.org/once" }
func (*declaredOnce) Clone() schemaext.Value           { return &declaredOnce{} }
func (*declaredOnce) Equal(other schemaext.Value) bool { _, ok := other.(*declaredOnce); return ok }

// declaredOnceOnPurpose implements the method and answers false.
type declaredOnceOnPurpose struct{}

func (*declaredOnceOnPurpose) Kind() schemaext.Kind   { return "example.org/once-on-purpose" }
func (*declaredOnceOnPurpose) Clone() schemaext.Value { return &declaredOnceOnPurpose{} }
func (*declaredOnceOnPurpose) Equal(other schemaext.Value) bool {
	_, ok := other.(*declaredOnceOnPurpose)
	return ok
}
func (*declaredOnceOnPurpose) Redeclarable() bool { return false }

func declared(value schemaext.Value, targets ...string) schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(value.Kind()),
		Name: objectidentity.Part{Source: "x", Normalized: "x"}}, Value: value, Targets: targets}
}

// TestDeclare_AnEqualRedeclarableObjectIsOneDeclaration pins both ways of
// declaring an idempotent object twice: one at a time and by merging.
func TestDeclare_AnEqualRedeclarableObjectIsOneDeclaration(t *testing.T) {
	c := qt.New(t)
	held := must.Must(schemaext.NewObjects(declared(&idempotent{Version: "1.6"}, "postgres")))

	once, err := held.Declare(declared(&idempotent{Version: "1.6"}, "postgres"))
	c.Assert(err, qt.IsNil)
	merged, err := held.MergeDeclarations(must.Must(schemaext.NewObjects(declared(&idempotent{Version: "1.6"}, "postgres"))))
	c.Assert(err, qt.IsNil)

	c.Assert(once.Equal(held), qt.IsTrue)
	c.Assert(merged.Equal(held), qt.IsTrue)
}

// TestDeclare_FailurePath pins every second declaration that stays a
// refusal: one that differs in value or scope, and any of a model whose owner
// does not state it is idempotent, equal or not.
func TestDeclare_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		held, another schemaext.Object
		want          string
	}{
		{name: "another value", held: declared(&idempotent{Version: "1.5"}), another: declared(&idempotent{Version: "1.6"}),
			want: `duplicate feature identity: conflicting definitions of example.org/idempotent x`},
		{name: "another scope", held: declared(&idempotent{}), another: declared(&idempotent{}, "postgres"),
			want: `duplicate feature identity: conflicting definitions of example.org/idempotent x`},
		{name: "a model that states nothing", held: declared(&declaredOnce{}), another: declared(&declaredOnce{}),
			want: `duplicate feature identity: object example.org/once x`},
		{name: "a model that answers no", held: declared(&declaredOnceOnPurpose{}), another: declared(&declaredOnceOnPurpose{}),
			want: `duplicate feature identity: object example.org/once-on-purpose x`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := must.Must(schemaext.NewObjects(test.held))

			declaredAgain, err := held.Declare(test.another)
			c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(declaredAgain.IsZero(), qt.IsTrue)
			merged, err := held.MergeDeclarations(must.Must(schemaext.NewObjects(test.another)))
			c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(merged.IsZero(), qt.IsTrue)
		})
	}
}
