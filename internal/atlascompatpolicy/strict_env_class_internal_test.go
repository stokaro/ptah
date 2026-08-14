package atlascompatpolicy

// White-box testing required: the mapping from an [envbool.Class] to a refusal
// is the design decision this file exists to pin, and the only class no
// exported path can reach is the interesting one. Every declaration in the tree
// states a class and cmd/internal/envboolguard keeps it that way, so an
// unclassified variable cannot be constructed through Resolve without first
// breaking that guard. Measured from outside, the fail-closed default would be
// untested exactly where it matters.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/envbool"
)

// TestStrictRefusesEnabledFailsClosedForAnUnstatedClass pins the default the
// derivation chose.
//
// A variable whose declaration says nothing about strict compatibility is
// refused, not retained. Both directions of the choice are wrong for somebody:
// refusing breaks a retained variable that was overlooked, retaining honors a
// gated one that was. The difference is which failure the operator sees. The
// refusal names the variable on the first run that sets it, and the fix is one
// word at the declaration. The other default produces a strict run that quietly
// used a capability the pinned community binary does not have, and reports
// parity for it -- which is the defect the whole derivation exists to close, put
// back one level up.
//
// Retained and selector opt out of the default rather than being sorted by it,
// which is what makes the default the safe one: a variable reaches the
// permissive answer only by saying so.
func TestStrictRefusesEnabledFailsClosedForAnUnstatedClass(t *testing.T) {
	tests := []struct {
		name  string
		class envbool.Class
		want  bool
	}{
		{name: "an unstated class is refused", class: envbool.Unclassified, want: true},
		{name: "a gated class is refused", class: envbool.Gated, want: true},
		{name: "a retained class is honored", class: envbool.Retained, want: false},
		{name: "the selector is never refused", class: envbool.Selector, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := strictRefusesEnabled(test.class)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
