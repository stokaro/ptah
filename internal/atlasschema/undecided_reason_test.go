package atlasschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// The warning is the only place a withheld addition surfaces at all: no
// statement is rendered for it, so a user who does not read this line sees a
// plan that quietly does less than they asked for.
//
// Naming the directive was all the line could do while every record said the
// same word. Now the record says why, and the four reasons that word flattened
// are four different problems with four different fixes -- grant the privilege,
// widen the selection, use a target that has the feature, or turn the policy off
// (stokaro/ptah#1346).
func TestUndecidedWarningExplainsWhyTheCurrentSideCouldNotDecide(t *testing.T) {
	tests := []struct {
		name   string
		object coverage.Object
		want   string
	}{
		{
			name:   "a read the server refused",
			object: withheldExtension(coverage.Refused(coverage.Extension)),
			want: "--from does not describe extension objects because" +
				" the read was refused the catalog that would have listed them",
		},
		{
			name: "a target that cannot report the kind",
			object: withheldExtension(coverage.Object{
				Reason:     coverage.Unsupported,
				Provenance: coverage.DerivedFromTarget,
			}),
			want: "--from does not describe extension objects because this target cannot report them",
		},
		{
			name: "a selection that ruled the kind out",
			object: withheldExtension(coverage.Object{
				Reason:     coverage.OutsideScope,
				Provenance: coverage.Configured,
			}),
			want: "--from does not describe extension objects because" +
				" the selection this run was given put them outside it",
		},
		{
			name: "a compatibility policy that omitted the block",
			object: withheldExtension(coverage.Object{
				Reason:     coverage.SuppressedByPolicy,
				Provenance: coverage.Defaulted,
			}),
			want: "--from does not describe extension objects because" +
				" a compatibility policy left them out of the description",
		},
		{
			// A hand-authored directive says nothing but the kind, so the
			// warning quotes it back: that line is what the user will search
			// the document for, and it is all the document said.
			name:   "a document that gave no reason",
			object: withheldExtension(coverage.Object{}),
			want:   "--from records `ptah:not-described extension`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics strings.Builder

			atlasschema.ReportUndecidedAdditions(
				&diagnostics, []coverage.Object{test.object}, "--from", "--to")

			c.Assert(diagnostics.String(), qt.Contains, test.want)
			c.Assert(diagnostics.String(), qt.Contains,
				`Warning: extension "citext" is declared by --to but no change was planned for it:`)
			c.Assert(diagnostics.String(), qt.Contains,
				"cannot safely converge from an unknown current state.\n")
		})
	}
}

// withheldExtension is the record the comparator builds for one withheld
// extension: the object it held back, wearing the reason and provenance the
// current side's coverage gave for its silence.
func withheldExtension(limit coverage.Object) coverage.Object {
	return coverage.Object{
		Kind:       coverage.Extension,
		Name:       "citext",
		Reason:     limit.Reason,
		Provenance: limit.Provenance,
	}
}

// TestUndecidedWarningsAreDistinctPerReason is the assertion the rows above
// cannot make one at a time: five records that differ only in reason and
// provenance must produce five different sentences. A surface that printed one
// sentence for all of them would pass every row and still tell the user nothing.
func TestUndecidedWarningsAreDistinctPerReason(t *testing.T) {
	c := qt.New(t)
	limits := []coverage.Object{
		coverage.Refused(coverage.Extension),
		{Reason: coverage.Unsupported, Provenance: coverage.DerivedFromTarget},
		{Reason: coverage.OutsideScope, Provenance: coverage.Configured},
		{Reason: coverage.SuppressedByPolicy, Provenance: coverage.Defaulted},
		{Reason: coverage.Unresolved},
		{},
	}

	warnings := make(map[string]struct{}, len(limits))
	for _, limit := range limits {
		var diagnostics strings.Builder
		atlasschema.ReportUndecidedAdditions(
			&diagnostics, []coverage.Object{withheldExtension(limit)}, "--from", "--to")
		warnings[diagnostics.String()] = struct{}{}
	}

	c.Assert(warnings, qt.HasLen, len(limits))
}
