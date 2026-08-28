package embedreport

// White-box testing required: configuredFacts is the unexported assembly the
// plan is built from, and the check this file runs -- Facts.Undetailed -- is
// only meaningful against the list something actually produces. A black-box
// copy of that list would be a second literal, which is the shape the check
// exists to catch.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedplan"
	"go.5x5.cz/ptah/internal/embedspec"
)

// TestConfiguredFacts_EveryFactThatOwesAnExplanationGivesOne runs the plan's
// own self-check against the plan's own facts.
//
// `Facts.Undetailed` reports facts whose provenance owes a reason and that
// carry none -- an inference without its premise, an unknown without its
// reason. Each is a sentence that sounds like an answer and is not one. Nothing
// called it until stokaro/ptah#2474.
//
// It found one immediately: source.table took the specification's NAME as its
// source, which read as `source.table = articles (configured: articles)` when
// the two matched, and as no detail at all when the specification carried no
// name.
func TestConfiguredFacts_EveryFactThatOwesAnExplanationGivesOne(t *testing.T) {
	tests := []struct {
		name   string
		loaded embedspec.Loaded
	}{
		{
			name: "a specification that fills everything",
			loaded: embedspec.Loaded{
				Spec: embedgen.Spec{
					Name:   "articles",
					Source: embedgen.Source{Table: "articles"},
					Model:  embedgen.Model{Identifier: "bge-small-en", Revision: "1"},
				},
				// #nosec G101 -- a reference to where a credential lives, which
				// is the only form a specification carries. The value is read
				// at run time and never written down.
				Credential: "env:PTAH_EMBED_TOKEN",
			},
		},
		{
			// The case the check found. A specification is not required to
			// carry a name, and a fact whose detail came from one had none.
			name: "a specification with no name",
			loaded: embedspec.Loaded{
				Spec: embedgen.Spec{
					Source: embedgen.Source{Table: "articles"},
					Model:  embedgen.Model{Identifier: "bge-small-en"},
				},
			},
		},
		{
			name:   "an empty specification",
			loaded: embedspec.Loaded{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(configuredFacts(test.loaded).Undetailed(), qt.HasLen, 0)
		})
	}
}

// TestConfiguredFacts_AFactBuiltWithoutItsReasonIsReported is the control.
//
// Every row above is satisfied by an Undetailed that reports nothing. This
// builds the shape it exists to catch -- a struct literal, which is the only
// way to make one, because every constructor takes the detail.
func TestConfiguredFacts_AFactBuiltWithoutItsReasonIsReported(t *testing.T) {
	c := qt.New(t)

	facts := append(configuredFacts(embedspec.Loaded{}),
		unknownWithNoReason())

	c.Assert(facts.Undetailed(), qt.DeepEquals, []string{"provider.reachable"})
}

// unknownWithNoReason is the fact no constructor can build.
func unknownWithNoReason() embedplan.Fact {
	return embedplan.Fact{Name: "provider.reachable", Value: "no", Provenance: embedplan.Unknown}
}
