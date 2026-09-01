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

// TestConfiguredFacts_ReproducibilityIsReportedEitherWay pins what three
// documentation pages promise: that `plan` says whether the generation can be
// rebuilt, and names what is missing when it cannot (stokaro/ptah#2648).
//
// It is inferred rather than configured on both arms, because the answer is a
// claim about the PROVIDER read out of the specification: a revision Ptah was
// given is not a revision Ptah watched the provider honor. So a `full` answer
// joins "What is not established" alongside the partial one, which is the
// honest place for it.
//
// The reason each arm carries is asserted whole rather than by prefix. The
// detail is the half a reader acts on, and a fact reporting `partial` with an
// empty premise passes any check that only looks at the value.
func TestConfiguredFacts_ReproducibilityIsReportedEitherWay(t *testing.T) {
	tests := []struct {
		name       string
		revision   string
		wantValue  string
		wantDetail string
	}{
		{
			name:       "a pinned revision",
			revision:   "1",
			wantValue:  "full",
			wantDetail: "the specification pins an immutable model revision",
		},
		{
			name:      "no revision",
			revision:  "",
			wantValue: "partial",
			wantDetail: `provider "openai-compatible" exposes no immutable revision ` +
				`for model "bge-small-en", so asking it again may answer with different vectors`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			facts := configuredFacts(embedspec.Loaded{Spec: embedgen.Spec{
				Source: embedgen.Source{Table: "articles"},
				Model: embedgen.Model{
					Provider:   "openai-compatible",
					Identifier: "bge-small-en",
					Revision:   test.revision,
				},
			}})

			c.Assert(facts, qt.Contains, embedplan.InferredFact(
				"generation.reproducibility", test.wantValue, test.wantDetail))
		})
	}
}

// TestReproducibility_DescribeAndThePlanWordAPartialAnswerIdentically holds the
// one claim the comments make about the two verbs.
//
// They differ on `full` deliberately -- `describe`'s `reproducibility_reason`
// is a complaint and stays absent, while the plan states the premise its fact
// vocabulary requires. Where there IS a complaint they must say the same
// sentence, and the only thing keeping them together is that both read the
// identity's own field. This is what would notice if one of them started
// composing its own.
func TestReproducibility_DescribeAndThePlanWordAPartialAnswerIdentically(t *testing.T) {
	c := qt.New(t)
	loaded := embedspec.Loaded{Spec: describableSpec("")}

	described, err := DescribeSpecification(loaded)
	c.Assert(err, qt.IsNil)

	fact := reproducibilityFact(c, configuredFacts(loaded))
	c.Assert(described.Reproducibility, qt.Equals, "partial")
	c.Assert(described.Reproducibility, qt.Equals, fact.Value)
	c.Assert(described.Reason, qt.Equals, fact.Detail)
	c.Assert(described.Reason, qt.Not(qt.Equals), "")
}

// TestReproducibility_OnlyThePlanCarriesAPremiseForAFullAnswer is the other
// half, and the reason the test above cannot simply assert equality always.
func TestReproducibility_OnlyThePlanCarriesAPremiseForAFullAnswer(t *testing.T) {
	c := qt.New(t)
	loaded := embedspec.Loaded{Spec: describableSpec("1")}

	described, err := DescribeSpecification(loaded)
	c.Assert(err, qt.IsNil)

	fact := reproducibilityFact(c, configuredFacts(loaded))
	c.Assert(described.Reproducibility, qt.Equals, "full")
	c.Assert(described.Reason, qt.Equals, "")
	c.Assert(fact.Value, qt.Equals, "full")
	c.Assert(fact.Detail, qt.Equals, "the specification pins an immutable model revision")
}

// reproducibilityFact is the plan's answer, found by name.
func reproducibilityFact(c *qt.C, facts embedplan.Facts) embedplan.Fact {
	c.Helper()
	for _, fact := range facts {
		if fact.Name == "generation.reproducibility" {
			return fact
		}
	}
	c.Fatal("the plan carries no generation.reproducibility fact")
	return embedplan.Fact{}
}

// describableSpec is the smallest specification DescribeSpecification accepts,
// with the model revision as the one thing that varies.
//
// It carries a target because `describe` resolves the objects it would create,
// and a specification naming no target table is refused before reproducibility
// is reached. That refusal is stokaro/ptah#2648 finding 5 seen from the inside.
func describableSpec(revision string) embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Table: "articles", KeyFields: []string{"id"}, InputFields: []string{"body"},
		},
		Model: embedgen.Model{
			Provider: "openai-compatible", Identifier: "bge-small-en",
			Revision: revision, ReportedDimension: 4,
		},
		Target: embedgen.Target{
			Table: "articles", Column: "embedding",
			Representation: "vector", Metric: embedgen.MetricCosine,
		},
	}
}
