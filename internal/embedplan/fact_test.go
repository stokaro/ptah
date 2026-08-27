package embedplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedplan"
)

// TestFact_EveryProvenanceExceptMeasuredOwesAnExplanation is the rule the
// constructors exist to enforce.
//
// An inference without its premise, an unknown without its reason and an
// unsupported without what is missing are each a sentence that sounds like an
// answer and is not one. A measurement is the exception: Ptah asked, and this
// is the reply.
func TestFact_EveryProvenanceExceptMeasuredOwesAnExplanation(t *testing.T) {
	c := qt.New(t)
	facts := embedplan.Facts{
		embedplan.MeasuredFact("a", "1"),
		embedplan.ConfiguredFact("b", "2", "the specification"),
		embedplan.InferredFact("c", "3", "because of a"),
		embedplan.UnknownFact("d", "nobody counted"),
		embedplan.UnsupportedFact("e", "this build has no such renderer"),
	}

	c.Assert(facts.Undetailed(), qt.HasLen, 0)
}

// TestFact_AHandBuiltFactWithoutAReasonIsCaught is what makes the row above
// mean something.
//
// The constructors cannot be the only guard, because the struct is exported and
// a caller can fill it in directly. This is the shape that gets past them.
func TestFact_AHandBuiltFactWithoutAReasonIsCaught(t *testing.T) {
	c := qt.New(t)
	facts := embedplan.Facts{
		{Name: "sneaked", Value: "0", Provenance: embedplan.Unknown},
		{Name: "measured", Value: "1", Provenance: embedplan.Measured},
	}

	c.Assert(facts.Undetailed(), qt.DeepEquals, []string{"sneaked"})
}

// TestUnknownFact_CannotCarryAValue is the constructor's whole point.
//
// "unknown, approximately 40000" is a number an operator will plan around, and
// a signature that let a caller supply one would make the type a label rather
// than a rule.
func TestUnknownFact_CannotCarryAValue(t *testing.T) {
	c := qt.New(t)

	fact := embedplan.UnknownFact("source.estimated_rows", "nobody counted")

	c.Assert(fact.Value, qt.Equals, "unknown")
	c.Assert(fact.Established(), qt.IsFalse)
}

// TestFacts_AddReplacesByName is what lets a measurement supersede a default.
//
// The plan is assembled in layers and the last word belongs to whoever actually
// looked. Appending instead would leave two answers to one question and let
// whichever was read first decide.
func TestFacts_AddReplacesByName(t *testing.T) {
	c := qt.New(t)
	var facts embedplan.Facts
	facts.Add(embedplan.ConfiguredFact("rows", "40000", "the specification"))
	facts.Add(embedplan.MeasuredFact("rows", "120000"))

	c.Assert(facts, qt.HasLen, 1)
	c.Assert(facts[0].Value, qt.Equals, "120000")
	c.Assert(facts[0].Provenance, qt.Equals, embedplan.Measured)
}

// TestFacts_AddKeepsTheOriginalPosition keeps a superseded fact from moving to
// the end of the plan.
//
// A plan read top to bottom tells a story, and a measurement that jumped
// position would reorder it for no reason a reader could see.
func TestFacts_AddKeepsTheOriginalPosition(t *testing.T) {
	c := qt.New(t)
	var facts embedplan.Facts
	facts.Add(embedplan.ConfiguredFact("first", "a", "the specification"))
	facts.Add(embedplan.ConfiguredFact("second", "b", "the specification"))
	facts.Add(embedplan.MeasuredFact("first", "c"))

	c.Assert(facts[0].Name, qt.Equals, "first")
	c.Assert(facts[0].Value, qt.Equals, "c")
	c.Assert(facts[1].Name, qt.Equals, "second")
}

// TestFact_EstablishedSeparatesWhatADecisionCanRestOn pins the two halves.
func TestFact_EstablishedSeparatesWhatADecisionCanRestOn(t *testing.T) {
	tests := []struct {
		name        string
		fact        embedplan.Fact
		established bool
	}{
		{name: "measured", fact: embedplan.MeasuredFact("a", "1"), established: true},
		{name: "configured", fact: embedplan.ConfiguredFact("a", "1", "why"), established: true},
		{name: "inferred", fact: embedplan.InferredFact("a", "1", "why"), established: false},
		{name: "unknown", fact: embedplan.UnknownFact("a", "why"), established: false},
		{name: "unsupported", fact: embedplan.UnsupportedFact("a", "why"), established: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.fact.Established(), qt.Equals, test.established)
		})
	}
}
