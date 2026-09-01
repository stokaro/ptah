// Package embedplan turns a resolved specification into a plan that says where
// every one of its answers came from.
//
// A plan whose facts all read the same is a plan an operator has to take on
// trust. This one separates what was measured against the database from what
// the operator configured, what Ptah worked out, what nobody knows, and what
// this build cannot do at all -- because the decision to run a migration is
// different in each case, and a plan that hid the difference would be at its
// most confident exactly where it knows least (stokaro/ptah#2068).
package embedplan

import (
	"fmt"
	"sort"
	"strings"
)

// Provenance is where a fact came from.
type Provenance string

const (
	// Measured means Ptah asked the database or the provider and this is the
	// answer it got.
	Measured Provenance = "measured"
	// Configured means a person wrote it down. It is not checked against
	// anything here: a configured fact that is wrong stays wrong until
	// something measures it.
	Configured Provenance = "configured"
	// Inferred means Ptah derived it from other facts. Every inference names
	// what it was derived from, so an operator disagreeing with the conclusion
	// can find the premise.
	Inferred Provenance = "inferred"
	// Unknown means nobody knows and Ptah could not find out. It is a first-
	// class answer rather than a zero, because a plan that reports an unknown
	// row count as zero has told the operator the migration is free.
	Unknown Provenance = "unknown"
	// Unsupported means this build cannot do it. It is separate from unknown
	// on purpose: one is a gap in knowledge and the other is a gap in the
	// product, and they have different fixes.
	Unsupported Provenance = "unsupported"
)

// provenanceOrder ranks the values from most to least established, for a plan
// that reads worst-last.
var provenanceOrder = map[Provenance]int{
	Measured: 0, Configured: 1, Inferred: 2, Unknown: 3, Unsupported: 4,
}

// Fact is one answer with its provenance.
type Fact struct {
	// Name identifies the fact, in the plan's own vocabulary.
	Name string
	// Value is the answer, rendered for a person.
	Value string
	// Provenance says where it came from.
	Provenance Provenance
	// Detail says how, and is required for everything except a measurement.
	//
	// A measurement speaks for itself: Ptah asked and this is the reply. An
	// inference without its premise, an unknown without its reason, and an
	// unsupported without what is missing are each a sentence that sounds like
	// an answer and is not one.
	Detail string
}

// Established reports whether this fact is one a decision can rest on.
func (f Fact) Established() bool {
	return f.Provenance == Measured || f.Provenance == Configured
}

// String renders the fact for a plan a person reads.
func (f Fact) String() string {
	if f.Detail == "" {
		return fmt.Sprintf("%s = %s (%s)", f.Name, f.Value, f.Provenance)
	}
	return fmt.Sprintf("%s = %s (%s: %s)", f.Name, f.Value, f.Provenance, f.Detail)
}

// MeasuredFact is an answer read back from the database or the provider.
func MeasuredFact(name, value string) Fact {
	return Fact{Name: name, Value: value, Provenance: Measured}
}

// ConfiguredFact is an answer a person wrote down.
func ConfiguredFact(name, value, source string) Fact {
	return Fact{Name: name, Value: value, Provenance: Configured, Detail: source}
}

// InferredFact is an answer Ptah derived, with what it was derived from.
func InferredFact(name, value, from string) Fact {
	return Fact{Name: name, Value: value, Provenance: Inferred, Detail: from}
}

// UnknownFact is a question Ptah could not answer, with why.
//
// The value is fixed rather than taken from the caller. A caller that could
// supply one would not be reporting an unknown, and "unknown, approximately
// 40000" is the sentence this type exists to make unwritable.
func UnknownFact(name, reason string) Fact {
	return Fact{Name: name, Value: "unknown", Provenance: Unknown, Detail: reason}
}

// UnsupportedFact is something this build cannot do, with what is missing.
func UnsupportedFact(name, missing string) Fact {
	return Fact{Name: name, Value: "unsupported", Provenance: Unsupported, Detail: missing}
}

// Facts is an ordered set of facts, addressable by name.
type Facts []Fact

// Add appends a fact, replacing one of the same name.
//
// Replacing rather than appending is what lets a measurement supersede a
// configured default: the plan is assembled in layers, and the last word
// belongs to whoever actually looked.
func (f *Facts) Add(fact Fact) {
	for index := range *f {
		if (*f)[index].Name == fact.Name {
			(*f)[index] = fact
			return
		}
	}
	*f = append(*f, fact)
}

// Unestablished lists the facts no decision should rest on, worst last.
func (f Facts) Unestablished() Facts {
	var weak Facts
	for _, fact := range f {
		if !fact.Established() {
			weak = append(weak, fact)
		}
	}
	sort.SliceStable(weak, func(left, right int) bool {
		return provenanceOrder[weak[left].Provenance] < provenanceOrder[weak[right].Provenance]
	})
	return weak
}

// Undetailed lists the facts that owe an explanation and do not give one.
//
// Nothing here calls it at plan time: it is a ratchet over the constructors,
// and the test that runs it is what keeps a fact from being built by hand with
// its reason left out.
func (f Facts) Undetailed() []string {
	var missing []string
	for _, fact := range f {
		if fact.Provenance != Measured && strings.TrimSpace(fact.Detail) == "" {
			missing = append(missing, fact.Name)
		}
	}
	return missing
}
