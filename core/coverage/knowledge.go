package coverage

import (
	"fmt"
	"slices"
	"strings"
)

// Reason says WHY a description does not describe something.
//
// [Set] on its own is a two-state answer: a kind or an object is either
// described or it is not. That answer is enough to keep a comparator from
// turning silence into a removal, and it was the whole of stokaro/ptah#1276.
// It is not enough for anything that has to explain itself, because the same
// silence arrives from reasons a reader cannot tell apart afterwards:
//
//   - a read was refused the catalog it needed;
//   - a selection the user wrote put the object outside the run;
//   - the target cannot have the object family at all;
//   - a compatibility policy left the block out of a document;
//   - a reference the description depends on never resolved.
//
// Flattening those into one word costs a user the one sentence that would tell
// them what to do about it: grant the privilege, widen the selection, ignore
// it, or fix the reference. So the reason travels with the record, through
// projection and through serialization, to the surface that prints it
// (stokaro/ptah#1346).
//
// The list is closed and an unknown token is refused, for the reason
// [ParseKind] gives: a value nothing understands reads as no value at all.
type Reason string

// The reasons a description can decline to describe something. [ReasonUnspecified]
// is the coarse answer -- the record says only that the absence carries no
// information -- and it is what a hand-authored directive that names no reason
// means.
const (
	// ReasonUnspecified records no reason. It is the zero value, so a coarse
	// record built by [Set.WithKind] or [Set.WithObject] carries it, and a
	// directive written without a reason attribute decodes back to it.
	ReasonUnspecified Reason = ""
	// NotInspected means the reader did not look. The catalog it needed was
	// refused, or it was never queried, and the object family may well be
	// there.
	NotInspected Reason = "not-inspected"
	// OutsideScope means the object family was ruled out by the selection this
	// run was given, not by anything about the database.
	OutsideScope Reason = "outside-scope"
	// Unsupported means this side cannot express or read the object family at
	// all: a source language with no syntax for it, or a target whose
	// capabilities say the family cannot exist.
	Unsupported Reason = "unsupported"
	// SuppressedByPolicy means the object family was left out on purpose by a
	// policy, so a document could stay readable by something else.
	SuppressedByPolicy Reason = "suppressed"
	// Unresolved means a reference the description depends on was never
	// resolved, so what it would have contributed is unknown rather than empty.
	Unresolved Reason = "unresolved"
)

// reasons is every valid [Reason] other than [ReasonUnspecified], which is
// spelled by leaving the attribute out.
var reasons = []Reason{NotInspected, OutsideScope, Unsupported, SuppressedByPolicy, Unresolved}

// ParseReason resolves a serialized reason token, refusing anything outside the
// closed list.
func ParseReason(token string) (Reason, error) {
	reason := Reason(strings.ToLower(strings.TrimSpace(token)))
	if slices.Contains(reasons, reason) {
		return reason, nil
	}
	return "", fmt.Errorf("unknown coverage reason %q: valid reasons are %s", token, tokenList(reasons))
}

// Valid reports whether the reason is one this build understands.
// [ReasonUnspecified] is valid: a record may decline to give a reason.
func (r Reason) Valid() bool { return r == ReasonUnspecified || slices.Contains(reasons, r) }

// Provenance says HOW a fact was learned. It is the second half of the same
// question [Reason] answers: a limit learned by watching a server refuse a
// query is worth more than one a default policy applied without looking, and a
// surface that cannot tell them apart cannot say which.
//
// It rides on coverage records here. The broader migration that puts provenance
// on every fact in the canonical state is stokaro/ptah#1349's, and this type is
// the one both halves share so the two do not drift into separate vocabularies.
//
// The list is closed and an unknown token is refused, exactly as [Kind] and
// [Reason] are.
type Provenance string

// The ways a fact reaches Ptah. [ProvenanceUnspecified] is the coarse answer.
const (
	// ProvenanceUnspecified records no provenance. It is the zero value.
	ProvenanceUnspecified Provenance = ""
	// Declared means a description stated it: a directive in a document, an
	// annotation in a source file.
	Declared Provenance = "declared"
	// Observed means Ptah watched the target produce it, including watching the
	// target refuse.
	Observed Provenance = "observed"
	// DerivedFromTarget means it follows from what the target is -- its
	// dialect, version or capability set -- rather than from anything read out
	// of it.
	DerivedFromTarget Provenance = "derived-from-target"
	// DerivedFromFact means it follows from another fact Ptah already holds.
	DerivedFromFact Provenance = "derived-from-fact"
	// Configured means the run was told: a flag, a selection, an environment
	// variable.
	Configured Provenance = "configured"
	// Defaulted means nothing told the run, and a default applied.
	Defaulted Provenance = "defaulted"
	// Inferred means Ptah concluded it without direct evidence.
	Inferred Provenance = "inferred"
	// Unavailable means the provenance itself is not known. It is not the same
	// as [ProvenanceUnspecified]: this record says the question was asked and
	// has no answer, where the zero value says it was never asked.
	Unavailable Provenance = "unavailable"
)

// provenances is every valid [Provenance] other than [ProvenanceUnspecified].
var provenances = []Provenance{
	Declared, Observed, DerivedFromTarget, DerivedFromFact,
	Configured, Defaulted, Inferred, Unavailable,
}

// ParseProvenance resolves a serialized provenance token, refusing anything
// outside the closed list.
func ParseProvenance(token string) (Provenance, error) {
	provenance := Provenance(strings.ToLower(strings.TrimSpace(token)))
	if slices.Contains(provenances, provenance) {
		return provenance, nil
	}
	return "", fmt.Errorf(
		"unknown coverage provenance %q: valid provenances are %s", token, tokenList(provenances))
}

// Valid reports whether the provenance is one this build understands.
// [ProvenanceUnspecified] is valid: a record may decline to give one.
func (p Provenance) Valid() bool {
	return p == ProvenanceUnspecified || slices.Contains(provenances, p)
}

// Direct reports whether the fact was learned first-hand -- stated by a
// description, watched happening, or given to the run -- as opposed to worked
// out from something else or assumed.
//
// This is the certainty axis, and it is derived from provenance rather than
// stored beside it, because a stored certainty is a third thing to keep in step
// with the other two and nothing would notice when it stopped being.
// [ProvenanceUnspecified] is not direct: a record that never said where it came
// from has not earned the stronger word.
func (p Provenance) Direct() bool {
	switch p {
	case Declared, Observed, Configured:
		return true
	case ProvenanceUnspecified, DerivedFromTarget, DerivedFromFact, Defaulted, Inferred, Unavailable:
		return false
	default:
		return false
	}
}

// tokenList renders a closed list for an error message.
func tokenList[T ~string](values []T) string {
	names := make([]string, 0, len(values))
	for _, value := range values {
		names = append(names, string(value))
	}
	return strings.Join(names, ", ")
}

// Explain renders a record's reason and provenance as a clause a diagnostic can
// drop into a sentence, in the plural, without a leading capital or a trailing
// period: "the read was refused the catalog that would have listed them".
//
// It is empty when the record gives no reason, which is exactly what a
// hand-authored directive naming only a kind means -- the description declined
// the kind and said no more -- and a surface meeting an empty clause says that
// much and no more either.
//
// The vocabulary lives here rather than in the surfaces so that every surface
// says the same thing about the same record. A reason that reached a user as
// "unsupported" in one command and "not read" in another would be worse than
// the coarse answer it replaced.
func (o Object) Explain() string {
	switch o.Reason {
	case NotInspected:
		if o.Provenance == Observed {
			return "the read was refused the catalog that would have listed them"
		}
		return "nothing in this run looked for them"
	case OutsideScope:
		return "the selection this run was given put them outside it"
	case Unsupported:
		switch o.Provenance {
		case DerivedFromTarget:
			return "this target cannot report them"
		case DerivedFromFact:
			return "the format that description is written in cannot express them"
		default:
			return "that side cannot describe them"
		}
	case SuppressedByPolicy:
		return "a compatibility policy left them out of the description"
	case Unresolved:
		return "a reference the description depends on never resolved"
	case ReasonUnspecified:
		return ""
	default:
		return ""
	}
}
