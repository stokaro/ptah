package coverage

import (
	"fmt"
	"slices"
	"strings"
)

// Vocabulary is the set of kinds the directives of one run may name: the
// common kinds this package declares, and the kinds the selected owners
// register for state they record and do not model, such as a YDB changefeed.
// A runtime builds it from its providers; see
// [ptah.run/engine.Runtime.CoverageVocabulary].
//
// Its zero value holds the common kinds only, so an owner's kind is refused
// by name unless its owner was selected. A Vocabulary is immutable and safe
// for concurrent use.
type Vocabulary struct {
	owned []Kind
}

// NewVocabulary returns a vocabulary of the common kinds and owned. Each owned
// kind must be a lower-case name, appear once, and not be a common kind:
// anything else is refused, so two owners cannot claim one spelling and no
// owner can redefine a common one.
func NewVocabulary(owned ...Kind) (Vocabulary, error) {
	result := Vocabulary{owned: make([]Kind, 0, len(owned))}
	for _, kind := range owned {
		token := string(kind)
		if token == "" || token != strings.ToLower(strings.TrimSpace(token)) || strings.ContainsAny(token, " \t\"=") {
			return Vocabulary{}, fmt.Errorf("coverage kind %q is not a lower-case name", token)
		}
		if slices.Contains(kinds, kind) {
			return Vocabulary{}, fmt.Errorf("coverage kind %q is a common kind and cannot be an owner's", token)
		}
		if slices.Contains(result.owned, kind) {
			return Vocabulary{}, fmt.Errorf("coverage kind %q is registered twice", token)
		}
		result.owned = append(result.owned, kind)
	}
	slices.Sort(result.owned)
	return result, nil
}

// Kinds returns every kind the vocabulary accepts, common and owned, in a new
// sorted slice.
func (v Vocabulary) Kinds() []Kind {
	all := append(slices.Clone(kinds), v.owned...)
	slices.Sort(all)
	return all
}

// ParseKind resolves a serialized kind token against the vocabulary. It
// refuses anything the vocabulary does not hold rather than returning a zero
// value, and names every kind it accepts, so a directive this run does not
// understand fails loudly instead of silently covering nothing.
func (v Vocabulary) ParseKind(token string) (Kind, error) {
	kind := Kind(strings.ToLower(strings.TrimSpace(token)))
	if slices.Contains(kinds, kind) || slices.Contains(v.owned, kind) {
		return kind, nil
	}
	return "", fmt.Errorf("unknown coverage kind %q: valid kinds are %s", token, tokenList(v.Kinds()))
}

// Validate reports the first record of set whose kind, reason or provenance
// the vocabulary does not understand.
func (v Vocabulary) Validate(set Set) error {
	for _, object := range set.Objects {
		if _, err := v.ParseKind(string(object.Kind)); err != nil {
			return err
		}
		if err := object.validateAttributes(); err != nil {
			return err
		}
	}
	return nil
}

// Runtime supplies the vocabulary of a run's selected owners to a reader of
// document headers. *ptah.run/engine.Runtime implements it.
type Runtime interface {
	CoverageVocabulary() Vocabulary
}
