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
// It also knows the owners' in-process kinds: state a read records and an
// owner consults in the same process, which no document names, such as a
// YDB replica table. A writer leaves a record of one out of a document and
// refuses a record of a kind the vocabulary does not know at all.
//
// It also knows, per document format, the owners' kinds the format has no
// spelling for, such as a YDB changefeed in HCL; see
// [Vocabulary.FormatLimits].
//
// Its zero value holds the common kinds only, so an owner's kind is refused
// by name unless its owner was selected. A Vocabulary is immutable and safe
// for concurrent use.
type Vocabulary struct {
	owned   []Kind
	process []Kind
	limits  map[Format][]Kind
}

// Format names a schema document format a loader reads, so that an owner can
// state which of its kinds the format has no spelling for. The list is
// closed, and an unknown format is refused.
type Format string

// The document formats an owner can state a limit for.
const (
	// FormatHCL is an HCL schema document.
	FormatHCL Format = "hcl"
	// FormatDBML is a DBML document.
	FormatDBML Format = "dbml"
	// FormatYAML is a YAML schema document.
	FormatYAML Format = "yaml"
	// FormatSQL is a SQL schema document.
	FormatSQL Format = "sql"
)

// formats is every valid [Format].
var formats = []Format{FormatDBML, FormatHCL, FormatSQL, FormatYAML}

// FormatLimit is an owner's statement that a document format has no spelling
// for some of the owner's kinds. A loader records each of them as not
// described by a document in that format, with the reason [Unsupported] and
// the provenance [DerivedFromFact], so the document's silence about one is not
// read as a request to remove it.
type FormatLimit struct {
	// Format is the document format.
	Format Format
	// Kinds are the owner's kinds the format cannot spell.
	Kinds []Kind
}

// NewVocabulary returns a vocabulary of the common kinds and owned. Each owned
// kind must be a lower-case name, appear once, and not be a common kind:
// anything else is refused, so two owners cannot claim one spelling and no
// owner can redefine a common one.
func NewVocabulary(owned ...Kind) (Vocabulary, error) {
	result := Vocabulary{owned: make([]Kind, 0, len(owned))}
	for _, kind := range owned {
		token := string(kind)
		if !isKindName(token) {
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

// WithProcess returns the vocabulary that also knows kinds as in-process
// kinds; see [Vocabulary.InProcess]. Each must be a lower-case name, appear
// once, and be neither a common kind nor one the vocabulary accepts, so a
// kind a document may name cannot also be left out of every document.
func (v Vocabulary) WithProcess(kinds ...Kind) (Vocabulary, error) {
	result := Vocabulary{owned: v.owned, process: slices.Clone(v.process), limits: v.limits}
	for _, kind := range kinds {
		token := string(kind)
		if !isKindName(token) {
			return Vocabulary{}, fmt.Errorf("in-process coverage kind %q is not a lower-case name", token)
		}
		if slices.Contains(v.Kinds(), kind) {
			return Vocabulary{}, fmt.Errorf("coverage kind %q is a kind documents name and cannot be in-process", token)
		}
		if slices.Contains(result.process, kind) {
			return Vocabulary{}, fmt.Errorf("in-process coverage kind %q is registered twice", token)
		}
		result.process = append(result.process, kind)
	}
	slices.Sort(result.process)
	return result, nil
}

// WithFormatLimits returns the vocabulary that also knows the limits; see
// [Vocabulary.FormatLimits]. Each format must be one this package names, and
// each kind an owner's kind the vocabulary accepts, stated once per format. A
// common kind, an in-process kind and a kind the vocabulary does not hold are
// refused: a limit on a kind no owner of the run registers would record what
// nothing reads.
func (v Vocabulary) WithFormatLimits(limits ...FormatLimit) (Vocabulary, error) {
	result := Vocabulary{owned: v.owned, process: v.process, limits: make(map[Format][]Kind, len(v.limits))}
	for format, kinds := range v.limits {
		result.limits[format] = slices.Clone(kinds)
	}
	for _, limit := range limits {
		if !slices.Contains(formats, limit.Format) {
			return Vocabulary{}, fmt.Errorf("unknown document format %q: valid formats are %s", limit.Format, tokenList(formats))
		}
		for _, kind := range limit.Kinds {
			if slices.Contains(kinds, kind) {
				return Vocabulary{}, fmt.Errorf("format %s cannot limit coverage kind %q: it is a common kind, not an owner's", limit.Format, kind)
			}
			if !slices.Contains(v.owned, kind) {
				return Vocabulary{}, fmt.Errorf("format %s cannot limit coverage kind %q: the vocabulary does not accept it", limit.Format, kind)
			}
			if slices.Contains(result.limits[limit.Format], kind) {
				return Vocabulary{}, fmt.Errorf("format %s limits coverage kind %q twice", limit.Format, kind)
			}
			result.limits[limit.Format] = append(result.limits[limit.Format], kind)
		}
		slices.Sort(result.limits[limit.Format])
	}
	return result, nil
}

// FormatLimits returns the owners' kinds a document in format has no spelling
// for, in a new sorted slice. It is nil when no owner of the vocabulary states
// a limit for the format, and always nil for the zero vocabulary.
func (v Vocabulary) FormatLimits(format Format) []Kind {
	return slices.Clone(v.limits[format])
}

// isKindName reports whether token is a lower-case name a header can spell.
func isKindName(token string) bool {
	return token != "" && token == strings.ToLower(strings.TrimSpace(token)) && !strings.ContainsAny(token, " \t\"=")
}

// InProcess reports whether kind is an in-process kind of the vocabulary:
// one a read records and an owner consults in the same process, which no
// document names. [Vocabulary.ParseKind] refuses it.
func (v Vocabulary) InProcess(kind Kind) bool {
	return slices.Contains(v.process, kind)
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
