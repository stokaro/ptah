package sqlext

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// shiftedNamesOverlap compares patterns whose names start at different
// positions, such as ALTER TABLE t ENABLE and ALTER TABLE ONLY t ENABLE.
// A name consumes an identifier followed by zero or more dot/identifier
// pairs. An overlap has a witness whose name lengths are bounded by the
// fixed words: any additional pairs consumed by both names can be removed
// without changing either pattern's match.
func shiftedNamesOverlap(a, b Pattern) bool {
	limit := len(a.Words) + len(a.AfterName) + len(b.Words) + len(b.AfterName) + 1
	// Each shape of b is compared with every shape of a, so it is built once.
	right := make([][]nameSymbol, limit)
	for parts := 1; parts <= limit; parts++ {
		right[parts-1] = nameShape(b, parts)
	}
	for leftParts := 1; leftParts <= limit; leftParts++ {
		left := nameShape(a, leftParts)
		for _, shape := range right {
			if shapesOverlap(left, shape) {
				return true
			}
		}
	}
	return false
}

// nameSymbol is a fixed word, a name part, or the constraint on the first
// word after a pattern. Beyond its shape a pattern places no constraints.
type nameSymbol struct {
	word          string
	identifier    bool
	next, notNext []string
}

func nameShape(pattern Pattern, parts int) []nameSymbol {
	shape := make([]nameSymbol, 0, len(pattern.Words)+2*parts+len(pattern.AfterName)+1)
	for _, word := range pattern.Words {
		shape = append(shape, nameSymbol{word: word})
	}
	shape = append(shape, nameSymbol{identifier: true})
	for i := 1; i < parts; i++ {
		shape = append(shape, nameSymbol{word: "."}, nameSymbol{identifier: true})
	}
	for _, word := range pattern.AfterName {
		shape = append(shape, nameSymbol{word: word})
	}
	if len(pattern.Next) > 0 || len(pattern.NotNext) > 0 {
		shape = append(shape, nameSymbol{next: pattern.Next, notNext: pattern.NotNext})
	}
	return shape
}

func shapesOverlap(a, b []nameSymbol) bool {
	for i := range min(len(a), len(b)) {
		if !symbolsOverlap(a[i], b[i]) {
			return false
		}
	}
	return true
}

func symbolsOverlap(a, b nameSymbol) bool {
	switch {
	case a.word != "":
		return b.accepts(a.word)
	case b.word != "":
		return a.accepts(b.word)
	case len(a.next) > 0:
		return slices.ContainsFunc(a.next, func(word string) bool { return a.accepts(word) && b.accepts(word) })
	case len(b.next) > 0:
		return slices.ContainsFunc(b.next, func(word string) bool { return a.accepts(word) && b.accepts(word) })
	default:
		// Both admit infinitely many identifiers, except for finite exclusions.
		return true
	}
}

func (s nameSymbol) accepts(word string) bool {
	if s.word != "" {
		return strings.EqualFold(s.word, word)
	}
	if s.identifier {
		first, _ := utf8.DecodeRuneInString(word)
		return unicode.IsLetter(first) || first == '_' || first == '"' || first == '[' || first == '`'
	}
	return !containsFold(s.notNext, word) && (len(s.next) == 0 || containsFold(s.next, word))
}
