package sqlext

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// Set is the frozen collection of extensions one parse selects. The zero
// value is unselected and refused by the frontend; [None] selects no owner.
// A Set is safe for concurrent use.
type Set struct {
	selected   bool
	extensions []Extension
}

// None returns a selected set without owners. A parse with it reads the
// shared grammar alone.
func None() Set {
	return Set{selected: true}
}

// NewSet validates and freezes extensions. It refuses an extension without an
// owner or a target, a pattern without a name, a known site or opening words,
// patterns without a parser, two patterns of one extension under one name,
// and two patterns that one document could consult at one site and that can
// match the same syntax.
func NewSet(extensions ...Extension) (Set, error) {
	set := Set{selected: true}
	for _, extension := range extensions {
		if err := validateExtension(extension); err != nil {
			return Set{}, err
		}
		extension.Targets = slices.Clone(extension.Targets)
		patterns := make([]Pattern, 0, len(extension.Patterns))
		for _, pattern := range extension.Patterns {
			pattern.Words = slices.Clone(pattern.Words)
			pattern.Next = slices.Clone(pattern.Next)
			pattern.NotNext = slices.Clone(pattern.NotNext)
			pattern.Stops = slices.Clone(pattern.Stops)
			patterns = append(patterns, pattern)
		}
		extension.Patterns = patterns
		set.extensions = append(set.extensions, extension)
	}
	if err := set.refuseOverlaps(); err != nil {
		return Set{}, err
	}
	return set, nil
}

func validateExtension(extension Extension) error {
	if strings.TrimSpace(extension.Owner) == "" {
		return fmt.Errorf("SQL extension names no owner")
	}
	if len(extension.Targets) == 0 || slices.ContainsFunc(extension.Targets, func(target string) bool { return strings.TrimSpace(target) == "" }) {
		return fmt.Errorf("SQL extension of %s names no target", extension.Owner)
	}
	if len(extension.Patterns) > 0 && extension.Parser == nil {
		return fmt.Errorf("SQL extension of %s declares patterns without a parser", extension.Owner)
	}
	names := make(map[string]bool, len(extension.Patterns))
	for _, pattern := range extension.Patterns {
		if strings.TrimSpace(pattern.Name) == "" {
			return fmt.Errorf("SQL extension of %s declares a pattern without a name", extension.Owner)
		}
		if names[pattern.Name] {
			return fmt.Errorf("%w: SQL extension of %s declares pattern %q twice", schemaext.ErrDuplicate, extension.Owner, pattern.Name)
		}
		names[pattern.Name] = true
		if !knownSite(pattern.Site) {
			return fmt.Errorf("SQL pattern %q of %s names unknown site %q", pattern.Name, extension.Owner, pattern.Site)
		}
		if len(pattern.Words) == 0 || slices.ContainsFunc(pattern.Words, blankWord) {
			return fmt.Errorf("SQL pattern %q of %s opens with no words", pattern.Name, extension.Owner)
		}
	}
	return nil
}

func knownSite(site Site) bool {
	switch site {
	case TableElement, AlterOperation, ViewClause:
		return true
	default:
		return false
	}
}

func blankWord(word string) bool {
	return strings.TrimSpace(word) == "" || strings.ContainsAny(word, " \t\r\n")
}

// refuseOverlaps refuses two patterns a document consults together at one
// site when some syntax matches both: the frontend could not say whose it is.
func (s Set) refuseOverlaps() error {
	type bound struct {
		extension int
		pattern   Pattern
	}
	var patterns []bound
	for index, extension := range s.extensions {
		for _, pattern := range extension.Patterns {
			patterns = append(patterns, bound{extension: index, pattern: pattern})
		}
	}
	for i, first := range patterns {
		for _, second := range patterns[i+1:] {
			if first.pattern.Site != second.pattern.Site || !s.consultedTogether(first.extension, first.pattern, second.extension, second.pattern) {
				continue
			}
			if overlaps(first.pattern, second.pattern) {
				return fmt.Errorf("%w: SQL pattern %q of %s and pattern %q of %s can match the same %s syntax",
					schemaext.ErrDuplicate, first.pattern.Name, s.extensions[first.extension].Owner,
					second.pattern.Name, s.extensions[second.extension].Owner, first.pattern.Site)
			}
		}
	}
	return nil
}

// consultedTogether reports whether one document consults both patterns: a
// document naming a target both extensions read, or one naming none where
// neither pattern is targeted.
func (s Set) consultedTogether(first int, a Pattern, second int, b Pattern) bool {
	if !a.Targeted && !b.Targeted {
		return true
	}
	return slices.ContainsFunc(s.extensions[first].Targets, func(target string) bool {
		return slices.Contains(s.extensions[second].Targets, target)
	})
}

// overlaps reports whether some syntax matches both patterns.
func overlaps(a, b Pattern) bool {
	shorter, longer := a, b
	if len(b.Words) < len(a.Words) {
		shorter, longer = b, a
	}
	for i, word := range shorter.Words {
		if !strings.EqualFold(word, longer.Words[i]) {
			return false
		}
	}
	if len(shorter.Words) < len(longer.Words) {
		return acceptsNext(shorter, longer.Words[len(shorter.Words)])
	}
	switch {
	case len(a.Next) > 0:
		return slices.ContainsFunc(a.Next, func(word string) bool { return acceptsNext(a, word) && acceptsNext(b, word) })
	case len(b.Next) > 0:
		return slices.ContainsFunc(b.Next, func(word string) bool { return acceptsNext(a, word) && acceptsNext(b, word) })
	default:
		// Each admits every word its NotNext leaves out, and no finite list
		// leaves out every word.
		return true
	}
}

// acceptsNext reports whether word may follow the pattern's opening words.
func acceptsNext(pattern Pattern, word string) bool {
	if containsFold(pattern.NotNext, word) {
		return false
	}
	return len(pattern.Next) == 0 || containsFold(pattern.Next, word)
}

func containsFold(words []string, word string) bool {
	return slices.ContainsFunc(words, func(candidate string) bool { return strings.EqualFold(candidate, word) })
}

// Selected reports whether the set was built by [NewSet] or [None].
func (s Set) Selected() bool {
	return s.selected
}

// Candidate is a pattern one document consults at one site, bound to the
// extension that registered it.
type Candidate struct {
	// Pattern is the pattern.
	Pattern   Pattern
	extension int
}

// Candidates returns the patterns a document naming target consults at site,
// in the order the extensions registered them. An empty target is a document
// that names none, which consults every pattern that is not targeted.
func (s Set) Candidates(target string, site Site) []Candidate {
	var candidates []Candidate
	for index, extension := range s.extensions {
		if target != "" && !slices.Contains(extension.Targets, target) {
			continue
		}
		for _, pattern := range extension.Patterns {
			if pattern.Site != site || (target == "" && pattern.Targeted) {
				continue
			}
			candidates = append(candidates, Candidate{Pattern: pattern, extension: index})
		}
	}
	return candidates
}

// Matches reports whether words, the words a document holds from the point
// the frontend is at, open the candidate's syntax.
func (c Candidate) Matches(words []string) bool {
	if len(words) < len(c.Pattern.Words) {
		return false
	}
	for i, word := range c.Pattern.Words {
		if !strings.EqualFold(word, words[i]) {
			return false
		}
	}
	if len(words) == len(c.Pattern.Words) {
		return len(c.Pattern.Next) == 0
	}
	return acceptsNext(c.Pattern, words[len(c.Pattern.Words)])
}

// Match is a claim the frontend made for a candidate.
type Match struct {
	Candidate Candidate
	Claim     Claim
}

// Parse hands each extension the claims its candidates made, in one call per
// extension, and returns the values in the order of matches. A parser that
// fails, or answers with another number of values than it was given claims,
// fails the parse.
func (s Set) Parse(ctx context.Context, target string, matches []Match) ([]Value, error) {
	values := make([]Value, len(matches))
	positions := make(map[int][]int)
	var order []int
	for position, match := range matches {
		if _, seen := positions[match.Candidate.extension]; !seen {
			order = append(order, match.Candidate.extension)
		}
		positions[match.Candidate.extension] = append(positions[match.Candidate.extension], position)
	}
	for _, index := range order {
		extension := s.extensions[index]
		request := Request{Target: target, Claims: make([]Claim, 0, len(positions[index]))}
		for _, position := range positions[index] {
			request.Claims = append(request.Claims, matches[position].Claim)
		}
		result, err := extension.Parser.ParseSQL(ctx, request)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(result.Values) != len(request.Claims) {
			return nil, fmt.Errorf("%w: SQL parser of %s answered %d claims with %d values",
				schemaext.ErrInvalidValue, extension.Owner, len(request.Claims), len(result.Values))
		}
		for i, position := range positions[index] {
			values[position] = result.Values[i]
		}
	}
	return values, nil
}

// Coverage combines the claims of the extensions that read documents of
// target, and reports whether any of them claims something. A document that
// names no target claims nothing.
func (s Set) Coverage(target string) (schemaext.Coverage, bool, error) {
	var combined schemaext.Coverage
	claimed := false
	if target == "" {
		return combined, false, nil
	}
	for _, extension := range s.extensions {
		if !slices.Contains(extension.Targets, target) || extension.Coverage == nil {
			continue
		}
		claim, err := extension.Coverage()
		if err != nil {
			return schemaext.Coverage{}, false, fmt.Errorf("coverage of %s: %w", extension.Owner, err)
		}
		if !claimed {
			combined, claimed = claim, true
			continue
		}
		if combined, err = combined.Combine(claim); err != nil {
			return schemaext.Coverage{}, false, fmt.Errorf("coverage of %s: %w", extension.Owner, err)
		}
	}
	return combined, claimed, nil
}
