package sqlext

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
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
// owner or a target, an object or facet kind that is not valid, patterns at a
// site that takes facets without facet kinds, a pattern without a name, a
// known site or opening words, patterns without a parser,
// two patterns of one extension under one name, and two patterns that one
// document could consult at one site and that can match the same syntax.
func NewSet(extensions ...Extension) (Set, error) {
	set := Set{selected: true}
	for _, extension := range extensions {
		if err := validateExtension(extension); err != nil {
			return Set{}, err
		}
		extension.Targets = slices.Clone(extension.Targets)
		extension.Header = slices.Clone(extension.Header)
		extension.Kinds = slices.Clone(extension.Kinds)
		extension.Facets = slices.Clone(extension.Facets)
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
	if err := set.refuseSecondGrammar(); err != nil {
		return Set{}, err
	}
	return set, nil
}

// refuseSecondGrammar refuses two grammars for one target, and owner patterns
// for a target whose documents a grammar reads: the shared grammar the
// patterns extend never reads them.
func (s Set) refuseSecondGrammar() error {
	grammars := make(map[string]string)
	for _, extension := range s.extensions {
		if extension.Grammar == nil {
			continue
		}
		for _, target := range extension.Targets {
			if owner, found := grammars[target]; found {
				return fmt.Errorf("%w: SQL grammar of target %q is registered by %s and %s",
					schemaext.ErrDuplicate, target, owner, extension.Owner)
			}
			grammars[target] = extension.Owner
		}
	}
	for _, extension := range s.extensions {
		for _, target := range extension.Targets {
			if owner, found := grammars[target]; found && len(extension.Patterns) > 0 {
				return fmt.Errorf("SQL extension of %s declares patterns for target %q, whose documents the grammar of %s reads",
					extension.Owner, target, owner)
			}
		}
	}
	return nil
}

// Grammar returns the grammar that reads documents of target, and false for a
// target written in the shared grammar.
func (s Set) Grammar(target string) (Grammar, bool) {
	if target == "" {
		return nil, false
	}
	for _, extension := range s.extensions {
		if extension.Grammar != nil && slices.Contains(extension.Targets, target) {
			return extension.Grammar, true
		}
	}
	return nil, false
}

// ReadsHeader reports whether an owner of target reads a header entry of kind
// as a limit on its own claim; see [Extension.Header]. A document that names
// no target has no owner header.
func (s Set) ReadsHeader(target string, kind coverage.Kind) bool {
	if target == "" {
		return false
	}
	return slices.ContainsFunc(s.extensions, func(extension Extension) bool {
		return slices.Contains(extension.Targets, target) && containsKind(extension.Header, kind)
	})
}

// containsKind compares kinds as a header spells them, ignoring case and
// surrounding space. It lowers both rather than fold them: [strings.EqualFold]
// also matches U+017F to "s", which would read a kind no owner records as one
// of its own and drop it.
// claimsFor reports whether extension's claim holds for a document of
// target. A document that names no target reads only untargeted patterns, so
// it makes the claim of an extension that reads it through patterns alone,
// at least one, every one untargeted: it can write all the extension reads,
// and the claim is about that. The PostgreSQL extension owner's statements
// are such. An extension with a targeted pattern, a grammar, or no syntax at
// all claims nothing there.
func claimsFor(extension Extension, target string) bool {
	if target != "" {
		return slices.Contains(extension.Targets, target)
	}
	if extension.Grammar != nil || len(extension.Patterns) == 0 {
		return false
	}
	for _, pattern := range extension.Patterns {
		if pattern.Targeted {
			return false
		}
	}
	return true
}

func containsKind(kinds []coverage.Kind, kind coverage.Kind) bool {
	want := strings.ToLower(strings.TrimSpace(string(kind)))
	return slices.ContainsFunc(kinds, func(candidate coverage.Kind) bool {
		return strings.ToLower(strings.TrimSpace(string(candidate))) == want
	})
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
	if extension.Grammar != nil && len(extension.Patterns) > 0 {
		return fmt.Errorf("SQL extension of %s declares patterns beside a grammar", extension.Owner)
	}
	for _, kind := range extension.Kinds {
		if !kind.Valid() {
			return fmt.Errorf("SQL extension of %s declares invalid object kind %q", extension.Owner, kind)
		}
	}
	for _, kind := range extension.Facets {
		if !kind.Valid() {
			return fmt.Errorf("SQL extension of %s declares invalid facet kind %q", extension.Owner, kind)
		}
	}
	if len(extension.Facets) == 0 && slices.ContainsFunc(extension.Patterns, func(pattern Pattern) bool { return FacetSite(pattern.Site) }) {
		return fmt.Errorf("SQL extension of %s declares patterns at a site that takes facets, and no facet kinds", extension.Owner)
	}
	return validatePatterns(extension)
}

// validatePatterns refuses a pattern without a name, a name declared twice, an
// unknown site, missing or blank opening words, and words after a name where
// the pattern does not read one.
func validatePatterns(extension Extension) error {
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
		if len(pattern.AfterName) > 0 && (pattern.Site != Statement || slices.ContainsFunc(pattern.AfterName, blankWord)) {
			return fmt.Errorf("SQL pattern %q of %s names words after a name, which only a statement pattern takes, and each a word",
				pattern.Name, extension.Owner)
		}
	}
	return nil
}

// FacetSite reports whether a value at site carries the owner's facets of its
// subject and nothing else: [ViewClause], [TableOption], [ColumnOption] and
// [IndexOption].
func FacetSite(site Site) bool {
	switch site {
	case ViewClause, TableOption, ColumnOption, IndexOption:
		return true
	default:
		return false
	}
}

func knownSite(site Site) bool {
	switch site {
	case TableElement, AlterOperation, ViewClause, TableOption, ColumnOption, IndexOption, Statement:
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

// overlaps reports whether some syntax matches both patterns. A name stands
// after the opening words of a pattern with AfterName, and a name is any
// identifier: a pattern without AfterName whose opening words end where the
// name begins matches some of what the other matches, whatever its Next.
func overlaps(a, b Pattern) bool {
	if len(a.AfterName) > 0 || len(b.AfterName) > 0 {
		return overlapsAroundName(a, b)
	}
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

// overlapsAroundName is [overlaps] for two patterns at least one of which
// reads a name after its opening words. Where the two differ in a word both
// fix, nothing matches both; where one leaves a word to the name, any word
// may stand there.
func overlapsAroundName(a, b Pattern) bool {
	named, other := a, b
	if len(a.AfterName) == 0 {
		named, other = b, a
	}
	common := min(len(named.Words), len(other.Words))
	for i := range common {
		if !strings.EqualFold(named.Words[i], other.Words[i]) {
			return false
		}
	}
	switch {
	case len(other.Words) < len(named.Words):
		return acceptsNext(other, named.Words[len(other.Words)])
	case len(other.AfterName) == 0:
		// The other's next word, or its next opening word, is the name.
		return true
	case len(other.Words) != len(named.Words):
		// Both read a name, at different words; the one that reads it later
		// takes the earlier one's name as an opening word. Refuse rather
		// than reason about names that are keywords.
		return true
	}
	shorter, longer := named, other
	if len(other.AfterName) < len(named.AfterName) {
		shorter, longer = other, named
	}
	for i, word := range shorter.AfterName {
		if !strings.EqualFold(word, longer.AfterName[i]) {
			return false
		}
	}
	if len(shorter.AfterName) < len(longer.AfterName) {
		return acceptsNext(shorter, longer.AfterName[len(shorter.AfterName)])
	}
	switch {
	case len(a.Next) > 0:
		return slices.ContainsFunc(a.Next, func(word string) bool { return acceptsNext(a, word) && acceptsNext(b, word) })
	case len(b.Next) > 0:
		return slices.ContainsFunc(b.Next, func(word string) bool { return acceptsNext(a, word) && acceptsNext(b, word) })
	default:
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
// the frontend is at, open the candidate's syntax. A pattern with AfterName
// never matches so: the frontend reads the name and asks [Candidate.MatchesNamed].
func (c Candidate) Matches(words []string) bool {
	if len(c.Pattern.AfterName) > 0 || len(words) < len(c.Pattern.Words) {
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

// MatchesNamed reports whether the candidate's syntax opens with before and
// continues, after the qualified name the frontend read, with after: before
// holds the pattern's opening words, and after begins with its AfterName and
// the word one of Next must be. A pattern without AfterName never matches so.
func (c Candidate) MatchesNamed(before, after []string) bool {
	pattern := c.Pattern
	if len(pattern.AfterName) == 0 || len(before) != len(pattern.Words) || len(after) < len(pattern.AfterName) {
		return false
	}
	for i, word := range pattern.Words {
		if !strings.EqualFold(word, before[i]) {
			return false
		}
	}
	for i, word := range pattern.AfterName {
		if !strings.EqualFold(word, after[i]) {
			return false
		}
	}
	if len(after) == len(pattern.AfterName) {
		return len(pattern.Next) == 0
	}
	return acceptsNext(pattern, after[len(pattern.AfterName)])
}

// Match is a claim the frontend made for a candidate.
type Match struct {
	Candidate Candidate
	Claim     Claim
}

// Parse hands each extension the claims its candidates made, in one call per
// extension, with the objects of declared its statements may amend, and
// returns the values in the order of matches. declared is what the documents
// read before this one declared. A parser that fails, answers with another
// number of values than it was given claims, or answers a claim with a value
// its site does not take, fails the parse.
func (s Set) Parse(ctx context.Context, target string, matches []Match, declared schemaext.Objects) ([]Value, error) {
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
		owned, err := ownedObjects(extension, declared)
		if err != nil {
			return nil, err
		}
		request := Request{Target: target, Claims: make([]Claim, 0, len(positions[index])), Declared: owned}
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
			if err := checkValue(extension, request.Claims[i], result.Values[i]); err != nil {
				return nil, err
			}
			values[position] = result.Values[i]
		}
	}
	return values, nil
}

// ownedObjects returns the objects of declared whose kind is one of the
// extension's Kinds, in identity order.
func ownedObjects(extension Extension, declared schemaext.Objects) ([]schemaext.Object, error) {
	if len(extension.Kinds) == 0 || declared.IsZero() {
		return nil, nil
	}
	all, err := declared.Select(func(ref objectidentity.ID) bool {
		return slices.Contains(extension.Kinds, schemaext.Kind(ref.Kind))
	}).All()
	if err != nil || len(all) == 0 {
		return nil, err
	}
	return all, nil
}

// checkValue refuses a value that does not fit its claim's site: a statement
// carries the owner's statement or the table it declares, one and not both,
// only a statement declares a table or declares or amends objects, a
// statement that declares a table names it and declares and amends nothing
// else, a site that takes facets alone takes no index and no operation, every
// facet is of one of the extension's Facets kinds, and every object is of one
// of its Kinds.
func checkValue(extension Extension, claim Claim, value Value) error {
	if value.Declined {
		return checkDeclined(extension, claim, value)
	}
	for _, kind := range value.Facets.Kinds() {
		if !slices.Contains(extension.Facets, kind) {
			return fmt.Errorf("%w: SQL parser of %s answered a facet of kind %q, which its extension does not declare",
				schemaext.ErrInvalidValue, extension.Owner, kind)
		}
	}
	if claim.Site != Statement {
		if value.Statement != nil || value.Table != nil || len(value.Objects) > 0 || len(value.Replaces) > 0 {
			return fmt.Errorf("%w: SQL parser of %s answered its %s claim %q with a statement's value",
				schemaext.ErrInvalidValue, extension.Owner, claim.Site, claim.Pattern)
		}
		if FacetSite(claim.Site) && (value.Index != nil || value.Operation != nil) {
			return fmt.Errorf("%w: SQL parser of %s answered its %s claim %q with an index or an operation, which the site does not take",
				schemaext.ErrInvalidValue, extension.Owner, claim.Site, claim.Pattern)
		}
		return nil
	}
	if err := checkStatementValue(extension, claim, value); err != nil {
		return err
	}
	for _, object := range slices.Concat(value.Objects, value.Replaces) {
		if !slices.Contains(extension.Kinds, schemaext.Kind(object.Ref.Kind)) {
			return fmt.Errorf("%w: SQL parser of %s answered an object of kind %q, which its extension does not declare",
				schemaext.ErrInvalidValue, extension.Owner, object.Ref.Kind)
		}
	}
	return nil
}

// checkDeclined refuses a decline of a claim other than a statement, which
// the frontend could not read again by itself, and a decline that carries a
// value besides.
func checkDeclined(extension Extension, claim Claim, value Value) error {
	if claim.Site != Statement {
		return fmt.Errorf("%w: SQL parser of %s declined its %s claim %q; only a statement claim may decline",
			schemaext.ErrInvalidValue, extension.Owner, claim.Site, claim.Pattern)
	}
	if value.Index != nil || value.Operation != nil || !value.Facets.IsZero() || value.Statement != nil || value.Table != nil ||
		len(value.Objects) > 0 || len(value.Replaces) > 0 {
		return fmt.Errorf("%w: SQL parser of %s declined its statement claim %q and answered a value beside it",
			schemaext.ErrInvalidValue, extension.Owner, claim.Pattern)
	}
	return nil
}

// checkStatementValue refuses a statement's value that carries neither the
// owner's statement nor a table, or both, and a table without a name or with
// objects beside it.
func checkStatementValue(extension Extension, claim Claim, value Value) error {
	var problem string
	switch {
	case value.Statement == nil && value.Table == nil:
		problem = "no statement"
	case value.Statement != nil && value.Table != nil:
		problem = "both a statement and a table"
	case value.Table != nil && strings.TrimSpace(value.Table.Name) == "":
		problem = "a table without a name"
	case value.Table != nil && (len(value.Objects) > 0 || len(value.Replaces) > 0):
		problem = "a table and objects beside it"
	default:
		return nil
	}
	return fmt.Errorf("%w: SQL parser of %s answered its statement claim %q with %s",
		schemaext.ErrInvalidValue, extension.Owner, claim.Pattern, problem)
}

// Coverage combines the claims of the extensions that read documents of
// target, each handed the entries of header whose kind it reads, and reports
// whether any of them claims something. A document that names no target
// claims what an extension claims only where it reads all of that
// extension's syntax; see [claimsFor].
func (s Set) Coverage(target string, header []coverage.Object) (schemaext.Coverage, bool, error) {
	var combined schemaext.Coverage
	claimed := false
	for _, extension := range s.extensions {
		if !claimsFor(extension, target) || extension.Coverage == nil {
			continue
		}
		var limits []coverage.Object
		for _, entry := range header {
			if containsKind(extension.Header, entry.Kind) {
				limits = append(limits, entry)
			}
		}
		claim, err := extension.Coverage(limits)
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
