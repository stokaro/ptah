// Package lintnames declares the categories every Ptah lint rule name begins
// with, the grammar of a name, and the predicate that decides whether a
// selector written as a name or a category selects a rule.
//
// A rule is identified by its name, `category/name`, everywhere: findings,
// baselines, selectors and reports. Ptah rules have no numeric code. This
// package knows no Atlas code either; the table that maps names to Atlas
// check codes lives in the Atlas compatibility layer, which only the
// compatibility surface imports.
//
// migration/lint and internal/sqllint match selectors with this package, and
// internal/lintcatalog checks the registries against the categories declared
// here, which is why the declarations live below all three.
package lintnames

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// Category is the hazard class a rule name begins with: the part before the
// slash in `destructive/drop-table`.
//
// A category names what the statement risks, not the engine it runs on; the
// engine is the rule's dialect list. An engine category is the one exception:
// a rule an executable provider publishes for a hazard only its engine has
// may be named under the engine, as `ydb/add-column-form` is.
type Category struct {
	// Name is the first segment of every rule name in the category.
	Name string
	// Summary is the one-line description of the hazard the category covers.
	Summary string
	// Engine is the only dialect whose rules the category may hold, for an
	// engine category, and empty for a hazard category.
	Engine string
}

// categories declares every category a rule name may begin with.
// internal/lintcatalog refuses a rule whose category is missing here and a
// category no rule uses.
var categories = []Category{
	{Name: "capability", Summary: "the target server version lacks a capability the statement needs"},
	{Name: "correctness", Summary: "the statement is wrong as written: malformed, naming a column the schema does not declare, " +
		"or declaring a property its body breaks"},
	{Name: "coverage", Summary: "something the linter did not read, so a clean result does not mean it was checked"},
	{Name: "data-dependent", Summary: "the statement succeeds, fails or changes values depending on the rows the table already holds"},
	{Name: "dependency", Summary: "the statement drops or renames something another object still uses"},
	{Name: "design", Summary: "a schema choice that applies cleanly and costs later, in row padding, table bloat or rows with no key to address them by; " +
		"silencing it hides advice, never a statement that fails or blocks"},
	{Name: "destructive", Summary: "the statement deletes rows, values, objects or history for good"},
	{Name: "file-form", Summary: "the migration file or its directory does not have the form the run reads"},
	{Name: "integrity", Summary: "the statement removes a constraint or a NOT NULL the database enforces on the data"},
	{Name: "locking", Summary: "the statement holds a lock that blocks reads or writes while it runs"},
	{Name: "naming", Summary: "a name breaks the configured convention, or is longer than the server keeps"},
	{Name: "online", Summary: "online mode, which the user selects, could not prove the statement runs without a lock that blocks reads or writes; " +
		"its rules fire only in that mode, so `--disable locking` leaves them on"},
	{Name: "rewrite", Summary: "the statement rewrites or copies every row of the table; an info rule here reports that the rewrite was checked for and is absent"},
	{Name: "rollout", Summary: "the change breaks a client or a consumer deployed against the current schema"},
	{Name: "security", Summary: "the statement weakens access control, or writes a secret or an unquoted value into SQL"},
	{Name: "side-effect", Summary: "the statement resets, ignores or leaves behind something its text does not show; " +
		"silenced, a migration can succeed and leave the database different from what it says"},
	{Name: "transaction", Summary: "the statement cannot run, or runs differently, inside the migration's transaction or query"},
	{Name: "ydb", Summary: "a statement form only YDB refuses", Engine: "ydb"},
}

// Categories returns every declared category, ordered by name. The slice is a
// copy.
func Categories() []Category { return slices.Clone(categories) }

// CategoryFor returns the declared category called name.
func CategoryFor(name string) (Category, bool) {
	index := slices.IndexFunc(categories, func(category Category) bool { return category.Name == name })
	if index < 0 {
		return Category{}, false
	}
	return categories[index], true
}

// namePattern is the grammar of a rule name: two lowercase segments, each a
// run of letters and digits joined by single hyphens, with a slash between
// them. It cannot match a rule code or an Atlas check code, which are upper
// case.
//
// The second segment follows the SQL spelling of the statement it reports, so
// a rule about NO FORCE ROW LEVEL SECURITY is
// `security/no-force-row-level-security`, not a paraphrase of it.
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*/[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// WellFormed reports whether name follows the rule-name grammar. It does not
// say the category is declared; see [CategoryFor].
func WellFormed(name string) bool { return namePattern.MatchString(name) }

// CategoryOf returns the category segment of a rule name, or the empty string
// when the name has no slash.
func CategoryOf(name string) string {
	category, _, found := strings.Cut(name, "/")
	if !found {
		return ""
	}
	return category
}

// IsSelector reports whether selector is written as a name rather than as a
// code: it holds a slash and a lower-case letter, so it names a rule
// (`locking/set-not-null`) or a category (`locking/`), or it is a declared
// category's name (`locking`).
//
// Every other selector is a code or a code prefix and is matched by the
// linter's own code rule. The split is total and the two sides cannot meet: a
// name holds a lower-case letter, and a code is upper case. Upper-case codes
// joined by a slash, `DS101/DS102`, and a slash on its own stay on the code
// side, where a nolint directive splits them at the slash. The selector is
// read as written; a caller that trims whitespace trims it first.
func IsSelector(selector string) bool {
	_, _, ok := parse(selector)
	return ok
}

// parse reads a name-space selector. It returns the rule name it names, or
// the category, and false for a selector written as a code.
func parse(selector string) (name, category string, ok bool) {
	if _, declared := CategoryFor(selector); declared {
		return "", selector, true
	}
	if !strings.Contains(selector, "/") || !strings.ContainsFunc(selector, unicode.IsLower) {
		return "", "", false
	}
	if prefix, found := strings.CutSuffix(selector, "/"); found {
		return "", prefix, true
	}
	return selector, "", true
}

// Selects reports whether selector selects the rule called ruleName: the
// selector is the rule's name, or the category its name begins with, written
// `locking` or `locking/`. This is the one predicate every selector of every
// linter goes through: a flag, a configuration file, a baseline and a
// `ptah:nolint` comment.
//
// There is no prefix match. `locking/set-not` selects no rule, and `lock`
// is not a category, so a selector cannot reach a rule its author did not
// spell.
func Selects(selector, ruleName string) bool {
	name, category, ok := parse(selector)
	switch {
	case !ok || ruleName == "":
		return false
	case name != "":
		return name == ruleName
	default:
		return category == CategoryOf(ruleName)
	}
}

// ConfigKeys returns the keys of a `rules:` section that name the rule alone,
// which is its name.
func ConfigKeys(ruleName string) []string { return []string{ruleName} }

// GroupKeys returns the keys that name the rule's category, written as
// `locking` and as `locking/`. Every linter that reads a `rules:` section
// resolves a category key through this one function, so the two spellings
// cannot drift apart.
func GroupKeys(ruleName string) []string {
	category := CategoryOf(ruleName)
	if category == "" {
		return nil
	}
	return []string{category, category + "/"}
}

// Set is the rule names one linter run holds. It judges selectors against
// them, so a selector that reaches no rule of the run is refused rather than
// read as a rule turned off. Its zero value holds no rule.
type Set struct {
	linter string
	names  []string
}

// NewSet returns the set of names, sorted and without repeats. linter names
// the linter for refusals, such as "migration lint".
func NewSet(linter string, names []string) Set {
	return Set{linter: linter, names: slices.Compact(slices.Sorted(slices.Values(names)))}
}

// Names returns the names the set holds, sorted.
func (s Set) Names() []string { return slices.Clone(s.names) }

// Has reports whether the set holds the name.
func (s Set) Has(name string) bool {
	_, found := slices.BinarySearch(s.names, name)
	return found
}

// Resolve returns the names a name-space selector selects, sorted. It refuses
// a selector naming no rule of the set: a name it does not hold, a category
// that is not declared, and a declared category none of its rules is named
// under. A code-shaped selector is refused as [RefuseCode] says.
func (s Set) Resolve(selector string) ([]string, error) {
	if IsCode(selector) {
		return nil, RefuseCode(selector)
	}
	name, category, ok := parse(selector)
	if !ok {
		return nil, fmt.Errorf("%q is not a rule name or category; lint rules are named category/name, such as %s", selector, exampleName)
	}
	var selected []string
	for _, candidate := range s.names {
		if Selects(selector, candidate) {
			selected = append(selected, candidate)
		}
	}
	if len(selected) > 0 {
		return selected, nil
	}
	if name != "" {
		return nil, fmt.Errorf("%q is not the name of a %s rule; a name is a category and a rule joined by a slash, such as %s",
			selector, s.linter, exampleName)
	}
	if _, declared := CategoryFor(category); !declared {
		return nil, fmt.Errorf("%q is not a rule category; the categories are %s", selector, categoryList())
	}
	return nil, fmt.Errorf("%q is a category that holds no %s rule", selector, s.linter)
}

// exampleName is the rule name refusals show.
const exampleName = "locking/add-foreign-key"

// RefuseCode is the refusal of a word spelled as a rule code, such as DS102.
// Ptah rules are named, and the message names no rule the code might once
// have stood for: native Ptah holds no table from codes to rules.
func RefuseCode(word string) error {
	return fmt.Errorf("lint rules are named category/name; %s is not a rule name", word)
}

// MeantAsSelector reports whether a word of a nolint directive's free-form
// line is written as a rule name or category with a slash, so that a wrong one
// is an error rather than prose. A word is meant as a selector when its first
// segment is a declared category, or it is one lower-case segment and a
// slash. A bare category is not: the caller decides where one may stand. A
// URL, `and/or` and `dba/ops` are none of these.
func MeantAsSelector(word string) bool {
	if _, declared := CategoryFor(word); declared {
		return false
	}
	category, rest, found := strings.Cut(word, "/")
	if !found {
		return false
	}
	if _, declared := CategoryFor(category); declared {
		return true
	}
	return rest == "" && WellFormedSegment(category)
}

// WellFormedSegment reports whether segment is one lower-case name segment:
// letters and digits joined by single hyphens.
func WellFormedSegment(segment string) bool { return segmentPattern.MatchString(segment) }

var segmentPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// IsCode reports whether word is spelled as a rule code or a code prefix: an
// upper-case ASCII letter followed by upper-case letters and digits.
func IsCode(word string) bool {
	for index, value := range word {
		if value >= 'A' && value <= 'Z' {
			continue
		}
		if index > 0 && value >= '0' && value <= '9' {
			continue
		}
		return false
	}
	return word != ""
}

// ValidateSelector judges a selector written in a flag or a configuration
// file, the way every linter does before it runs. A selector is a rule name
// or a category the set holds. A code-shaped word is refused with
// [RefuseCode], and anything else is refused: matched against nothing it
// would read as a rule turned off.
//
// The error begins with the quoted selector or the refused code, so a caller
// prefixes it with the place the selector was written.
func (s Set) ValidateSelector(selector string) error {
	switch {
	case selector == "":
		return nil
	case IsCode(selector):
		return RefuseCode(selector)
	case IsSelector(selector):
		_, err := s.Resolve(selector)
		return err
	default:
		return fmt.Errorf("%q is not a rule name or category; lint rules are named category/name, such as %s or %s",
			selector, exampleName, CategoryOf(exampleName))
	}
}

// CategoryNames returns the declared category names, ordered.
func CategoryNames() []string {
	names := make([]string, 0, len(categories))
	for _, category := range categories {
		names = append(names, category.Name)
	}
	return names
}

// categoryList joins the declared category names for a refusal.
func categoryList() string { return strings.Join(CategoryNames(), ", ") }

// ValidateGrammar judges a selector or a declared rule's name by form alone,
// without a set of rules to look it up in: a configuration file is read before
// the run knows which rules it has. A code-shaped word is refused as
// [RefuseCode] says; a name or category must be well formed and begin with a
// declared category.
func ValidateGrammar(selector string) error {
	switch {
	case selector == "":
		return nil
	case IsCode(selector):
		return RefuseCode(selector)
	}
	name, category, ok := parse(selector)
	if !ok {
		return fmt.Errorf("%q is not a rule name or category; lint rules are named category/name, such as %s or %s",
			selector, exampleName, CategoryOf(exampleName))
	}
	if name != "" {
		category = CategoryOf(name)
		if !WellFormed(name) {
			return fmt.Errorf("%q is not a rule name; a name is a category and a rule joined by a slash, such as %s",
				selector, exampleName)
		}
	}
	if _, declared := CategoryFor(category); !declared {
		return fmt.Errorf("%q is not a rule category; the categories are %s", selector, categoryList())
	}
	return nil
}
