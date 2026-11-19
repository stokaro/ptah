package lintcatalog

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerpublished"
)

// Category is the hazard class a rule's Ptah name begins with: the part before
// the slash in `destructive/drop-table`.
//
// A category names what the statement risks, not the engine it runs on; the
// engine is the rule's [Entry.Dialects]. An engine category is the one
// exception: a rule an executable provider publishes for a hazard only its
// engine has may be named under the engine, as `ydb/add-column-form` is.
type Category struct {
	// Name is the first segment of every rule name in the category.
	Name string
	// Summary is the one-line description of the hazard the category covers.
	Summary string
	// Engine is the only dialect whose rules the category may hold, for an
	// engine category, and empty for a hazard category.
	Engine string
}

// categories declares every category a rule name may begin with. [Validate]
// refuses a rule whose category is missing here and a category no rule uses.
var categories = []Category{
	{Name: "capability", Summary: "the target server version lacks a capability the statement needs"},
	{Name: "correctness", Summary: "the server accepts the statement, and the result is not what the statement declares"},
	{Name: "coverage", Summary: "something the linter did not read, so a clean result does not mean it was checked"},
	{Name: "data-dependent", Summary: "the statement succeeds, fails or changes values depending on the rows the table already holds"},
	{Name: "dependency", Summary: "the statement drops or renames something another object still uses"},
	{Name: "design", Summary: "a schema shape whose cost is paid later rather than when the migration runs"},
	{Name: "destructive", Summary: "the statement deletes rows, values, objects or history for good"},
	{Name: "file-form", Summary: "the migration file or its directory does not have the form the run reads"},
	{Name: "integrity", Summary: "the statement removes a constraint or a NOT NULL the database enforces on the data"},
	{Name: "locking", Summary: "the statement holds a lock that blocks reads or writes while it runs"},
	{Name: "naming", Summary: "a name breaks the configured convention, or is longer than the server keeps"},
	{Name: "online", Summary: "online mode could not prove the statement runs without a lock that blocks reads or writes"},
	{Name: "parse", Summary: "the linter cannot read the statement"},
	{Name: "rewrite", Summary: "the statement rewrites or copies every row of the table"},
	{Name: "rollout", Summary: "the change breaks a client or a consumer deployed against the current schema"},
	{Name: "security", Summary: "the statement weakens access control, or writes a secret or an unquoted value into SQL"},
	{Name: "side-effect", Summary: "the statement has an effect it does not state, or the server keeps less than it says"},
	{Name: "transaction", Summary: "the statement cannot run, or runs differently, inside the migration's transaction or query"},
	{Name: "ydb", Summary: "a statement form only YDB refuses", Engine: "ydb"},
}

// Categories returns every declared category, ordered by name.
func Categories() []Category { return slices.Clone(categories) }

// categoryFor returns the declared category called name.
func categoryFor(name string) (Category, bool) {
	index := slices.IndexFunc(categories, func(category Category) bool { return category.Name == name })
	if index < 0 {
		return Category{}, false
	}
	return categories[index], true
}

// Owner says who publishes a rule.
type Owner string

const (
	// OwnerHost marks a rule Ptah itself carries: migration lint's own rules,
	// the rules a bundled engine owner publishes, and SQL lint's.
	OwnerHost Owner = "host"
	// OwnerProvider marks a rule an executable provider publishes, as
	// internal/providerpublished records the official providers' catalogs.
	OwnerProvider Owner = "provider"
)

// namePattern is the grammar of a Ptah rule name: two lowercase segments,
// each a run of letters and digits joined by single hyphens, with a slash
// between them. It cannot match an Atlas check code, which is upper case.
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*/[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// Category returns the category segment of the rule's Ptah name, or the empty
// string when the name has no slash.
func (e Entry) Category() string {
	category, _, found := strings.Cut(e.Name, "/")
	if !found {
		return ""
	}
	return category
}

// nameProblems returns every way entries break the naming rule [Entry.Name]
// and [Category] state. A dialect list that is empty means every dialect, so a
// rule for every dialect shares its name with no other rule of its linter.
func nameProblems(entries []Entry) []string {
	var problems []string
	for _, entry := range entries {
		problems = append(problems, entryNameProblems(entry)...)
	}
	return append(problems, sharedNameProblems(entries)...)
}

// entryNameProblems checks the name of one entry on its own.
func entryNameProblems(entry Entry) []string {
	switch {
	case entry.Name == "":
		return []string{fmt.Sprintf("rule %s has no Ptah name", entry.Code)}
	case !namePattern.MatchString(entry.Name):
		return []string{fmt.Sprintf("rule %s is named %q, which is not two lowercase hyphenated segments joined by a slash", entry.Code, entry.Name)}
	}
	category, declared := categoryFor(entry.Category())
	switch {
	case !declared:
		return []string{fmt.Sprintf("rule %s is named %s, whose category %q is not declared", entry.Code, entry.Name, entry.Category())}
	case category.Engine != "" && (entry.Owner != OwnerProvider || !slices.Equal(entry.Dialects, []string{category.Engine})):
		return []string{fmt.Sprintf("rule %s is named %s under the engine category %q, which holds only rules a provider publishes for %s alone",
			entry.Code, entry.Name, category.Name, category.Engine)}
	}
	return nil
}

// sharedNameProblems checks the entries that share a name against each other.
func sharedNameProblems(entries []Entry) []string {
	var problems []string
	for index, entry := range entries {
		for _, other := range entries[index+1:] {
			if entry.Name == "" || entry.Name != other.Name {
				continue
			}
			if entry.Severity != other.Severity {
				problems = append(problems, fmt.Sprintf("rules %s and %s share the name %s with different severities, %s and %s",
					entry.Code, other.Code, entry.Name, entry.Severity, other.Severity))
			}
			if entry.Kind == other.Kind && !disjointDialects(entry.Dialects, other.Dialects) {
				problems = append(problems, fmt.Sprintf("rules %s and %s share the name %s on overlapping dialects (%s; %s): "+
					"one name is one hazard, checked at most once on each dialect", entry.Code, other.Code, entry.Name, entry.DialectsLabel(), other.DialectsLabel()))
			}
		}
	}
	return problems
}

// disjointDialects reports whether two dialect lists name dialects and share
// none. An empty list means every dialect, so it overlaps every other list.
func disjointDialects(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return !slices.ContainsFunc(a, func(dialect string) bool { return slices.Contains(b, dialect) })
}

// unusedCategoryProblems reports a declared category no entry is named under,
// which is a declaration nothing is held to.
func unusedCategoryProblems(entries []Entry) []string {
	var problems []string
	for _, category := range categories {
		if !slices.ContainsFunc(entries, func(entry Entry) bool { return entry.Category() == category.Name }) {
			problems = append(problems, fmt.Sprintf("category %s is declared and names no rule", category.Name))
		}
	}
	return problems
}

// providerOwnedCodes returns the code of every rule an official provider's
// recorded catalog publishes.
func providerOwnedCodes() (map[string]bool, error) {
	descriptions, err := providerpublished.Descriptions()
	if err != nil {
		return nil, err
	}
	owned := make(map[string]bool)
	for _, description := range descriptions {
		for _, engine := range description.GetEngines() {
			for _, rule := range providerhost.PublishedLintRules(engine) {
				owned[rule.Code] = true
			}
		}
	}
	return owned, nil
}

// ownerOf returns who publishes the rule code.
func ownerOf(code string, providerOwned map[string]bool) Owner {
	if providerOwned[code] {
		return OwnerProvider
	}
	return OwnerHost
}
