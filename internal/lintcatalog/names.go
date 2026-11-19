package lintcatalog

import (
	"fmt"
	"slices"

	"ptah.run/internal/lintnames"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerpublished"
)

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

// Category returns the category segment of the rule's Ptah name, or the empty
// string when the name has no slash.
func (e Entry) Category() string { return lintnames.CategoryOf(e.Name) }

// nameProblems returns every way entries break the naming rule [Entry.Name]
// and [lintnames.Category] state. A dialect list that is empty means every dialect, so a
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
	case !lintnames.WellFormed(entry.Name):
		return []string{fmt.Sprintf("rule %s is named %q, which is not two lowercase hyphenated segments joined by a slash", entry.Code, entry.Name)}
	}
	category, declared := lintnames.CategoryFor(entry.Category())
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
	for _, category := range lintnames.Categories() {
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
