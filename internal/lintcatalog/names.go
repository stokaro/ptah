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
// and [lintnames.Category] state.
func nameProblems(entries []Entry) []string {
	var problems []string
	for _, entry := range entries {
		problems = append(problems, entryNameProblems(entry)...)
	}
	return append(problems, sharedNameProblems(entries)...)
}

// entryNameProblems checks the name of one entry on its own.
func entryNameProblems(entry Entry) []string {
	if !lintnames.WellFormed(entry.Name) {
		return []string{fmt.Sprintf("rule %q is not two lowercase hyphenated segments joined by a slash", entry.Name)}
	}
	category, declared := lintnames.CategoryFor(entry.Category())
	switch {
	case !declared:
		return []string{fmt.Sprintf("rule %s names a category %q that is not declared", entry.Name, entry.Category())}
	case category.Engine != "" && (entry.Owner != OwnerProvider || !slices.Equal(entry.Dialects, []string{category.Engine})):
		return []string{fmt.Sprintf("rule %s is under the engine category %q, which holds only rules a provider publishes for %s alone",
			entry.Name, category.Name, category.Engine)}
	}
	return nil
}

// sharedNameProblems checks the entries that share a name against each other.
// Each linter holds one entry per name, so these are a name both linters
// report, and the two must agree on its default severity.
func sharedNameProblems(entries []Entry) []string {
	var problems []string
	for index, entry := range entries {
		for _, other := range entries[index+1:] {
			if entry.Name != other.Name {
				continue
			}
			if entry.Kind == other.Kind {
				problems = append(problems, fmt.Sprintf("rule %s has two entries in %s lint", entry.Name, entry.Kind))
			}
			if entry.Severity != other.Severity {
				problems = append(problems, fmt.Sprintf("rule %s is %s in %s lint and %s in %s lint; one name has one default severity",
					entry.Name, entry.Severity, entry.Kind, other.Severity, other.Kind))
			}
		}
	}
	return problems
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

// providerOwnedNames returns, for every rule name an official provider's
// recorded catalog publishes, the engines it publishes it for.
func providerOwnedNames() (map[string][]string, error) {
	descriptions, err := providerpublished.Descriptions()
	if err != nil {
		return nil, err
	}
	owned := make(map[string][]string)
	for _, description := range descriptions {
		for _, engine := range description.GetEngines() {
			for _, rule := range providerhost.PublishedLintRules(engine) {
				owned[rule.Name] = append(owned[rule.Name], rule.Targets...)
			}
		}
	}
	return owned, nil
}

// ownerOf returns who publishes the rule: the provider when it judges only
// engines a provider publishes the name for, and the host otherwise.
func ownerOf(name string, dialects []string, providerOwned map[string][]string) Owner {
	engines := providerOwned[name]
	if len(dialects) == 0 || len(engines) == 0 {
		return OwnerHost
	}
	for _, dialect := range dialects {
		if !slices.Contains(engines, dialect) {
			return OwnerHost
		}
	}
	return OwnerProvider
}
