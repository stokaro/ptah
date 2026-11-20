package atlascodes

import (
	"fmt"
	"slices"
)

// Validate is the gate over the correspondence between Ptah rules and Atlas
// checks, in both directions, against the rule names the linters register
// (registered) and the reviewed snapshot of Atlas's own list.
//
// Each rule that reports an Atlas check is named by that check's row, and each
// row that claims a rule names a rule a linter registers, so neither side can
// claim what the other does not. Every row's code is in the snapshot and every
// snapshot code has a row, because consistency between the table and the
// generated page cannot see a check absent from both (stokaro/ptah#2972).
// A covered or partial row names a rule; an absent or waived one names none.
//
// It returns one problem per disagreement, sorted by the table's own order,
// and nil when the two sides agree.
func Validate(registered []string) []string {
	problems := referenceProblems()
	for _, identity := range identities {
		if !slices.Contains(registered, identity.Name) {
			problems = append(problems, fmt.Sprintf("rule %s reports Atlas %s but no linter registers it", identity.Name, identity.Code))
			continue
		}
		check, ok := checkFor(identity.Code)
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("rule %s reports Atlas %s, which the Atlas analyzer list does not document", identity.Name, identity.Code))
		case !slices.Contains(check.PtahRules, identity.Name):
			// Both directions of the same fact, so a row can never claim a
			// rule the rule does not claim back.
			problems = append(problems, fmt.Sprintf("rule %s reports Atlas %s, but that check's row does not name %s", identity.Name, identity.Code, identity.Name))
		}
	}
	for _, check := range checks {
		for _, name := range check.PtahRules {
			if !slices.Contains(registered, name) {
				problems = append(problems, fmt.Sprintf("Atlas check %s claims coverage by Ptah rule %s, which no linter registers", check.Code, name))
			}
		}
		switch check.Status {
		case StatusCovered, StatusPartial:
			if len(check.PtahRules) == 0 {
				problems = append(problems, fmt.Sprintf("Atlas check %s is marked %s and names no Ptah rule", check.Code, check.Status))
			}
		case StatusAbsent, StatusWaived:
			if len(check.PtahRules) > 0 {
				problems = append(problems, fmt.Sprintf("Atlas check %s is marked %s yet names Ptah rules", check.Code, check.Status))
			}
		default:
			problems = append(problems, fmt.Sprintf("Atlas check %s has unknown status %q", check.Code, check.Status))
		}
	}
	return problems
}

// referenceProblems compares the check list with the reviewed snapshot, code
// for code. The check that was missing: a count cannot see a check absent from
// both the list and the page it was built from.
func referenceProblems() []string {
	drift := CompareReference()
	if drift.Empty() {
		return nil
	}
	var problems []string
	for _, code := range drift.MissingFromCatalog {
		problems = append(problems, fmt.Sprintf(
			"Atlas check %s is in the reviewed reference and has no catalog entry; assess it and add a row, or refresh the reference deliberately", code))
	}
	for _, code := range drift.MissingFromReference {
		problems = append(problems, fmt.Sprintf(
			"Atlas check %s has a catalog entry and is not in the reviewed reference; an upstream removal is reviewed by refreshing the reference, not by leaving the row", code))
	}
	for _, code := range drift.ProDisagreements {
		problems = append(problems, fmt.Sprintf(
			"Atlas check %s carries a different Atlas Pro marking in the catalog than in the reviewed reference", code))
	}
	return problems
}
