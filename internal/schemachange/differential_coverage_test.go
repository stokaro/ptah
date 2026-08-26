package schemachange_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// excludedIndexOrConstraintCategories are the categories no differential row
// exercises, each with the reason it does not.
//
// A category with no row and no entry here reddens: that is the point of the
// gate. An entry is a decision somebody wrote down, not a way to make the gate
// quiet -- and TestEveryDifferentialCoverageExclusionIsNecessary reddens on an
// entry that is no longer needed, so the list cannot grow stale in the quiet
// direction either.
//
// It was measured rather than guessed. Emptying it and running the gate names
// exactly the categories that need one; it named this and nothing else.
var excludedIndexOrConstraintCategories = map[string]string{
	"ConstraintBackedIndexRemovals": "not a change of its own: it marks which of " +
		"IndexesRemoved a constraint owns, so the planner drops the constraint " +
		"and not the index under it. A row would have to exercise IndexesRemoved " +
		"and this field together, and the shape that fills it is a UNIQUE " +
		"constraint's backing index, which the constraint rows already cover " +
		"from the other side.",
}

// TestEveryIndexOrConstraintCategoryHasADifferentialRow is the fourth item of
// stokaro/ptah#1663: "the differential test covers every fixture in
// migration/schemadiff touching indexes or constraints".
//
// It is derived in both directions rather than asserted. The categories come
// from [difftypes.SchemaDiff] by reflection, so a family added to the comparator
// is measured without anyone updating a list here; and what each fixture covers
// comes from RUNNING the comparator on it and reading which fields it filled,
// so a row that stops exercising its category stops counting.
//
// A hand-curated list of fixture names would have been the obvious shape and the
// wrong one: a walk over a hand-written list is the hand-written list it exists
// to guard.
func TestEveryIndexOrConstraintCategoryHasADifferentialRow(t *testing.T) {
	c := qt.New(t)

	categories := indexOrConstraintCategories()
	c.Assert(len(categories) > 0, qt.IsTrue,
		qt.Commentf("the walk found no index or constraint categories at all"))

	covered := categoriesCoveredByDifferentialRows()

	var uncovered []string
	for _, category := range categories {
		uncovered = appendIfUncovered(uncovered, category, covered)
	}

	c.Assert(uncovered, qt.HasLen, 0,
		qt.Commentf("these index or constraint categories have no differential row "+
			"and no written exclusion: %v", uncovered))
}

// TestTheDifferentialCoverageGateReadsRealRows is the control on the gate.
//
// A gate whose fixture walk stopped filling anything would compare an empty
// covered set against an exclusion list and report every category uncovered --
// loud, and therefore safe. The dangerous direction is the other one: a walk
// that reported everything covered would pass forever. This asserts the walk
// found the two categories the slice most obviously exercises.
func TestTheDifferentialCoverageGateReadsRealRows(t *testing.T) {
	c := qt.New(t)

	covered := categoriesCoveredByDifferentialRows()

	c.Assert(covered["IndexesAdded"], qt.IsTrue)
	c.Assert(covered["ConstraintsAddedWithTables"], qt.IsTrue)
}

// TestEveryDifferentialCoverageExclusionIsExplained holds the exclusions to the
// same standard as the rows: a reason, in words, or it is not an exclusion.
func TestEveryDifferentialCoverageExclusionIsExplained(t *testing.T) {
	c := qt.New(t)

	for category, reason := range excludedIndexOrConstraintCategories {
		c.Assert(len(reason) > 40, qt.IsTrue,
			qt.Commentf("%s is excluded without a reason worth reading", category))
	}
}

// TestEveryDifferentialCoverageExclusionIsNecessary is what keeps the exclusion
// list from outliving its reasons.
//
// An exclusion for a category some row now covers is not harmless: it is a hole
// the gate stops watching, and nothing would ever say so. This asserts each
// excluded category is genuinely uncovered, so an exclusion that became
// unnecessary reddens and gets deleted.
func TestEveryDifferentialCoverageExclusionIsNecessary(t *testing.T) {
	c := qt.New(t)

	covered := categoriesCoveredByDifferentialRows()

	for category := range excludedIndexOrConstraintCategories {
		c.Assert(covered[category], qt.IsFalse,
			qt.Commentf("%s is excluded but a differential row covers it; delete the exclusion", category))
	}
}

// indexOrConstraintCategories lists the SchemaDiff fields that carry an index or
// constraint change, read from the type rather than from a list.
func indexOrConstraintCategories() []string {
	structType := reflect.TypeFor[difftypes.SchemaDiff]()
	var categories []string
	for field := range structType.Fields() {
		if !field.IsExported() || field.Type.Kind() != reflect.Slice {
			continue
		}
		if !namesAnIndexOrConstraint(field.Name) {
			continue
		}
		categories = append(categories, field.Name)
	}
	slices.Sort(categories)
	return categories
}

// namesAnIndexOrConstraint reports whether a category is one of the two families
// this issue is about.
func namesAnIndexOrConstraint(field string) bool {
	return strings.Contains(field, "Index") || strings.Contains(field, "Constraint")
}

// categoriesCoveredByDifferentialRows runs the comparator over every fixture the
// differential slices carry and records which categories each one fills.
func categoriesCoveredByDifferentialRows() map[string]bool {
	covered := make(map[string]bool)
	for _, fixture := range statementDifferentialFixtures() {
		diff := schemadiff.CompareWithDialect(fixture.description, fixture.catalog, "postgres")
		recordFilledCategories(covered, diff)
	}
	return covered
}

// recordFilledCategories marks every index or constraint category the diff
// actually filled.
func recordFilledCategories(covered map[string]bool, diff *difftypes.SchemaDiff) {
	value := reflect.ValueOf(diff).Elem()
	fields := value.Type()
	for i := range fields.NumField() {
		field := fields.Field(i)
		if !field.IsExported() || field.Type.Kind() != reflect.Slice {
			continue
		}
		if namesAnIndexOrConstraint(field.Name) && value.Field(i).Len() > 0 {
			covered[field.Name] = true
		}
	}
}

// appendIfUncovered records a category that neither a row nor an exclusion
// accounts for.
func appendIfUncovered(uncovered []string, category string, covered map[string]bool) []string {
	if covered[category] {
		return uncovered
	}
	if _, excluded := excludedIndexOrConstraintCategories[category]; excluded {
		return uncovered
	}
	return append(uncovered, category)
}
