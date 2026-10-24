package safety_test

import (
	"maps"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/risk"
	"ptah.run/migration/safety"
)

// kindlessAccessChange is an owner change that names no valid kind and still
// claims an access effect, which no snapshot accepts.
type kindlessAccessChange struct{}

func (*kindlessAccessChange) Kind() schemaext.Kind               { return "" }
func (*kindlessAccessChange) CloneChange() schemaext.ChangeValue { return &kindlessAccessChange{} }
func (*kindlessAccessChange) AccessEffect() schemaext.AccessEffect {
	return schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "admits more rows"}
}

// TestFindingCategories_ListsWhatTheClassifierReports holds the vocabulary to
// the classifier in both directions at once. The rows are a diff with one
// change in every place the walk reaches, the values a zero entry cannot
// take, and a feature owner's change of every effect and every access
// assessment. Each category the rows report must be listed with the
// severities and the forms they report it in, and nothing else may be listed:
// a severity or a form no row reports is one the classifier cannot report.
func TestFindingCategories_ListsWhatTheClassifierReports(t *testing.T) {
	c := qt.New(t)

	reported := reportedVocabulary(classifierRows())

	c.Assert(safety.FindingCategories(), qt.DeepEquals, reported)
}

// TestFindingCategories_IsSortedAndRanked pins the order a reader of the
// generated document can rely on, and the control that the list is not empty.
func TestFindingCategories_IsSortedAndRanked(t *testing.T) {
	c := qt.New(t)

	categories := safety.FindingCategories()

	c.Assert(len(categories) > 50, qt.IsTrue, qt.Commentf("listed %d categories", len(categories)))
	c.Assert(slices.IsSortedFunc(categories, func(a, b safety.FindingCategory) int {
		return strings.Compare(a.Category, b.Category)
	}), qt.IsTrue)
	c.Assert(categories[slices.IndexFunc(categories, func(category safety.FindingCategory) bool {
		return category.Category == "materialized_views_modified"
	})], qt.DeepEquals, safety.FindingCategory{
		Category: "materialized_views_modified", Severities: []safety.Severity{safety.Warning, safety.Destructive}, Bare: true,
	})
}

// classifierRows is every diff the vocabulary test classifies.
func classifierRows() []*difftypes.SchemaDiff {
	rows := []*difftypes.SchemaDiff{
		{RLSForceChanged: difftypes.RLSForceChanges{schemamodel.RLSEnabledTable{Forced: true}}},
		{TablesModified: []difftypes.TableDiff{{ColumnsModified: []difftypes.ColumnDiff{
			{Changes: map[string]string{"type": "vector(3) -> vector(4)"}},
		}}}},
		{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{ViewName: "daily", FeatureChanges: []schemaext.ChangeRecord{
			ownerChange(&classifiedChange{effect: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "rows are kept"}}),
		}}}},
		{FeatureChanges: []schemaext.ChangeRecord{{Value: &kindlessAccessChange{}}}},
	}
	for _, impact := range []schemaext.Impact{schemaext.Additive, schemaext.Behavioral, schemaext.Destructive} {
		rows = append(rows, &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
			ownerChange(&classifiedChange{effect: schemaext.Effect{Impact: impact, Reason: "the owner's reading"}}),
		}})
	}
	for _, access := range []schemaext.Access{
		schemaext.AccessUnchanged, schemaext.AccessNarrows, schemaext.AccessWidens, schemaext.AccessUnknown,
	} {
		rows = append(rows, &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
			ownerChange(&accessChange{effect: addsObject, access: schemaext.AccessEffect{Access: access, Reason: "the owner's reading"}}),
		}})
	}
	for _, slot := range changeSlots() {
		rows = append(rows, slot.diff)
	}
	return rows
}

// ownerChange is a change on an object a feature owner holds.
func ownerChange(value schemaext.ChangeValue) schemaext.ChangeRecord {
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts("example.org/subject", "app", "name")
	return schemaext.ChangeRecord{Subject: ref, Value: value}
}

// reportedVocabulary is the vocabulary the rows' findings spell out, in the
// shape FindingCategories returns.
func reportedVocabulary(rows []*difftypes.SchemaDiff) []safety.FindingCategory {
	byName := make(map[string]*safety.FindingCategory)
	for _, row := range rows {
		for _, finding := range safety.ClassifySchemaDiff(row) {
			name, _, qualified := strings.Cut(finding.Category, ":")
			entry := byName[name]
			if entry == nil {
				entry = &safety.FindingCategory{Category: name}
				byName[name] = entry
			}
			entry.Severities = append(entry.Severities, finding.Severity)
			entry.Bare = entry.Bare || !qualified
			entry.Qualified = entry.Qualified || qualified
		}
	}
	vocabulary := make([]safety.FindingCategory, 0, len(byName))
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		entry := *byName[name]
		slices.SortFunc(entry.Severities, func(a, b safety.Severity) int { return risk.Rank(a) - risk.Rank(b) })
		entry.Severities = slices.Compact(entry.Severities)
		vocabulary = append(vocabulary, entry)
	}
	return vocabulary
}
