package drift_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/drift"
	"ptah.run/internal/datamigrate"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/safety"
)

// TestFindingVocabulary_TheDocumentIsCurrent holds the committed document to
// the categories the drift report can carry. A reader that does not build
// Ptah, Ptah Operator among them, reads the document at a commit, so a
// category added without regenerating it would be refused there as unknown.
// `scripts/check-docsync.sh --write` regenerates it.
func TestFindingVocabulary_TheDocumentIsCurrent(t *testing.T) {
	c := qt.New(t)
	var rendered bytes.Buffer

	err := drift.WriteFindingVocabulary(&rendered)

	c.Assert(err, qt.IsNil)
	committed, readErr := os.ReadFile(filepath.Join("..", "..", "..", drift.VocabularyPath))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(committed), qt.Equals, rendered.String(),
		qt.Commentf("%s is stale; run scripts/check-docsync.sh --write", drift.VocabularyPath))
}

// TestFindingCategories_JoinsEveryProducer pins that the report's vocabulary
// is the three producers the report joins, each category once.
func TestFindingCategories_JoinsEveryProducer(t *testing.T) {
	c := qt.New(t)

	categories, err := drift.FindingCategories()

	c.Assert(err, qt.IsNil)
	want := slices.Concat(safety.FindingCategories(), datamigrate.FindingCategories(), undecidednote.FindingCategories())
	c.Assert(categories, qt.HasLen, len(want))
	c.Assert(categories, qt.Any(qt.DeepEquals), safety.FindingCategory{
		Category: "data_rows_updated", Severities: []safety.Severity{safety.Destructive}, Bare: true,
	})
	c.Assert(categories, qt.Any(qt.DeepEquals), safety.FindingCategory{
		Category: "undecided", Severities: []safety.Severity{safety.Warning}, Bare: true,
	})
	c.Assert(categories, qt.Any(qt.DeepEquals), safety.FindingCategory{
		Category: "feature_access_widened", Severities: []safety.Severity{safety.Destructive}, Qualified: true,
	})
}
