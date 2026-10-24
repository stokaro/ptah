package drift

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/internal/datamigrate"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/safety"
)

// VocabularyPath is where the repository keeps the document
// [WriteFindingVocabulary] writes, relative to its root. A reader that does
// not build Ptah reads the categories of a commit from this file at that
// commit.
const VocabularyPath = "docs/drift-findings.json"

// vocabularyComment is the document's own note on where it comes from.
const vocabularyComment = "Every finding category `ptah schema drift` can report, with the severities a finding of it can carry " +
	"and whether it is written alone (bare) or followed by a colon and a feature owner's change kind (qualified). " +
	"Generated from internal/cli/drift.FindingCategories by `scripts/check-docsync.sh --write`; do not edit."

// vocabulary is the document [WriteFindingVocabulary] writes.
type vocabulary struct {
	Comment    string                   `json:"comment"`
	Categories []safety.FindingCategory `json:"categories"`
}

// FindingCategories returns every category a drift report's findings can
// carry, sorted by name. The report joins three producers, and so does this:
// the schema comparison's categories ([safety.FindingCategories]), the
// declared rows' ([datamigrate.FindingCategories]) and the undecided objects'
// ([undecidednote.FindingCategories]). A category two producers claim is an
// error, since a reader could not tell which of them reported it.
func FindingCategories() ([]safety.FindingCategory, error) {
	categories := slices.Concat(
		safety.FindingCategories(),
		datamigrate.FindingCategories(),
		undecidednote.FindingCategories(),
	)
	slices.SortStableFunc(categories, func(a, b safety.FindingCategory) int {
		return strings.Compare(a.Category, b.Category)
	})
	for i := 1; i < len(categories); i++ {
		if categories[i].Category == categories[i-1].Category {
			return nil, fmt.Errorf("drift finding category %q is reported by two producers", categories[i].Category)
		}
	}
	return categories, nil
}

// WriteFindingVocabulary writes the document kept at [VocabularyPath]: a
// comment, and [FindingCategories] as a JSON array under "categories".
func WriteFindingVocabulary(w io.Writer) error {
	categories, err := FindingCategories()
	if err != nil {
		return err
	}
	document, err := json.MarshalIndent(vocabulary{Comment: vocabularyComment, Categories: categories}, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(append(document, '\n'))
	return err
}
