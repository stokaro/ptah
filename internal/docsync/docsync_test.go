package docsync_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/docsync"
)

// document is a page with one generated block and hand-written text on both
// sides of it, which is the shape every marker target has.
const document = `# A page

Prose above.

<!-- BEGIN GENERATED THING -->
| a | b |
<!-- END GENERATED THING -->

Prose below.
`

func markerTarget() docsync.Target {
	return docsync.Target{
		Name:  "the thing",
		Path:  "docs/thing.md",
		Begin: "<!-- BEGIN GENERATED THING -->",
		End:   "<!-- END GENERATED THING -->",
		Render: func(w io.Writer) error {
			_, err := io.WriteString(w, "| a | b |\n")
			return err
		},
	}
}

func TestExtract_ReadsTheBlockBetweenTheMarkers(t *testing.T) {
	c := qt.New(t)

	block, err := docsync.Extract(document, markerTarget())

	c.Assert(err, qt.IsNil)
	c.Assert(block, qt.Equals, "| a | b |\n")
}

// TestExtract_RefusesADocumentWithNoMarkers is the refusal that keeps this from
// comparing nothing to nothing.
//
// A file whose markers were renamed or lost in a merge yields an empty block on
// both sides, the comparison finds them identical, and the gate reports success
// at exactly the moment it stopped working.
func TestExtract_RefusesADocumentWithNoMarkers(t *testing.T) {
	c := qt.New(t)

	_, err := docsync.Extract("# A page with no markers\n", markerTarget())

	c.Assert(err, qt.ErrorMatches, `the thing: docs/thing.md carries no <!-- BEGIN GENERATED THING --> line`)
}

// TestExtract_RefusesAMarkerThatIsNotAloneOnItsLine is the shape a substring
// check accepts and then splits nothing on.
//
// It was not hypothetical: a fixture that inserted text mid-marker passed the
// existence check, the split found no marker line, and Replace wrote a SECOND
// copy of the block at the end of the file rather than refusing.
func TestExtract_RefusesAMarkerThatIsNotAloneOnItsLine(t *testing.T) {
	c := qt.New(t)

	broken := strings.Replace(document,
		"<!-- BEGIN GENERATED THING -->",
		"<!-- BEGIN GENERATED THING -->| appended |", 1)

	_, err := docsync.Extract(broken, markerTarget())
	c.Assert(err, qt.ErrorMatches, `the thing: docs/thing.md carries no <!-- BEGIN GENERATED THING --> line`)

	_, err = docsync.Replace(broken, "| c | d |\n", markerTarget())
	c.Assert(err, qt.ErrorMatches, `the thing: docs/thing.md carries no <!-- BEGIN GENERATED THING --> line`)
}

// TestReplace_IsIdempotent keeps a rewrite from drifting the document by a
// newline each time, which a marker split is easy to get wrong in.
func TestReplace_IsIdempotent(t *testing.T) {
	c := qt.New(t)

	once, err := docsync.Replace(document, "| c | d |\n", markerTarget())
	c.Assert(err, qt.IsNil)
	twice, err := docsync.Replace(once, "| c | d |\n", markerTarget())
	c.Assert(err, qt.IsNil)

	c.Assert(twice, qt.Equals, once)
	// And rewriting what is already there returns the document unchanged.
	same, err := docsync.Replace(document, "| a | b |\n", markerTarget())
	c.Assert(err, qt.IsNil)
	c.Assert(same, qt.Equals, document)
}

// TestGenerate_RefusesAGeneratorThatPrintsNothing is the other half of that
// property, from the generator's side: an empty rendering would rewrite the
// block to nothing on a --write, and compare equal to a document somebody had
// already emptied.
func TestGenerate_RefusesAGeneratorThatPrintsNothing(t *testing.T) {
	c := qt.New(t)

	target := markerTarget()
	target.Render = func(io.Writer) error { return nil }

	_, err := docsync.Generate(target)

	c.Assert(err, qt.ErrorMatches, `the thing: the generator produced nothing; refusing to compare docs/thing.md against an empty block`)
}

// TestGenerate_ReportsWhatTheGeneratorSaid keeps a generator's own diagnostic
// from being replaced by "produced nothing", which says only that something is
// wrong.
func TestGenerate_ReportsWhatTheGeneratorSaid(t *testing.T) {
	c := qt.New(t)

	target := markerTarget()
	target.Render = func(io.Writer) error { return errors.New("the command tree has no runnable verbs") }

	_, err := docsync.Generate(target)

	c.Assert(err, qt.ErrorMatches, `the thing: the command tree has no runnable verbs`)
}

func TestReplace_RewritesOnlyTheBlock(t *testing.T) {
	c := qt.New(t)

	updated, err := docsync.Replace(document, "| c | d |\n", markerTarget())

	c.Assert(err, qt.IsNil)
	c.Assert(updated, qt.Contains, "| c | d |")
	c.Assert(updated, qt.Not(qt.Contains), "| a | b |")
	// The hand-written halves survive, which is the whole reason a block has
	// markers rather than owning the file.
	c.Assert(updated, qt.Contains, "Prose above.")
	c.Assert(updated, qt.Contains, "Prose below.")
}

// TestWholeFile_IsTheDocument covers the second target shape: a page with no
// hand-written half, where the generated content IS the file.
func TestWholeFile_IsTheDocument(t *testing.T) {
	c := qt.New(t)

	target := docsync.Target{Name: "the page", Path: "docs/page.md"}
	c.Assert(target.WholeFile(), qt.IsTrue)

	block, err := docsync.Extract(document, target)
	c.Assert(err, qt.IsNil)
	c.Assert(block, qt.Equals, document)

	updated, err := docsync.Replace(document, "everything\n", target)
	c.Assert(err, qt.IsNil)
	c.Assert(updated, qt.Equals, "everything\n")
}

// TestUnifiedDiff_NamesBothSides is what a stale-content diagnostic prints.
func TestUnifiedDiff_NamesBothSides(t *testing.T) {
	c := qt.New(t)

	diff := docsync.UnifiedDiff("kept\nwanted\n", "kept\nfound\n")

	c.Assert(diff, qt.Contains, "-found")
	c.Assert(diff, qt.Contains, "+wanted")
	c.Assert(diff, qt.Not(qt.Contains), "kept")
}
