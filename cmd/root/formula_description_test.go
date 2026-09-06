package root_test

// The Homebrew formula's description is what `brew info ptah` prints, and it is
// the first sentence a macOS user meets. It lived in `.goreleaser.yaml` as a
// hand-typed string and said "Schema management and migration tooling for Go
// projects" -- a Go-first framing the product moved away from, and one nothing
// could notice going stale (stokaro/ptah#2361).
//
// It reaches out of its own directory for `../../.goreleaser.yaml`, the way
// docs/readme_test.go reaches for `../README.md`: one named repository file
// that no Go package sits beside.

import (
	"os"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/root"
)

const goreleaserPath = "../../.goreleaser.yaml"

// formulaDescription reads the description under the `brews:` block.
var formulaDescription = regexp.MustCompile(`(?m)^    description: "([^"]*)"`)

// TestFormulaDescription_IsTheProductSentenceTheBinaryPrints holds the two
// together.
//
// Two places say what Ptah is: the root command's Short, which `ptah --help`
// prints, and the formula description, which `brew info` prints. A reader meets
// whichever their route gave them, and there is no reason for those to be
// different sentences -- so they are one sentence, and this is what makes them
// stay one.
func TestFormulaDescription_IsTheProductSentenceTheBinaryPrints(t *testing.T) {
	c := qt.New(t)

	body, err := os.ReadFile(goreleaserPath)
	c.Assert(err, qt.IsNil)
	matches := formulaDescription.FindAllStringSubmatch(string(body), -1)

	// Exactly one, or this test has stopped reading what it thinks it reads:
	// none means the block moved, and two mean a reader meets two answers.
	c.Assert(matches, qt.HasLen, 1)
	c.Assert(matches[0][1], qt.Equals, root.NewRootCommand().Short)
}

// TestRootShort_NamesBothHalvesOfTheProduct is why the sentence changed.
//
// The previous one listed the schema verbs and stopped there, so persistent
// inference state -- half of what Ptah manages -- was absent from the line most
// people read. A list of verbs is also a list that goes stale; this asserts the
// two halves are named rather than pinning the whole string, which would make
// every rewording a test edit.
func TestRootShort_NamesBothHalvesOfTheProduct(t *testing.T) {
	c := qt.New(t)

	short := strings.ToLower(root.NewRootCommand().Short)

	c.Assert(short, qt.Contains, "schema")
	c.Assert(short, qt.Contains, "inference state")
}
