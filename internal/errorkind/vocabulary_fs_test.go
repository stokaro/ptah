package errorkind_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/errorkind"
)

// documentedKind is a row of the kind table: a code span in the first cell.
var documentedKind = regexp.MustCompile("^\\| `([a-z_]+)` \\|")

// TestTheExitCodeReferenceListsEveryKind holds the published list to the
// vocabulary both ways, so a kind cannot ship without a meaning a reader can
// look up, and the page cannot describe a kind no command reports. The site
// page repeats the table, and its docs check holds the two copies together.
func TestTheExitCodeReferenceListsEveryKind(t *testing.T) {
	c := qt.New(t)
	page, err := os.ReadFile(filepath.Join("..", "..", "docs", "exit_codes.md"))
	c.Assert(err, qt.IsNil)
	_, section, found := strings.Cut(string(page), "\n## Why a Command Failed\n")
	c.Assert(found, qt.IsTrue)
	section, _, _ = strings.Cut(section, "\n## ")

	var documented []errorkind.Kind
	for line := range strings.SplitSeq(section, "\n") {
		for _, match := range documentedKind.FindAllStringSubmatch(line, 1) {
			documented = append(documented, errorkind.Kind(match[1]))
		}
	}

	c.Assert(documented, qt.DeepEquals, errorkind.Kinds())
}
