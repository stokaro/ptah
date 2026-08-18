package oci_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/oci"
)

// commandReference is the page that lists the native command surface.
var commandReference = filepath.Join(
	"..", "..", "docs", "site", "src", "content", "docs", "reference", "native-commands.md",
)

// ociVerbRow matches a table row naming an `oci` verb, so the census is taken
// from the rendered table a reader actually consults rather than from prose
// that happens to mention a command.
var ociVerbRow = regexp.MustCompile("(?m)^\\|\\s*`ptah oci ([a-z-]+)`\\s*\\|")

// TestOCIVerbs_MatchTheCommandReference keeps the documented surface and the
// built one from drifting.
//
// The namespace grew from one verb to nine across several changes, and every
// row was added to the reference by hand. A verb registered and never
// documented is invisible to everyone who reads the docs before the source; a
// row left behind for a verb that was removed sends a reader to a command that
// answers "unknown". Neither shows up in any other test, because both halves
// keep working on their own.
func TestOCIVerbs_MatchTheCommandReference(t *testing.T) {
	c := qt.New(t)

	var registered []string
	for _, command := range oci.NewCommand().Commands() {
		if command.Name() == "help" || command.Hidden {
			continue
		}
		registered = append(registered, command.Name())
	}
	slices.Sort(registered)
	c.Assert(len(registered) > 0, qt.IsTrue,
		qt.Commentf("the command tree reported no verbs, so this gate would pass vacuously"))

	contents, err := os.ReadFile(commandReference)
	c.Assert(err, qt.IsNil)
	var documented []string
	for _, match := range ociVerbRow.FindAllStringSubmatch(string(contents), -1) {
		documented = append(documented, match[1])
	}
	slices.Sort(documented)
	c.Assert(len(documented) > 0, qt.IsTrue,
		qt.Commentf("%s lists no oci verb, so the comparison below would be vacuous", commandReference))

	c.Assert(documented, qt.DeepEquals, registered)
}
