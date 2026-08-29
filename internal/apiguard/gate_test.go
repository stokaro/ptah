package apiguard_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// widgetSource is the fixture module's one documented package. Its exported
// field is what the gate has to notice moving.
const widgetSource = `// Package pkg is the documented package of the gate fixture module.
package pkg

// Widget is an exported type the fixture ledger lists.
type Widget struct {
	// Name identifies the widget.
	Name string
}
`

// widgetSourceWithAddedField is widgetSource with one more exported field. An
// ADDED field is the direction worth fixturing: a removed one also breaks
// compilation somewhere, while an added one is a widened public surface that
// builds, tests and vets cleanly and is visible only in the snapshot.
const widgetSourceWithAddedField = `// Package pkg is the documented package of the gate fixture module.
package pkg

// Widget is an exported type the fixture ledger lists.
type Widget struct {
	// Name identifies the widget.
	Name string

	// Weight is the field this fixture adds.
	Weight int
}
`

// TestGateReportsAnAddedExportedField proves the gate as a whole fails on a
// snapshot that no longer describes the tree, and names what changed.
//
// internal/apiguard's other tests prove the FRAGMENT differs; that is a
// different claim. A fragment that changes and a gate that compares it against
// the recorded one and exits non-zero are two links, and the gate is the one a
// pull request runs. Both halves of the run are asserted here so a failure
// cannot be the fixture module failing to build.
func TestGateReportsAnAddedExportedField(t *testing.T) {
	c := qt.New(t)

	dir := writeGateFixture(c, widgetSource)

	stdout, stderr, err := runGate(c, dir, "--update")
	c.Assert(err, qt.IsNil, qt.Commentf("recording the snapshot failed:\n%s%s", stdout, stderr))

	stdout, stderr, err = runGate(c, dir)
	c.Assert(err, qt.IsNil, qt.Commentf("the recorded snapshot did not match the tree it was taken from:\n%s%s", stdout, stderr))

	writeFile(c, filepath.Join(dir, "pkg", "widget.go"), widgetSourceWithAddedField)

	stdout, _, err = runGate(c, dir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Contains, "Weight int")
}

// TestGateAcceptsTheTreeItsSnapshotWasTakenFrom is the control for the test
// above. Without it, a gate that failed on everything -- a broken fixture
// module, a missing toolchain -- would read as the added field being caught.
//
// The second run also proves --update is what reconciles the two, so the
// failure above is about the recorded content rather than about anything the
// run does to the working tree.
func TestGateAcceptsTheTreeItsSnapshotWasTakenFrom(t *testing.T) {
	c := qt.New(t)

	dir := writeGateFixture(c, widgetSource)

	stdout, stderr, err := runGate(c, dir, "--update")
	c.Assert(err, qt.IsNil, qt.Commentf("recording the snapshot failed:\n%s%s", stdout, stderr))

	writeFile(c, filepath.Join(dir, "pkg", "widget.go"), widgetSourceWithAddedField)

	stdout, stderr, err = runGate(c, dir, "--update")
	c.Assert(err, qt.IsNil, qt.Commentf("re-recording the snapshot failed:\n%s%s", stdout, stderr))

	stdout, stderr, err = runGate(c, dir)
	c.Assert(err, qt.IsNil, qt.Commentf("the re-recorded snapshot did not match the tree it was taken from:\n%s%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
}

// TestGateRefusesAVacuousPassWhenTheLedgerListsNothing proves the floor that
// stops the gate comparing nothing against nothing.
//
// A ledger the scrape reads as empty produces an empty snapshot, which matches
// an empty recorded snapshot, which exits 0. That is the failure mode this whole
// family of gates exists to avoid: a check that measured nothing is
// indistinguishable from a check that passed. The refusal lives in the scrape --
// `featureinventory --list-ledger` -- and the assertion names that wording so it
// says which layer answers; a `[[ ! -s ]]` in the script would be a second
// answer no fixture could reach.
//
// The fixture's ledger is not blank -- it names the package in prose and in a
// heading -- because a blank file would also be caught by a weaker rule that
// merely required the file to have content.
func TestGateRefusesAVacuousPassWhenTheLedgerListsNothing(t *testing.T) {
	c := qt.New(t)

	dir := writeGateFixture(c, widgetSource)
	writeFile(c, filepath.Join(dir, "docs", "public_api.md"),
		"# Ledger fixture\n"+
			"\n"+
			"A paragraph mentioning `apiguardfixture/pkg` is a mention, not a listing.\n"+
			"\n"+
			"### `apiguardfixture/pkg`\n"+
			"\n"+
			"A heading is not a listing either.\n")

	_, stderr, err := runGate(c, dir)
	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "refusing to report a vacuous ledger")
}

// writeGateFixture builds a throwaway module holding one documented package and
// a ledger that lists it, and returns its directory. The gate reads the ledger
// and the snapshot relative to the working directory, so a fixture module is
// all it takes to run the real script end to end against a tree whose public
// surface the test controls.
func writeGateFixture(c *qt.C, source string) string {
	c.Helper()

	dir := c.TempDir()
	writeFile(c, filepath.Join(dir, "go.mod"), "module apiguardfixture\n\ngo 1.21\n")
	writeFile(c, filepath.Join(dir, "pkg", "widget.go"), source)
	writeFile(c, filepath.Join(dir, "docs", "public_api.md"),
		"# Ledger fixture\n\n- `apiguardfixture/pkg`\n")

	return dir
}

// writeFile writes content at path, creating the parent directories.
func writeFile(c *qt.C, path, content string) {
	c.Helper()

	err := os.MkdirAll(filepath.Dir(path), 0o700)
	c.Assert(err, qt.IsNil)
	err = os.WriteFile(path, []byte(content), 0o600)
	c.Assert(err, qt.IsNil)
}

// runGate runs the real gate script with the fixture module as its working
// directory and returns both streams alongside the exit status. The streams are
// returned rather than asserted so each test says what it expects to read on
// which one.
func runGate(c *qt.C, dir string, args ...string) (stdout, stderr string, err error) {
	c.Helper()

	script := filepath.Join(moduleRoot(c), "scripts", "check-public-api-snapshot.sh")
	// #nosec G204 -- the command is `bash`, the script path is derived from this
	// package's own location, and the arguments are literals written above.
	cmd := exec.Command("bash", append([]string{script}, args...)...)
	cmd.Dir = dir

	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err = cmd.Run()

	return out.String(), errOut.String(), err
}
