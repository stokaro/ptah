package atlas_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `migrate down` is the one verb that reads a NATIVE Atlas migration directory,
// executes SQL from it, and sits outside the stokaro/ptah#974 integrity gate.
//
// It is not a parity divergence. Measured 2026-08-03, the pinned community
// binary v1.3.0 answers `'atlas migrate down' is not supported by the community
// version`, so there is no behavior to match and no oracle to measure against.
// That is exactly why it needs a test: an ungated mutating verb with no oracle
// is invisible to scripts/probe-atlas-integrity-verbs.sh, which compares the two
// tools, and it was missing from the enumeration in the compat migrate-commands
// page for that reason.
//
// The two assertions below move together on purpose. Gating `migrate down`
// later is a defensible decision; doing it while the page still tells readers
// that `down` reports normally is not, and the pair is what stops the
// enumeration and the behavior from drifting apart again.

const atlasSumSectionHeading = "### Which verbs enforce `atlas.sum`"

// atlasSumSectionBody returns the enumeration section of the compat
// migrate-commands page: everything from its heading to the next second-level
// heading.
func atlasSumSectionBody(c *qt.C) string {
	c.Helper()
	page, err := os.ReadFile(filepath.Join(
		"..", "..", "docs", "site", "src", "content", "docs", "atlas", "migrate-commands.md"))
	c.Assert(err, qt.IsNil)
	_, afterHeading, found := strings.Cut(string(page), atlasSumSectionHeading)
	c.Assert(found, qt.IsTrue)
	body, _, _ := strings.Cut(afterHeading, "\n## ")
	return body
}

// TestCompatMigrateDown_NamedInAtlasSumEnumeration pins that the enumeration
// accounts for `down` at all.
//
// The section lists the verbs that enforce `atlas.sum` and the verbs that do
// not, verb by verb. `down` reads a directory and executes rollback SQL from it,
// so a reader auditing which reads are verified needs it named; leaving it out
// reads as "not applicable" rather than "ungated".
func TestCompatMigrateDown_NamedInAtlasSumEnumeration(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasSumSectionBody(c), qt.Contains, "`down`")
}

// TestCompatMigrateDown_UngatedOnDirectoryStatusRefuses is the measurement the
// enumeration line describes, asserted on one directory so the two verbs cannot
// be compared across different fixtures.
//
// This is a BOUNDARY row, not a discriminator: it passes with and without #974,
// because #974 did not touch `migrate down`. It fails the day `down` is gated,
// which is the point — that day the page has to change with it.
func TestCompatMigrateDown_UngatedOnDirectoryStatusRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m")
	writeStatusIntegrityEdited(c, dir)

	statusOut, _, statusErr := runCompat(
		"migrate", "status",
		"--url", "sqlite://"+filepath.Join(tempDir, "status.db"),
		"--dir", "file://"+dir,
	)
	downOut, downErrOut, downErr := runCompat(
		"migrate", "down",
		"--url", "sqlite://"+filepath.Join(tempDir, "down.db"),
		"--dir", "file://"+dir,
	)

	// status refuses the drifted directory outright.
	c.Assert(statusErr, qt.IsNotNil)
	c.Assert(statusErr.Error(), qt.Equals, "checksum mismatch")
	c.Assert(statusOut, qt.Equals, atlasChecksumGuidanceWith("L2: "+statusIntegrityMigration+" was edited"))
	// down reads the same directory and reports normally. The positive
	// assertion is load-bearing: without it, "no checksum text" would also hold
	// for output that was never captured.
	c.Assert(downErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", downOut, downErrOut))
	c.Assert(downOut, qt.Contains, "=== MIGRATE DOWN ===")
	c.Assert(downOut, qt.Not(qt.Contains), "checksum")
}
