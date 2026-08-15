package atlas_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `migrate down` reads a NATIVE Atlas migration directory and executes rollback
// SQL from it, and until this change it sat outside the stokaro/ptah#974
// integrity gate — the one verb in that position.
//
// Its absence was never a parity divergence. Measured 2026-08-03, the pinned
// community binary v1.3.0 answers `'atlas migrate down' is not supported by the
// community version`, so there is no behavior to match and no oracle to measure
// against. That is exactly why it needed a test: an ungated mutating verb with
// no oracle is invisible to scripts/probe-atlas-integrity-verbs.sh, which
// compares the two tools.
//
// Having no oracle is also why gating it is permitted rather than blocked.
// Compatibility policy (a) forbids exiting 0 where the community binary exits
// 1, and refusing cannot violate that; policy (b) says matching is the floor
// and not the ceiling, which is what a verb with no oracle at all leaves room
// for.
//
// The two assertions below move together on purpose. The behavior and the
// enumeration in the compat migrate-commands page change in the same commit,
// which is what stops them from drifting apart again.

const atlasSumSectionHeading = "### Which verbs enforce `atlas.sum`"

// atlasSumSectionBody returns the enumeration section of the compat
// migrate-commands page: everything from its heading to the next second-level
// heading.
func atlasSumSectionBody(tb testing.TB) string {
	c := qt.New(tb)
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

	c.Assert(atlasSumSectionBody(c.TB), qt.Contains, "`down`")
}

// TestCompatMigrateDown_RefusesWhereStatusRefuses is the measurement the
// enumeration line describes, asserted on ONE directory so the two verbs cannot
// be compared across different fixtures.
//
// It used to assert the opposite. Until this change `down` read the same
// directory `status` refuses and reported normally at exit 0, and the row was
// written as a boundary marker whose comment said it would fail the day `down`
// was gated. That day is this change: `down` executes rollback SQL from the
// directory, so it is a member of the executing class and gates with the rest
// of it.
//
// The two verbs still refuse through different machinery, and the assertions
// are deliberately not identical because of it. `status` runs the compat gate
// and reproduces the community binary's stdout guidance block byte for byte.
// `down` has no community behavior to reproduce — the pinned binary v1.3.0
// answers `'atlas migrate down' is not supported by the community version` —
// so the default forward inherits the NATIVE refusal from
// `ptah migrations down`, which names ptah's own drift report instead. Pinning
// `status`'s exact bytes onto `down` would invent a parity claim that has no
// oracle behind it.
func TestCompatMigrateDown_RefusesWhereStatusRefuses(t *testing.T) {
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
	// down now refuses the same directory. The negative assertion on the banner
	// is load-bearing: it is what separates "refused before executing" from
	// "started the rollback and failed partway through".
	c.Assert(downErr, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", downOut, downErrOut))
	c.Assert(downErr.Error(), qt.Contains, "migration sum verification failed")
	c.Assert(downErr.Error(), qt.Contains, statusIntegrityMigration)
	c.Assert(downOut, qt.Not(qt.Contains), "=== MIGRATE DOWN ===")
}
