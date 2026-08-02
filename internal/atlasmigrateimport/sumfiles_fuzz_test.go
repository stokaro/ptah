package atlasmigrateimport_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/migratesum"
)

// assertCaptureSelectsTheSame checks that selecting the covered set over the
// SNAPSHOT the apply gate verifies gives the same names, in the same order, as
// selecting it over the live directory.
//
// The differential fuzz proves SumFileNames matches the oracle over os.DirFS.
// The apply gate does not read os.DirFS — it reads a capture, and the capture
// filters. If the two ever disagree the gate recomputes a set the oracle never
// wrote, so this seam needs the same random population rather than the handful
// of fixtures a unit test can name. Order is asserted because it feeds the
// Atlas hash.
func assertCaptureSelectsTheSame(c *qt.C, dir string, format atlasmigrateimport.Format, names []string, layout []string) {
	c.Helper()
	captured, err := atlasmigrate.CaptureApplySource(os.DirFS(dir), format)
	c.Assert(err, qt.IsNil, qt.Commentf("PTAH_ATLAS_FUZZ_LAYOUT=%s", strings.Join(layout, ",")))
	fromCapture, err := atlasmigrateimport.SumFileNames(captured, format)
	c.Assert(err, qt.IsNil, qt.Commentf("PTAH_ATLAS_FUZZ_LAYOUT=%s", strings.Join(layout, ",")))
	c.Assert(fromCapture, qt.DeepEquals, names,
		qt.Commentf("the capture and the live directory disagree: PTAH_ATLAS_FUZZ_LAYOUT=%s",
			strings.Join(layout, ",")))
}

// assertImporterConsumesTheCoveredSet checks that the files a Flyway directory
// EXECUTES are the files its atlas.sum COVERS — same members, same order.
//
// This is the third assertion over one population, and the reason there are
// three. The sum comparison proves SumFileNames matches the oracle; the capture
// comparison proves the gate recomputes that same set; neither says anything
// about what the importer runs, and #982 lived in exactly that gap: the hasher
// and the importer were separate rules that agreed on ordinary directories and
// diverged on a superseded baseline and a lowercase prefix. Random generation
// over the same shapes is what stops them drifting apart again.
//
// Two limits worth stating. Membership is a genuine cross-check, but ORDER is
// weaker than it looks: both sides are derived from the same covered slice, so
// this cannot see a projection that assigns versions out of that order — that is
// what the literal expectations in TestLoadFSFlywayAtlasVersions and probe
// section 9a hold. And the enumeration below sits at LoadFS, so a refusal raised
// later, when the migrator registers the converted names, is invisible here
// (see TestLoadFSFlywayDescriptionEndingInDown_KnownDivergence).
//
// A refused directory is checked against the enumerated refusal classes rather
// than skipped. Skipping it would make this assertion vacuous exactly when the
// importer started refusing everything — the one regression it most needs to
// catch — so a refusal outside flywayRefusalClasses fails the shape.
func assertImporterConsumesTheCoveredSet(c *qt.C, dir string, names []string, layout []string) {
	c.Helper()
	loaded, err := atlasmigrateimport.LoadDir(dir, atlasmigrateimport.FormatFlyway)
	if err != nil {
		c.Assert(err, qt.ErrorMatches, flywayRefusalClasses,
			qt.Commentf("unenumerated refusal: PTAH_ATLAS_FUZZ_LAYOUT=%s", strings.Join(layout, ",")))
		return
	}
	bodies := make(map[string]string, len(layout))
	for i, name := range layout {
		bodies[fmt.Sprintf("CREATE TABLE t%d (id INTEGER PRIMARY KEY);\n", i)] = name
	}
	consumed := make([]string, 0, len(loaded.Entries))
	for _, entry := range loaded.Entries {
		consumed = append(consumed, bodies[string(entry.Data)])
	}
	c.Assert(consumed, qt.DeepEquals, names,
		qt.Commentf("the importer executes a different set than atlas.sum covers: PTAH_ATLAS_FUZZ_LAYOUT=%s",
			strings.Join(layout, ",")))
}

// flywayRefusalClasses enumerates every way a Flyway directory may be refused
// once its covered set has been selected. Each one is pinned by a named test in
// flywayselection_test.go; anything else reaching the fuzz is a refusal nobody
// decided on, which for a directory Atlas CE applies is the regression #982's
// fix most needed to avoid.
const flywayRefusalClasses = `no importable migration files found in .* for format "flyway"` +
	`|Flyway migrations .* and .* both carry the (Atlas version ".*"|empty Atlas version .*)` +
	`|Flyway migration .* has version ".*" (with more than 3 components|whose component .* is outside .*|that is too large|with a negative component) .*` +
	`|Flyway migration .* shares its version ordering key with more than the 4 files Ptah can tell apart .*`

// oracleEnv names the environment variable holding the path to the pinned
// Atlas CE binary the differential fuzz compares against.
const oracleEnv = "PTAH_ATLAS_ORACLE"

// oracleVersion is the only build this fuzz trusts. A different build may have
// changed the very rules under test, so comparing against it would report
// divergences that are really version drift.
const oracleVersion = "atlas community version v1.3.0"

func TestAtlasCEOracleLockMatchesDifferentialFuzz(t *testing.T) {
	c := qt.New(t)

	lock, err := os.ReadFile("../../scripts/atlas-ce-oracle.lock")
	c.Assert(err, qt.IsNil)
	version, found := strings.CutPrefix(oracleVersion, "atlas community version ")
	c.Assert(found, qt.IsTrue)
	c.Assert(string(lock), qt.Contains, "\nversion "+version+"\n")
}

// TestSumFileNamesDifferentialFuzz generates random Flyway directories and
// checks Ptah's integrity file set against the live oracle.
//
// It exists because a curated corpus can only cover shapes its author already
// thought of. The 61-shape corpus stayed green over a version comparator that
// was wrong for every pair differing only in trailing zero components (V1 vs
// V1.0), because no curated case happened to pair two such versions. Random
// generation is not shape-blind in that way.
//
// Skipped unless PTAH_ATLAS_ORACLE points at the pinned binary. The skip is
// deliberately loud: a silently absent oracle check is the failure mode of
// scripts/check-test-style.sh (#975), and this test is the main defense for
// rules that no unit test can derive on its own.
func TestSumFileNamesDifferentialFuzz(t *testing.T) {
	oracle := requireOracle(t)
	c := qt.New(t)

	iterations := fuzzIterations(c, 200)
	seed := fuzzSeed(c, 20260801)
	rng := newFuzzRNG(seed)

	t.Logf("differential fuzz: oracle=%s iterations=%d seed=%d", oracle, iterations, seed)

	for i := range iterations {
		layout := randomFlywayLayout(rng)
		c.Run(fmt.Sprintf("shape-%03d", i), func(c *qt.C) {
			checkFlywayLayout(c, oracle, layout)
		})
	}
}

// TestSumFileNamesDifferentialFuzzExplicitLayout reruns one layout named by
// PTAH_ATLAS_FUZZ_LAYOUT. A failing shape prints its layout in that form, which
// is what makes a fuzz failure reducible (see bin/minimize.sh) rather than
// merely reported.
func TestSumFileNamesDifferentialFuzzExplicitLayout(t *testing.T) {
	oracle := requireOracle(t)
	c := qt.New(t)

	checkFlywayLayout(c, oracle, requireFuzzLayout(t))
}

func requireFuzzLayout(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("PTAH_ATLAS_FUZZ_LAYOUT")
	if raw == "" {
		t.Skip("SKIPPED: set PTAH_ATLAS_FUZZ_LAYOUT to rerun one specific layout")
	}
	return strings.Split(raw, ",")
}

func checkFlywayLayout(c *qt.C, oracle string, layout []string) {
	c.Helper()

	dir := c.TempDir()
	writeLayout(c, dir, layout)

	want := oracleSum(c, oracle, dir, "flyway")

	fsys := os.DirFS(dir)
	names, err := atlasmigrateimport.SumFileNames(fsys, atlasmigrateimport.FormatFlyway)

	c.Assert(err, qt.IsNil)

	sum, err := migratesum.ComputeAtlasFiles(fsys, names)
	c.Assert(err, qt.IsNil)

	c.Assert(string(sum.Bytes()), qt.Equals, want,
		qt.Commentf("PTAH_ATLAS_FUZZ_LAYOUT=%s", strings.Join(layout, ",")))
	assertCaptureSelectsTheSame(c, dir, atlasmigrateimport.FormatFlyway, names, layout)
	assertImporterConsumesTheCoveredSet(c, dir, names, layout)
}

// TestSumFileNamesDifferentialFuzzRealisticFlyway restricts generation to what
// a real Flyway project contains: V/B/R/U prefixes, integer and dotted-integer
// versions, "__" separators, and plain subfolders. On this population the
// contract is stricter than never-silently-wrong — every shape must match the
// oracle exactly, and none may be refused.
//
// The split matters because the narrow refusal must never become a way to make
// the broad fuzz pass. A realistic project cannot reach it.
func TestSumFileNamesDifferentialFuzzRealisticFlyway(t *testing.T) {
	oracle := requireOracle(t)
	c := qt.New(t)

	iterations := fuzzIterations(c, 200)
	rng := newFuzzRNG(fuzzSeed(c, 20260803))

	for i := range iterations {
		layout := randomRealisticFlywayLayout(rng)
		c.Run(fmt.Sprintf("realistic-%03d", i), func(c *qt.C) {
			dir := c.TempDir()
			writeLayout(c, dir, layout)

			want := oracleSum(c, oracle, dir, "flyway")

			fsys := os.DirFS(dir)
			names, err := atlasmigrateimport.SumFileNames(fsys, atlasmigrateimport.FormatFlyway)
			c.Assert(err, qt.IsNil, qt.Commentf("a realistic layout must never be refused: PTAH_ATLAS_FUZZ_LAYOUT=%s",
				strings.Join(layout, ",")))

			sum, err := migratesum.ComputeAtlasFiles(fsys, names)
			c.Assert(err, qt.IsNil)
			c.Assert(string(sum.Bytes()), qt.Equals, want,
				qt.Commentf("PTAH_ATLAS_FUZZ_LAYOUT=%s", strings.Join(layout, ",")))
			assertCaptureSelectsTheSame(c, dir, atlasmigrateimport.FormatFlyway, names, layout)
			assertImporterConsumesTheCoveredSet(c, dir, names, layout)
		})
	}
}

// randomRealisticFlywayLayout generates only what a real project writes.
func randomRealisticFlywayLayout(rng *rand.Rand) []string {
	dirs := []string{""}
	for range rng.IntN(3) {
		dirs = append(dirs, [...]string{
			"sub", "views", "seed", "a/b",
			"0archive", "1old", "2tmp", "9z", "Archive", "Legacy", "V2", "a/0b",
		}[rng.IntN(12)])
	}

	count := 1 + rng.IntN(8)
	seen := make(map[string]bool, count)
	layout := make([]string, 0, count)
	for range count {
		prefix := [...]string{"V", "V", "V", "V", "V", "B", "R", "U"}[rng.IntN(8)]
		version := strconv.Itoa(rng.IntN(20))
		if rng.IntN(3) == 0 {
			version += "." + strconv.Itoa(rng.IntN(10))
		}
		if prefix == "R" {
			version = ""
		}
		description := [...]string{"init", "seed", "add_users", "create_index", "views"}[rng.IntN(5)]
		name := path4Join(dirs[rng.IntN(len(dirs))], prefix+version+"__"+description+".sql")
		if seen[name] {
			continue
		}
		seen[name] = true
		layout = append(layout, name)
	}
	return layout
}

// TestSumFileNamesDifferentialFuzzOtherFormats does the same for the formats
// whose rule is a flat suffix filter, so a future change there is caught too.
func TestSumFileNamesDifferentialFuzzOtherFormats(t *testing.T) {
	oracle := requireOracle(t)
	c := qt.New(t)

	rng := newFuzzRNG(20260802)

	formats := []atlasmigrateimport.Format{
		atlasmigrateimport.FormatAtlas,
		atlasmigrateimport.FormatGoose,
		atlasmigrateimport.FormatDBMate,
		atlasmigrateimport.FormatLiquibase,
		atlasmigrateimport.FormatGolangMigrate,
	}
	for _, format := range formats {
		for i := range 40 {
			layout := randomPlainLayout(rng)
			c.Run(fmt.Sprintf("%s-%02d", format, i), func(c *qt.C) {
				dir := c.TempDir()
				writeLayout(c, dir, layout)

				want := oracleSum(c, oracle, dir, string(format))

				fsys := os.DirFS(dir)
				names, err := atlasmigrateimport.SumFileNames(fsys, format)
				c.Assert(err, qt.IsNil)
				sum, err := migratesum.ComputeAtlasFiles(fsys, names)
				c.Assert(err, qt.IsNil)

				c.Assert(string(sum.Bytes()), qt.Equals, want, qt.Commentf("layout:\n  %s", strings.Join(layout, "\n  ")))
				assertCaptureSelectsTheSame(c, dir, format, names, layout)
			})
		}
	}
}

// fuzzIterations, fuzzSeed and newFuzzRNG keep the knob parsing out
// of the test bodies, which the repository's teststyle ratchet counts.
func fuzzIterations(c *qt.C, fallback int) int {
	c.Helper()
	raw := os.Getenv("PTAH_ATLAS_FUZZ_N")
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	c.Assert(err, qt.IsNil)
	return parsed
}

func fuzzSeed(c *qt.C, fallback uint64) uint64 {
	c.Helper()
	raw := os.Getenv("PTAH_ATLAS_FUZZ_SEED")
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	c.Assert(err, qt.IsNil)
	return parsed
}

func newFuzzRNG(seed uint64) *rand.Rand {
	// Reproducibility is the requirement here, not unpredictability.
	return rand.New(rand.NewPCG(seed, 0x9E3779B97F4A7C15)) //nolint:gosec // deterministic shape generation, not security
}

func requireOracle(t *testing.T) string {
	t.Helper()

	oracle := os.Getenv(oracleEnv)
	if oracle == "" {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s) to run the differential fuzz",
			oracleEnv, oracleVersion)
	}

	out, err := exec.Command(oracle, "version").Output() //nolint:gosec // the oracle path is operator-provided via PTAH_ATLAS_ORACLE
	if err != nil {
		t.Fatalf("%s=%s is not runnable: %v", oracleEnv, oracle, err)
	}
	got, _, _ := strings.Cut(string(out), "\n")
	if strings.TrimSpace(got) != oracleVersion {
		t.Fatalf("%s=%s reports %q, want %q; a different build may have changed the rules under test",
			oracleEnv, oracle, strings.TrimSpace(got), oracleVersion)
	}
	return oracle
}

func oracleSum(c *qt.C, oracle, dir, format string) string {
	c.Helper()

	//nolint:gosec // operator-provided oracle path, and dir is a test temp dir
	cmd := exec.Command(oracle, "migrate", "hash", "--dir", "file://"+dir+"?format="+format)
	out, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("oracle hash failed: %s", out))

	recorded, err := os.ReadFile(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)

	// The oracle's own sum file must not feed back into Ptah's computation.
	c.Assert(os.Remove(filepath.Join(dir, migratesum.AtlasFileName)), qt.IsNil)
	return string(recorded)
}

func writeLayout(c *qt.C, dir string, layout []string) {
	c.Helper()
	for i, name := range layout {
		full := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(full), 0o750), qt.IsNil)
		// Distinct bodies so a misordered sum cannot coincidentally match.
		body := fmt.Sprintf("CREATE TABLE t%d (id INTEGER PRIMARY KEY);\n", i)
		c.Assert(os.WriteFile(full, []byte(body), 0o600), qt.IsNil)
	}
}

// randomFlywayLayout builds a realistic-to-adversarial Flyway directory: mostly
// ordinary versioned migrations, with baselines, undo and repeatable files,
// subdirectories, hidden directories, and the version-token shapes that
// separate the candidate comparators.
func randomFlywayLayout(rng *rand.Rand) []string {
	dirs := []string{""}
	for range rng.IntN(3) {
		// The pool deliberately spans digits, uppercase, lowercase and
		// punctuation: the backwards reach compares a PATH against a version
		// token, so a pool of only lowercase names cannot exercise it.
		dirs = append(dirs, [...]string{
			"sub", "views", "a/b", ".archive", ".hidden/deep",
			"0archive", "1old", "2tmp", "9z", "Archive", "Legacy",
			"B3", "V", "a/0b", "-dash", "_under",
		}[rng.IntN(16)])
	}

	count := 1 + rng.IntN(7)
	seen := make(map[string]bool, count)
	layout := make([]string, 0, count)
	for range count {
		name := path4Join(dirs[rng.IntN(len(dirs))], randomFlywayName(rng))
		if seen[name] {
			continue
		}
		seen[name] = true
		layout = append(layout, name)
	}
	return layout
}

func randomFlywayName(rng *rand.Rand) string {
	prefix := [...]string{"V", "V", "V", "V", "B", "R", "U"}[rng.IntN(7)]
	version := randomFlywayVersion(rng)
	description := [...]string{"init", "seed", "add_users", "x", "a__b", ""}[rng.IntN(6)]
	if rng.IntN(8) == 0 {
		// No separator at all: still a covered migration.
		return prefix + version + ".sql"
	}
	return prefix + version + "__" + description + ".sql"
}

func randomFlywayVersion(rng *rand.Rand) string {
	switch rng.IntN(12) {
	case 0, 1, 2, 3:
		return strconv.Itoa(rng.IntN(12))
	case 4, 5:
		// Trailing-zero shapes: the class the curated corpus could not see.
		return [...]string{"1", "1.0", "1.0.0", "2", "2.0", "10", "10.0"}[rng.IntN(7)]
	case 6:
		return strconv.Itoa(rng.IntN(4)) + "." + strconv.Itoa(rng.IntN(9))
	case 7:
		return strconv.Itoa(rng.IntN(4)) + "_" + strconv.Itoa(rng.IntN(9))
	case 8:
		return [...]string{"", ".", "1.", ".1", "x", "1.x", "x.5"}[rng.IntN(7)]
	case 9:
		return "-" + strconv.Itoa(1+rng.IntN(9))
	case 10:
		return "20240101120000000000"
	default:
		return "0" + strconv.Itoa(rng.IntN(9))
	}
}

// randomPlainLayout builds directories for the suffix-filter formats: a mix of
// covered and uncovered extensions, plus subdirectories that must be ignored.
func randomPlainLayout(rng *rand.Rand) []string {
	count := 1 + rng.IntN(6)
	seen := make(map[string]bool, count)
	layout := make([]string, 0, count)
	for i := range count {
		base := [...]string{
			"1_init.sql", "2_more.sql", "foo.sql", "notes.SQL", "readme.md",
			"3_x.up.sql", "3_x.down.sql", "4_y.up.sql", "seed.go", "changelog.xml",
		}[rng.IntN(10)]
		dir := ""
		if rng.IntN(4) == 0 {
			dir = [...]string{"sub", "a/b", ".hidden"}[rng.IntN(3)]
		}
		name := path4Join(dir, fmt.Sprintf("%d_%s", i, base))
		if seen[name] {
			continue
		}
		seen[name] = true
		layout = append(layout, name)
	}
	return layout
}

func path4Join(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}
