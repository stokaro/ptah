package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin the measured `migrate import` source-integrity gate
// (stokaro/ptah#1095). Before it, `migrate import` was the one verb that read a
// migration directory and its atlas.sum without ever verifying them: a
// directory `migrate apply` refuses was converted, written out, and re-hashed,
// so the tampering ended up laundered into a destination that verifies clean.
//
// Everything asserted here was measured against the pinned community binary
// v1.3.0 at ptah-atlas-conformance/bin/atlas, on hashed source directories in
// all five importable layouts:
//
//	never hashed          exit 0, imports  <- NOT the apply-side rule
//	hashed clean          exit 0, imports
//	hashed then edited    exit 1, "checksum mismatch", L<n> names the SOURCE file
//	hashed then added     exit 1, "checksum mismatch", "<file> was added"
//	hashed then removed   exit 1, "checksum mismatch", "<file> was removed"
//	hashed, sum malformed exit 1, "checksum mismatch", no L<n> line
//
// The first row is where import parts company with apply, status, set, down and
// lint: on a directory with atlas.sum deleted, `migrate apply` exits 1 with
// `checksum file not found` while `migrate import` exits 0 and writes. That is
// the right way round — import exists to read a directory another tool wrote,
// which by construction was never hashed — and
// TestCompatMigrateImport_UnhashedSourceStillImports is what keeps the gate
// from over-refusing into it.

// compatImport runs `ptah-compat migrate import` with the source layout named
// through the --from query, the spelling the pinned binary's own examples use.
func compatImport(from, to, format string) (stdout, stderr string, err error) {
	return runCompat("migrate", "import", "--from", "file://"+from+"?format="+format, "--to", "file://"+to)
}

// importDirs returns a fresh (source, destination) pair under one temp root.
// The destination is a path that does not exist, so "was anything written?" is
// answerable by a single stat instead of by inspecting an empty directory the
// test itself created.
func importDirs(c *qt.C) (source, target string) {
	c.Helper()
	root := c.TempDir()
	return filepath.Join(root, "src"), filepath.Join(root, "dst")
}

// assertNothingImported asserts the destination was never created. It is the
// assertion the exit code alone cannot make: a gate that refuses AFTER the
// conversion has written the destination and hashed it still leaves a clean
// looking directory behind, which is the laundering half of #1095.
func assertNothingImported(c *qt.C, target string) {
	c.Helper()
	_, err := os.Stat(target)
	c.Assert(os.IsNotExist(err), qt.IsTrue,
		qt.Commentf("the refusal must precede writing anything to %s", target))
}

// destinationSum returns the atlas.sum an import wrote, or the empty string
// when nothing was written. The read error is dropped on purpose: its absence
// IS the expected outcome here, so turning it into a branch would put an `if`
// in the assertion path instead of in the comparison.
func destinationSum(c *qt.C, target string) string {
	c.Helper()
	data, _ := os.ReadFile(filepath.Join(target, "atlas.sum"))
	return string(data)
}

// TestCompatMigrateImport_HashedCleanSourceImports is the non-interference
// control for the gate: a hashed, untampered source in every importable layout
// still converts and still writes its destination sum.
//
// Reverting the gate leaves this green, which is the point of it — it is
// validated by the INVERSE mutant instead. Make verifyCoveredAtlasDirChecksum
// refuse unconditionally and all five rows go red on the import error.
func TestCompatMigrateImport_HashedCleanSourceImports(t *testing.T) {
	for _, fixture := range convertedApplyFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			c := qt.New(t)
			source, target := importDirs(c)
			writeConvertedApplyDir(c, source, fixture.files)
			hashConvertedApplyDir(c, source, fixture.format)

			stdout, stderr, err := compatImport(source, target, fixture.format)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
			_, statErr := os.Stat(filepath.Join(target, "atlas.sum"))
			c.Assert(statErr, qt.IsNil)
		})
	}
}

// TestCompatMigrateImport_UnhashedSourceStillImports pins the exemption that
// separates import from every other gated verb.
//
// A directory written by goose, dbmate, liquibase, flyway or golang-migrate
// carries no atlas.sum until somebody runs `atlas migrate hash` on it, and
// importing it is the entire point of the verb. Measured: the pinned binary
// exits 0 here, while on the SAME directory `migrate apply` exits 1 with
// `checksum file not found`. Adopting the apply-side policy would refuse the
// verb's primary use.
//
// Reverting the gate leaves this green; switching the import call site to
// requireAtlasSum turns all five rows red with `checksum file not found`.
func TestCompatMigrateImport_UnhashedSourceStillImports(t *testing.T) {
	for _, fixture := range convertedApplyFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			c := qt.New(t)
			source, target := importDirs(c)
			writeConvertedApplyDir(c, source, fixture.files)

			stdout, stderr, err := compatImport(source, target, fixture.format)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
			_, statErr := os.Stat(filepath.Join(target, "atlas.sum"))
			c.Assert(statErr, qt.IsNil)
		})
	}
}

// TestCompatMigrateImport_DriftingSourceRefused covers the three drift shapes
// Atlas CE distinguishes, in all five importable layouts, and asserts both
// halves: the refusal is byte-identical to the pinned binary's, and nothing was
// written before it.
//
// Each L<n> line names the SOURCE file, never a converted name: for
// golang-migrate the oracle prints `1_init.up.sql`, not the `1_init.sql` the
// converter would have produced.
//
// Reverted, every row prints `err: <nil>` where the test wants a non-nil error,
// empty stdout where it wants the checksum block, and a destination holding
// atlas.sum where assertNothingImported wants no destination at all.
func TestCompatMigrateImport_DriftingSourceRefused(t *testing.T) {
	for _, fixture := range convertedApplyFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			states := []struct {
				name string
				// mutate tampers with the hashed source without re-hashing it.
				mutate func(c *qt.C, dir string)
				line   int
				file   string
				reason string
			}{
				{
					name: "edited",
					mutate: func(c *qt.C, dir string) {
						appendToFile(c, filepath.Join(dir, fixture.covered), "\n-- tampered, sum not rehashed\n")
					},
					line: 2, file: fixture.covered, reason: "edited",
				},
				{
					name: "added",
					mutate: func(c *qt.C, dir string) {
						writeConvertedApplyDir(c, dir, map[string]string{fixture.extra: fixture.extraBody})
					},
					line: 3, file: fixture.extra, reason: "added",
				},
				{
					name: "removed",
					mutate: func(c *qt.C, dir string) {
						c.Assert(os.Remove(filepath.Join(dir, fixture.covered)), qt.IsNil)
					},
					line: 2, file: fixture.covered, reason: "removed",
				},
			}
			for _, state := range states {
				t.Run(state.name, func(t *testing.T) {
					c := qt.New(t)
					source, target := importDirs(c)
					writeConvertedApplyDir(c, source, fixture.files)
					hashConvertedApplyDir(c, source, fixture.format)
					state.mutate(c, source)

					stdout, stderr, err := compatImport(source, target, fixture.format)

					c.Assert(err, qt.IsNotNil)
					c.Assert(err.Error(), qt.Equals, "checksum mismatch")
					c.Assert(stdout, qt.Equals, atlasChecksumMismatchStdout(state.line, state.file, state.reason))
					c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
					assertNothingImported(c, target)
				})
			}
		})
	}
}

// The two bodies below differ by one statement inside the goose UP section, so
// the difference survives conversion. That placement is deliberate: appending
// to the END of a goose file lands in the DOWN section, which the importer
// drops, so a tampered file whose only edit is down there converts to exactly
// the same Atlas migration and produces exactly the same destination sum. That
// edit is still a checksum error on both tools — the sum covers source bytes,
// not converted ones — but it cannot demonstrate laundering, because nothing
// harmful reaches the destination.
const (
	gooseLaunderCleanBody = "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n" +
		"-- +goose Down\nDROP TABLE widgets;\n"
	gooseLaunderTamperedBody = "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n" +
		"CREATE TABLE pwned (id INTEGER PRIMARY KEY);\n" +
		"-- +goose Down\nDROP TABLE widgets;\n"
)

// TestCompatMigrateImport_TamperedSourceIsNotLaunderedIntoACleanDirectory is
// the fixture #1095 asks for by name: it does not stop at the exit code, it
// shows the destination's sum.
//
// The three legs are one experiment. Legs 1 and 2 import two sources that
// differ only by the injected `CREATE TABLE pwned`, each legitimately hashed,
// and show that the injected statement reaches a fresh, self-consistent
// destination atlas.sum that `migrate validate` would call clean. Leg 3 is the
// same tampered content with the source sum NOT re-hashed — the attacker's
// position — and it must produce no destination at all.
//
// Reverted, leg 3 exits 0 and writes a destination whose atlas.sum equals leg
// 2's byte for byte, and whose 1_init.sql holds the injected statement: the
// laundering stated as an equality rather than as a worry.
func TestCompatMigrateImport_TamperedSourceIsNotLaunderedIntoACleanDirectory(t *testing.T) {
	c := qt.New(t)
	const format = "goose"
	cleanFiles := map[string]string{"1_init.sql": gooseLaunderCleanBody}
	tamperedFiles := map[string]string{"1_init.sql": gooseLaunderTamperedBody}

	// Leg 1: the honest directory.
	cleanSource, cleanTarget := importDirs(c)
	writeConvertedApplyDir(c, cleanSource, cleanFiles)
	hashConvertedApplyDir(c, cleanSource, format)
	_, _, cleanErr := compatImport(cleanSource, cleanTarget, format)
	c.Assert(cleanErr, qt.IsNil)
	cleanSum, err := os.ReadFile(filepath.Join(cleanTarget, "atlas.sum"))
	c.Assert(err, qt.IsNil)

	// Leg 2: the same injection, but re-hashed, so the source is
	// self-consistent and the import is legitimate. Its destination is what leg
	// 3 must not be allowed to produce.
	rehashedSource, rehashedTarget := importDirs(c)
	writeConvertedApplyDir(c, rehashedSource, tamperedFiles)
	hashConvertedApplyDir(c, rehashedSource, format)
	_, _, rehashedErr := compatImport(rehashedSource, rehashedTarget, format)
	c.Assert(rehashedErr, qt.IsNil)
	tamperedSum, err := os.ReadFile(filepath.Join(rehashedTarget, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(tamperedSum), qt.Not(qt.Equals), string(cleanSum),
		qt.Commentf("the injected statement must reach the destination sum, or leg 3 proves nothing"))
	imported, err := os.ReadFile(filepath.Join(rehashedTarget, "1_init.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(imported), qt.Contains, "CREATE TABLE pwned")

	// Leg 3: the attacker's directory — injected, source sum left as it was.
	forgedSource, forgedTarget := importDirs(c)
	writeConvertedApplyDir(c, forgedSource, cleanFiles)
	hashConvertedApplyDir(c, forgedSource, format)
	writeConvertedApplyDir(c, forgedSource, tamperedFiles)

	stdout, stderr, forgedErr := compatImport(forgedSource, forgedTarget, format)

	// The laundering, as an equality: without the gate this destination sum is
	// leg 2's byte for byte, so the tampered source has been converted into a
	// directory `migrate validate` calls clean.
	c.Assert(destinationSum(c, forgedTarget), qt.Not(qt.Equals), string(tamperedSum))
	c.Assert(destinationSum(c, forgedTarget), qt.Equals, "")
	assertNothingImported(c, forgedTarget)
	c.Assert(forgedErr, qt.IsNotNil)
	c.Assert(forgedErr.Error(), qt.Equals, "checksum mismatch")
	c.Assert(stdout, qt.Equals, atlasChecksumMismatchStdout(2, "1_init.sql", "edited"))
	c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
}

// TestCompatMigrateImport_ChecksumRefusalOutranksTheDestinationChecks pins the
// ORDER the gate runs in, which is measured rather than chosen.
//
// On the pinned binary a tampered source is refused for its checksum even when
// the destination already holds SQL, and even when --to names the source
// itself; both of those are refusals `migrate import` produces on its own, and
// both used to win here because they ran first. The order matters beyond the
// message: the destination checks are the last thing standing between a
// tampered source and a written directory, so any of them passing on a source
// the checksum has not cleared is the bug.
//
// Reverted, the first row reports `target migration directory already contains
// SQL file: .../9_existing.sql` and the second `import --to must be different
// from --from for format "goose"` — both exit 1, both the wrong refusal, and
// the second one having already read the tampered directory.
func TestCompatMigrateImport_ChecksumRefusalOutranksTheDestinationChecks(t *testing.T) {
	fixture := convertedApplyFixtures()[0]
	tests := []struct {
		name string
		// prepare tampers with the hashed source and returns the destination to
		// import into, so each row carries its own wiring.
		prepare func(c *qt.C, source, target string) string
	}{
		{
			name: "destination already holds migrations",
			prepare: func(c *qt.C, source, target string) string {
				appendToFile(c, filepath.Join(source, fixture.covered), "\n-- tampered\n")
				writeConvertedApplyDir(c, target, map[string]string{"9_existing.sql": "SELECT 1;\n"})
				return target
			},
		},
		{
			name: "destination is the source directory",
			prepare: func(c *qt.C, source, _ string) string {
				appendToFile(c, filepath.Join(source, fixture.covered), "\n-- tampered\n")
				return source
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			source, target := importDirs(c)
			writeConvertedApplyDir(c, source, fixture.files)
			hashConvertedApplyDir(c, source, fixture.format)

			stdout, stderr, err := compatImport(source, tt.prepare(c, source, target), fixture.format)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, "checksum mismatch")
			c.Assert(stdout, qt.Equals, atlasChecksumMismatchStdout(2, fixture.covered, "edited"))
			c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
		})
	}
}

// TestCompatMigrateImport_MalformedSumRefused covers the sum file that cannot
// be parsed at all. There is no entry-level mismatch to name, so the pinned
// binary prints the guidance block with no L<n> line and still exits 1 —
// the same shape `migrate apply` produces on the same directory.
//
// Reverted, this prints an empty stdout, a nil error, and a destination
// directory holding a freshly written atlas.sum.
func TestCompatMigrateImport_MalformedSumRefused(t *testing.T) {
	c := qt.New(t)
	fixture := convertedApplyFixtures()[0]
	source, target := importDirs(c)
	writeConvertedApplyDir(c, source, fixture.files)
	writeConvertedApplyDir(c, source, map[string]string{"atlas.sum": "not a sum file\n"})

	stdout, stderr, err := compatImport(source, target, fixture.format)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum mismatch")
	c.Assert(stdout, qt.Equals, atlasChecksumFileNotFoundStdout)
	c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
	assertNothingImported(c, target)
}

// TestCompatMigrateImport_CoveredEntryThatIsADirectoryRefused covers the #991
// shape on the import path: a DIRECTORY named like a migration is a member of
// the covered set, because Atlas CE reaches a non-Flyway layout through a glob
// that matches on the name alone. The read that follows fails, and the pinned
// binary refuses the whole directory rather than hashing what is left.
//
// It is the row that separates "verify the files" from "verify the covered
// set": `2_evil.sql` is not a file the importer would ever convert, and a gate
// that only checked convertible files would let it through.
//
// The UNHASHED row is the one that keeps `verifyAtlasSumWhenPresent` from
// short-circuiting past the read. Measured, the pinned binary refuses both
// rows identically — membership of the covered set is decided by the name, so
// the entry is a member whether or not anything recorded a hash for it. An
// import policy that returned early on `!hashed` would exit 0 here and convert,
// which is the direction parity must never take.
//
// Reverted, both rows print a nil error, an empty stdout and a written
// destination. Removing only checkCoveredAtlasEntriesReadable reddens the
// unhashed row alone.
func TestCompatMigrateImport_CoveredEntryThatIsADirectoryRefused(t *testing.T) {
	fixture := convertedApplyFixtures()[0]
	tests := []struct {
		name string
		// hash decides whether the source carries an atlas.sum at all, which is
		// the axis this table exists to separate.
		hash func(c *qt.C, dir string)
	}{
		{
			name: "hashed",
			hash: func(c *qt.C, dir string) { hashConvertedApplyDir(c, dir, fixture.format) },
		},
		{
			name: "never hashed",
			hash: func(_ *qt.C, _ string) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			source, target := importDirs(c)
			writeConvertedApplyDir(c, source, fixture.files)
			tt.hash(c, source)
			c.Assert(os.MkdirAll(filepath.Join(source, "2_evil.sql"), 0o755), qt.IsNil)

			stdout, stderr, err := compatImport(source, target, fixture.format)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, atlasChecksumFileNotFoundStdout)
			c.Assert(stderr, qt.Contains, `read file "2_evil.sql": is a directory`)
			assertNothingImported(c, target)
		})
	}
}
