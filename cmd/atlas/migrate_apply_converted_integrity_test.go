package atlas_test

import (
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/testutils"
)

// errorText renders err for a containment assertion, so a test can say "this
// output mentions no checksum" without branching on whether there was an error
// at all.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// These tests pin the measured Atlas CE v1.2.0 apply-time integrity gate for
// migration directories laid out in a foreign tool's convention and read
// through `?format=` (stokaro/ptah#973).
//
// Everything asserted here was measured against the pinned oracle at
// ptah-atlas-conformance/bin/atlas, reproducible with
// scripts/probe-atlas-apply-gate.sh. The short version, five formats each:
//
//	never hashed          exit 1, "checksum file not found", no database created
//	hashed clean          exit 0, applies
//	hashed then edited    exit 1, "checksum mismatch", L<n> names the SOURCE file
//	hashed then added     exit 1, "checksum mismatch", "<file> was added"
//	hashed then removed   exit 1, "checksum mismatch", "<file> was removed"
//
// The refusal is not "any *.sql is unhashed": it is "the file set this layout's
// atlas.sum covers is non-empty and unverified". The negative tests below are
// the half that keeps a gate from over-refusing, and they are the reason the
// covered set is computed per format rather than globbed.

// convertedApplyFixture is one source directory in a foreign tool's layout,
// together with the file Atlas CE covers and one it would add.
type convertedApplyFixture struct {
	format string
	files  map[string]string
	// covered is a file Atlas CE hashes for this layout. Editing it must be a
	// checksum mismatch naming this exact name.
	covered string
	// extra is a file CE would also hash, used for the "added" state.
	extra string
	// extraBody is the content written for extra, in the layout's own syntax.
	extraBody string
}

func convertedApplyFixtures() []convertedApplyFixture {
	return []convertedApplyFixture{
		{
			format:    "goose",
			files:     map[string]string{"1_init.sql": "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n-- +goose Down\nDROP TABLE widgets;\n"},
			covered:   "1_init.sql",
			extra:     "2_extra.sql",
			extraBody: "-- +goose Up\nCREATE TABLE extra (id INTEGER PRIMARY KEY);\n",
		},
		{
			format:    "dbmate",
			files:     map[string]string{"1_init.sql": "-- migrate:up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n-- migrate:down\nDROP TABLE widgets;\n"},
			covered:   "1_init.sql",
			extra:     "2_extra.sql",
			extraBody: "-- migrate:up\nCREATE TABLE extra (id INTEGER PRIMARY KEY);\n",
		},
		{
			format:    "liquibase",
			files:     map[string]string{"1_init.sql": "--liquibase formatted sql\n--changeset app:1\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n--rollback DROP TABLE widgets;\n"},
			covered:   "1_init.sql",
			extra:     "2_extra.sql",
			extraBody: "--liquibase formatted sql\n--changeset app:2\nCREATE TABLE extra (id INTEGER PRIMARY KEY);\n",
		},
		{
			format:    "flyway",
			files:     map[string]string{"V1__init.sql": "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"},
			covered:   "V1__init.sql",
			extra:     "V2__extra.sql",
			extraBody: "CREATE TABLE extra (id INTEGER PRIMARY KEY);\n",
		},
		{
			// The discriminating layout: Atlas CE covers only the up file, so
			// the down file below is present in every state and never hashed.
			format: "golang-migrate",
			files: map[string]string{
				"1_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
				"1_init.down.sql": "DROP TABLE widgets;\n",
			},
			covered:   "1_init.up.sql",
			extra:     "2_extra.up.sql",
			extraBody: "CREATE TABLE extra (id INTEGER PRIMARY KEY);\n",
		},
	}
}

// writeConvertedApplyDir writes files under dir and returns dir.
func writeConvertedApplyDir(c *qt.C, dir string, files map[string]string) string {
	c.Helper()
	for name, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// hashConvertedApplyDir writes atlas.sum over dir's source files with the same
// `ptah-compat migrate hash` a user runs, so these tests exercise the hash then
// apply round trip rather than a sum assembled by the test.
//
// Round-tripping through Ptah's own hasher cannot, on its own, prove the gate
// uses Atlas CE's covered set: a wrong rule in both halves agrees with itself.
// TestCompatMigrateApply_ConvertedDirVerifiesOracleWrittenSum pins that against
// sums copied verbatim from the pinned oracle.
func hashConvertedApplyDir(c *qt.C, dir, format string) {
	c.Helper()
	stdout, stderr, err := runCompat("migrate", "hash", "--dir", "file://"+dir+"?format="+format)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
}

// appendToFile edits a migration in place without re-hashing.
func appendToFile(c *qt.C, path, text string) {
	c.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	c.Assert(err, qt.IsNil)
	_, err = file.WriteString(text)
	c.Assert(err, qt.IsNil)
	c.Assert(file.Close(), qt.IsNil)
}

func compatApplyConverted(dir, format, dbPath string, extra ...string) (stdout, stderr string, err error) {
	args := append([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + dir + "?format=" + format,
	}, extra...)
	return runCompat(args...)
}

const (
	atlasChecksumFileNotFoundStdout = "You have a checksum error in your migration directory.\n" +
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"
	atlasChecksumFileNotFoundStderr = "Error: checksum file not found\n"
	atlasChecksumMismatchStderr     = "Error: checksum mismatch\n"
)

func atlasChecksumMismatchStdout(line int, file, reason string) string {
	return "You have a checksum error in your migration directory.\n" +
		"\n\tL" + strconv.Itoa(line) + ": " + file + " was " + reason + "\n\n" +
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"
}

// TestCompatMigrateApply_ConvertedDirUnhashedRefuses replaces the former
// TestCompatMigrateApply_ConvertedDirStaysUngated_KnownDivergence. That test
// pinned the gap this one closes: a converted directory that was never hashed
// used to apply, while Atlas CE refuses it before creating the database.
func TestCompatMigrateApply_ConvertedDirUnhashedRefuses(t *testing.T) {
	for _, fixture := range convertedApplyFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), fixture.files)
			dbPath := filepath.Join(tempDir, "converted.db")

			stdout, stderr, err := compatApplyConverted(dir, fixture.format, dbPath)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, "checksum file not found")
			c.Assert(stdout, qt.Equals, atlasChecksumFileNotFoundStdout)
			c.Assert(stderr, qt.Equals, atlasChecksumFileNotFoundStderr)
			// Measured on the oracle: the target is never created.
			_, statErr := os.Stat(dbPath)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
		})
	}
}

func TestCompatMigrateApply_ConvertedDirHashedCleanApplies(t *testing.T) {
	for _, fixture := range convertedApplyFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), fixture.files)
			hashConvertedApplyDir(c, dir, fixture.format)
			dbPath := filepath.Join(tempDir, "converted.db")

			stdout, stderr, err := compatApplyConverted(dir, fixture.format, dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(sqliteTableCount(c, dbPath, "widgets"), qt.Equals, 1)
		})
	}
}

// TestCompatMigrateApply_ConvertedDirDriftRefuses covers the three drift shapes
// Atlas CE distinguishes. Each names the SOURCE file, never a converted name:
// for golang-migrate the oracle prints `1_init.up.sql`, not the `1_init.sql`
// the converter produces.
func TestCompatMigrateApply_ConvertedDirDriftRefuses(t *testing.T) {
	for _, fixture := range convertedApplyFixtures() {
		t.Run(fixture.format, func(t *testing.T) {
			states := []struct {
				name string
				// writeAfterHash are files written once atlas.sum has been
				// computed, so the sum no longer describes the directory: an
				// existing name is an edit, a new one is an addition.
				writeAfterHash map[string]string
				// removeAfterHash are files deleted once atlas.sum has been
				// computed.
				removeAfterHash []string
				line            int
				file            string
				reason          string
			}{
				{
					name: "edited",
					writeAfterHash: map[string]string{
						fixture.covered: fixture.files[fixture.covered] + "\n-- tampered, sum not rehashed\n",
					},
					line: 2, file: fixture.covered, reason: "edited",
				},
				{
					name:           "added",
					writeAfterHash: map[string]string{fixture.extra: fixture.extraBody},
					line:           3, file: fixture.extra, reason: "added",
				},
				{
					name:            "removed",
					removeAfterHash: []string{fixture.covered},
					line:            2, file: fixture.covered, reason: "removed",
				},
			}
			for _, state := range states {
				t.Run(state.name, func(t *testing.T) {
					c := qt.New(t)
					tempDir := c.TempDir()
					dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), fixture.files)
					hashConvertedApplyDir(c, dir, fixture.format)
					writeConvertedApplyDir(c, dir, state.writeAfterHash)
					for _, name := range state.removeAfterHash {
						c.Assert(os.Remove(filepath.Join(dir, name)), qt.IsNil)
					}
					dbPath := filepath.Join(tempDir, "converted.db")

					stdout, stderr, err := compatApplyConverted(dir, fixture.format, dbPath)

					c.Assert(err, qt.IsNotNil)
					c.Assert(err.Error(), qt.Equals, "checksum mismatch")
					c.Assert(stdout, qt.Equals, atlasChecksumMismatchStdout(state.line, state.file, state.reason))
					c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
					_, statErr := os.Stat(dbPath)
					c.Assert(os.IsNotExist(statErr), qt.IsTrue)
				})
			}
		})
	}
}

// TestCompatMigrateApply_ConvertedDirVerifiesOracleWrittenSum is the test the
// round-trip ones cannot be: every atlas.sum below was written by the pinned
// Atlas CE v1.2.0 binary and is copied in verbatim, so a covered-set rule that
// is self-consistently wrong in both Ptah's hasher and Ptah's verifier still
// fails here.
//
// Each layout is chosen because it separates the per-format rule from the
// plausible alternative of globbing every *.sql:
//
//   - golang-migrate: a down file sits beside the up file and is not in the sum.
//     A glob would hash it, compute a different digest, and refuse.
//   - flyway with an undo file: U1__undo.sql is not in the sum, same reasoning.
//   - flyway with a nested file: sub/V2__nested.sql IS in the sum, so a verifier
//     that only ever looks at the top level cannot even read what it must hash.
//   - flyway with a baseline: V1__one.sql is squashed away by B2__base.sql and
//     drops out of the sum entirely, which no suffix rule reproduces.
func TestCompatMigrateApply_ConvertedDirVerifiesOracleWrittenSum(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		files   map[string]string
		oracle  string
		wantTbl string
	}{
		{
			name:   "golang-migrate does not cover the down file",
			format: "golang-migrate",
			files: map[string]string{
				"1_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
				"1_init.down.sql": "DROP TABLE widgets;\n",
			},
			oracle: "h1:kLQfGRFTT89eJ+AUaleT+/c077hkM+x6tfc4UypYjZs=\n" +
				"1_init.up.sql h1:Mkd1ScxYQTnH/OgnXF2f/CMiaucf3GrraUCxWMd25G8=\n",
			wantTbl: "widgets",
		},
		{
			name:   "flyway does not cover the undo file",
			format: "flyway",
			files: map[string]string{
				"V1__init.sql": "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
				"U1__undo.sql": "DROP TABLE widgets;\n",
			},
			oracle: "h1:JgadXWUJDWcE3tTqFmxhkX7ZPxDzCIfqUKZ6rADoM3A=\n" +
				"V1__init.sql h1:Lcem1A1NyIMMjubsHiTK//OSyDNIY0ZIWtlHgWMHzB0=\n",
			wantTbl: "widgets",
		},
		{
			name:   "flyway covers a nested file",
			format: "flyway",
			files: map[string]string{
				"V1__init.sql":       "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
				"sub/V2__nested.sql": "CREATE TABLE nested (id INTEGER PRIMARY KEY);\n",
			},
			oracle: "h1:7ad3JauGs7aOZeyXi0cO10IbvTOqjvm2UTagEtkK6b8=\n" +
				"V1__init.sql h1:Lcem1A1NyIMMjubsHiTK//OSyDNIY0ZIWtlHgWMHzB0=\n" +
				"sub/V2__nested.sql h1:HvRyCNahtaW45X197re72+iFeMX4xKRoBtlDXvJUdKc=\n",
			wantTbl: "widgets",
		},
		{
			name:   "flyway drops a baseline-squashed file",
			format: "flyway",
			files: map[string]string{
				"V1__one.sql":  "CREATE TABLE one (id INTEGER PRIMARY KEY);\n",
				"B2__base.sql": "CREATE TABLE base (id INTEGER PRIMARY KEY);\n",
			},
			oracle: "h1:cm1D4+YYSuu/LHpdWCjdLKKdE3XPZOLm6nwzZUCWpVw=\n" +
				"B2__base.sql h1:EEPUhPy0sXNnJdd6+9SiVVRSEF86Has+0A3k27MymL0=\n",
			wantTbl: "base",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			files := map[string]string{"atlas.sum": tt.oracle}
			maps.Copy(files, tt.files)
			dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), files)
			dbPath := filepath.Join(tempDir, "converted.db")

			stdout, stderr, err := compatApplyConverted(dir, tt.format, dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(sqliteTableCount(c, dbPath, tt.wantTbl), qt.Equals, 1)
		})
	}
}

// TestCompatMigrateApply_ConvertedDirUncoveredFileEditedApplies is the negative
// direction, and it is what a gate that hashed every *.sql would fail. Every row
// edits a file Atlas CE's atlas.sum does not cover, after hashing; CE applies
// all three, so refusing any of them would trade one divergence for a worse one.
func TestCompatMigrateApply_ConvertedDirUncoveredFileEditedApplies(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		files   map[string]string
		edit    string
		wantTbl string
	}{
		{
			name:   "golang-migrate down file",
			format: "golang-migrate",
			files: map[string]string{
				"1_init.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
				"1_init.down.sql": "DROP TABLE widgets;\n",
			},
			edit:    "1_init.down.sql",
			wantTbl: "widgets",
		},
		{
			name:   "flyway undo file",
			format: "flyway",
			files: map[string]string{
				"V1__init.sql": "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
				"U1__undo.sql": "DROP TABLE widgets;\n",
			},
			edit:    "U1__undo.sql",
			wantTbl: "widgets",
		},
		{
			name:   "flyway file squashed by a higher baseline",
			format: "flyway",
			files: map[string]string{
				"V1__one.sql":  "CREATE TABLE one (id INTEGER PRIMARY KEY);\n",
				"B2__base.sql": "CREATE TABLE base (id INTEGER PRIMARY KEY);\n",
			},
			edit:    "V1__one.sql",
			wantTbl: "base",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), tt.files)
			hashConvertedApplyDir(c, dir, tt.format)
			appendToFile(c, filepath.Join(dir, tt.edit), "\n-- edited, and invisible to Atlas\n")
			dbPath := filepath.Join(tempDir, "converted.db")

			stdout, stderr, err := compatApplyConverted(dir, tt.format, dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(sqliteTableCount(c, dbPath, tt.wantTbl), qt.Equals, 1)
		})
	}
}

// TestCompatMigrateApply_ConvertedDirEmptyCoveredSetIsNotRefused pins the
// exemption predicate. Atlas CE keys its refusal on the covered set being
// non-empty, NOT on the directory holding any *.sql — measured: an unhashed
// golang-migrate directory holding only a down file, and an unhashed Flyway
// directory holding only an undo file or only a non-prefixed plain.sql, all
// exit 0 with "No migration files to execute", while an unhashed Goose
// directory holding only foo.sql exits 1.
//
// The assertion is one-sided on purpose. ptah-compat still exits 1 on these
// rows, but with its converter's "no importable migration files found" rather
// than a checksum refusal — a pre-existing divergence reached only after this
// gate passes, tracked as stokaro/ptah#980 and pinned by
// TestCompatMigrateApply_ConvertedEmptyDirReportsImportError_KnownDivergence.
// What this test holds is that the gate does not refuse them, which is the part
// a wrong exemption predicate would break.
func TestCompatMigrateApply_ConvertedDirEmptyCoveredSetIsNotRefused(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
	}{
		{
			name:   "golang-migrate with only a down file",
			format: "golang-migrate",
			files:  map[string]string{"1_init.down.sql": "DROP TABLE widgets;\n"},
		},
		{
			name:   "flyway with only an undo file",
			format: "flyway",
			files:  map[string]string{"U1__init.sql": "DROP TABLE widgets;\n"},
		},
		{
			name:   "flyway with only a non-prefixed SQL file",
			format: "flyway",
			files:  map[string]string{"plain.sql": "CREATE TABLE plain (id INTEGER PRIMARY KEY);\n"},
		},
		{
			name:   "goose with a migration only in a subdirectory",
			format: "goose",
			files:  map[string]string{"sub/1_init.sql": "-- +goose Up\nCREATE TABLE nested (id INTEGER PRIMARY KEY);\n"},
		},
		{
			name:   "dbmate with a migration only in a subdirectory",
			format: "dbmate",
			files:  map[string]string{"sub/1_init.sql": "-- migrate:up\nCREATE TABLE nested (id INTEGER PRIMARY KEY);\n"},
		},
		{
			name:   "golang-migrate with a migration only in a subdirectory",
			format: "golang-migrate",
			files:  map[string]string{"sub/1_init.up.sql": "CREATE TABLE nested (id INTEGER PRIMARY KEY);\n"},
		},
		{
			name:   "empty directory",
			format: "goose",
			files:  make(map[string]string),
		},
		{
			name:   "no SQL files",
			format: "goose",
			files:  map[string]string{"README.md": "migrations live here\n", ".gitkeep": ""},
		},
		{
			// The .sql suffix match is case-sensitive on both tools, so an
			// uppercase name leaves the covered set empty rather than making it
			// unverifiable.
			name:   "goose with only an uppercase .SQL file",
			format: "goose",
			files:  map[string]string{"1_INIT.SQL": "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			writeConvertedApplyDir(c, dir, tt.files)
			dbPath := filepath.Join(tempDir, "converted.db")

			stdout, stderr, err := compatApplyConverted(dir, tt.format, dbPath)

			c.Assert(stdout, qt.Not(qt.Contains), "checksum")
			c.Assert(stderr, qt.Not(qt.Contains), "checksum")
			c.Assert(errorText(err), qt.Not(qt.Contains), "checksum")
		})
	}
}

// TestCompatMigrateApply_ConvertedDirHashedEmptyCoveredSetIsNotRefused is the
// other side of the exemption, and it does not go through it: a directory whose
// covered set is empty and which WAS hashed carries the single-line empty-set
// sum `h1:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=` — the digest the pinned
// oracle writes for it — and verifying that must come out clean rather than
// reading as drift. A verifier that treated a sum with no entry lines as
// malformed would refuse a directory Atlas CE applies.
func TestCompatMigrateApply_ConvertedDirHashedEmptyCoveredSetIsNotRefused(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
	}{
		{
			name:   "golang-migrate with only a down file",
			format: "golang-migrate",
			files:  map[string]string{"1_init.down.sql": "DROP TABLE widgets;\n"},
		},
		{
			name:   "flyway with only an undo file",
			format: "flyway",
			files:  map[string]string{"U1__undo.sql": "DROP TABLE widgets;\n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			files := map[string]string{
				"atlas.sum": "h1:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n",
			}
			maps.Copy(files, tt.files)
			dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), files)

			stdout, stderr, err := compatApplyConverted(dir, tt.format, filepath.Join(tempDir, "converted.db"))

			c.Assert(stdout, qt.Not(qt.Contains), "checksum")
			c.Assert(stderr, qt.Not(qt.Contains), "checksum")
			c.Assert(errorText(err), qt.Not(qt.Contains), "checksum")
		})
	}
}

// TestCompatMigrateApply_ConvertedDirHashedEmptyCoveredSetDriftRefuses is the
// SECOND input separating `!hashed && len(names) == 0` from a bare
// `len(names) == 0`, and it is a different mechanism from the first.
//
// The first is the `removed` row of the drift suite: a hashed directory whose
// covered file was deleted, where the entry sequence diverges. This one has no
// entry sequence at all — the covered set is empty on both sides — and diverges
// only on the recorded directory-hash line. A bare `len(names) == 0` returns nil
// for both, so a suite holding only the first would let this one through.
//
// Atlas CE refuses it, measured 2026-08-02.
func TestCompatMigrateApply_ConvertedDirHashedEmptyCoveredSetDriftRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), map[string]string{
		"1_init.down.sql": "DROP TABLE widgets;\n",
		// The empty-set digest is h1:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=;
		// this is a hand-edited directory-hash line over the same empty set.
		"atlas.sum": "h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n",
	})
	dbPath := filepath.Join(tempDir, "converted.db")

	stdout, stderr, err := compatApplyConverted(dir, "golang-migrate", dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum mismatch")
	// No L<line> pointer: there is no entry sequence to point into.
	c.Assert(stdout, qt.Equals, atlasChecksumFileNotFoundStdout)
	c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

// TestCompatMigrateApply_ConvertedFlywaySubdirectoryOnlyRefuses is the row that
// separates "the per-format covered set" from "the top-level per-format covered
// set". Flyway is the one layout whose atlas.sum reaches below the root, so a
// directory whose only migration is sub/V2__nested.sql has a NON-empty covered
// set and Atlas CE refuses it unhashed — the opposite of the same shape read as
// goose, which the test above exempts.
func TestCompatMigrateApply_ConvertedFlywaySubdirectoryOnlyRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), map[string]string{
		"sub/V2__nested.sql": "CREATE TABLE nested (id INTEGER PRIMARY KEY);\n",
	})
	dbPath := filepath.Join(tempDir, "converted.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum file not found")
	c.Assert(stdout, qt.Equals, atlasChecksumFileNotFoundStdout)
	c.Assert(stderr, qt.Equals, atlasChecksumFileNotFoundStderr)
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

// TestCompatMigrateApply_ConvertedFlywayNestedFileTamperRefuses is the other
// half of the recursive rule: a nested file that IS covered must be verified,
// and the mismatch names its slash path.
func TestCompatMigrateApply_ConvertedFlywayNestedFileTamperRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), map[string]string{
		"V1__init.sql":       "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"sub/V2__nested.sql": "CREATE TABLE nested (id INTEGER PRIMARY KEY);\n",
	})
	hashConvertedApplyDir(c, dir, "flyway")
	appendToFile(c, filepath.Join(dir, "sub", "V2__nested.sql"), "\n-- tampered\n")
	dbPath := filepath.Join(tempDir, "converted.db")

	stdout, stderr, err := compatApplyConverted(dir, "flyway", dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum mismatch")
	c.Assert(stdout, qt.Equals, atlasChecksumMismatchStdout(3, "sub/V2__nested.sql", "edited"))
	c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
}

// TestCompatMigrateApply_ConvertedDirGatePrecedesSourceParse pins the ordering
// the restructure exists for. Both rows hold a file Ptah's Goose converter
// cannot read, and both must report the checksum state rather than the parse
// failure, exactly as Atlas CE does.
func TestCompatMigrateApply_ConvertedDirGatePrecedesSourceParse(t *testing.T) {
	t.Run("unhashed and unparseable reports the missing sum", func(t *testing.T) {
		c := qt.New(t)
		tempDir := c.TempDir()
		dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), map[string]string{
			"1_init.sql": "CREATE TABLE nd (id INTEGER PRIMARY KEY);\n",
		})
		dbPath := filepath.Join(tempDir, "converted.db")

		stdout, stderr, err := compatApplyConverted(dir, "goose", dbPath)

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "checksum file not found")
		c.Assert(stdout, qt.Equals, atlasChecksumFileNotFoundStdout)
		c.Assert(stderr, qt.Equals, atlasChecksumFileNotFoundStderr)
	})

	t.Run("tampered until unparseable reports the mismatch", func(t *testing.T) {
		c := qt.New(t)
		tempDir := c.TempDir()
		dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), map[string]string{
			"1_init.sql": "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		})
		hashConvertedApplyDir(c, dir, "goose")
		// Removing the directive is what a conversion-first order would report
		// as `migration file 1_init.sql has no "-- +goose Up" section`.
		writeConvertedApplyDir(c, dir, map[string]string{
			"1_init.sql": "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		})
		dbPath := filepath.Join(tempDir, "converted.db")

		stdout, stderr, err := compatApplyConverted(dir, "goose", dbPath)

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Equals, "checksum mismatch")
		c.Assert(stdout, qt.Equals, atlasChecksumMismatchStdout(2, "1_init.sql", "edited"))
		c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
	})
}

// TestCompatMigrateApply_ConvertedDirGatePrecedesConnection pins the other
// ordering: Atlas CE emits the checksum refusal INSTEAD of the connection error
// when --url is unreachable, which is what proves the gate runs before the
// connection rather than merely appearing to.
func TestCompatMigrateApply_ConvertedDirGatePrecedesConnection(t *testing.T) {
	c := qt.New(t)
	dir := writeConvertedApplyDir(c, filepath.Join(c.TempDir(), "m"), map[string]string{
		"1_init.sql": "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
	})

	stdout, stderr, err := runCompat(
		"migrate", "apply",
		"--url", "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		"--dir", "file://"+dir+"?format=goose",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum file not found")
	c.Assert(stdout, qt.Equals, atlasChecksumFileNotFoundStdout)
	c.Assert(stderr, qt.Equals, atlasChecksumFileNotFoundStderr)
	// The alternative ordering would surface a dial error instead.
	c.Assert(stderr, qt.Not(qt.Contains), testutils.RefusedConnection)
}

func TestCompatMigrateApply_ConvertedDirRefusesDryRun(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), map[string]string{
		"1_init.sql": "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
	})
	dbPath := filepath.Join(tempDir, "converted-dry-run.db")

	_, stderr, err := compatApplyConverted(dir, "goose", dbPath, "--dry-run")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Equals, atlasChecksumFileNotFoundStderr)
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

// TestCompatMigrateApply_ConvertedDirMalformedSumRefuses pins the shape Atlas
// CE gives an atlas.sum it cannot parse: a plain "checksum mismatch" with no
// L<line> pointer, because there is no entry sequence to point into.
func TestCompatMigrateApply_ConvertedDirMalformedSumRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), map[string]string{
		"1_init.sql": "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"atlas.sum":  "not a sum file at all\n",
	})
	dbPath := filepath.Join(tempDir, "converted.db")

	stdout, stderr, err := compatApplyConverted(dir, "goose", dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum mismatch")
	c.Assert(stdout, qt.Equals, atlasChecksumFileNotFoundStdout)
	c.Assert(stderr, qt.Equals, atlasChecksumMismatchStderr)
}

// TestCompatMigrateApply_ConvertedDirMatchesValidateOutput holds the property
// #962 established for native directories and #992 for the validate verb: a
// refusal from apply is byte-identical to `migrate validate` on the same
// directory, because both render through the same migratevalidate helpers.
func TestCompatMigrateApply_ConvertedDirMatchesValidateOutput(t *testing.T) {
	const upBody = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"

	tests := []struct {
		name string
		// hashFirst names the layouts atlas.sum is written for before the edit
		// below, empty for a directory that was never hashed.
		hashFirst []string
		// editAfterHash rewrites files once atlas.sum has been computed, so the
		// sum no longer describes the directory.
		editAfterHash map[string]string
	}{
		{name: "unhashed"},
		{
			name:          "tampered",
			hashFirst:     []string{"golang-migrate"},
			editAfterHash: map[string]string{"1_init.up.sql": upBody + "\n-- tampered\n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := writeConvertedApplyDir(c, filepath.Join(tempDir, "m"), map[string]string{
				"1_init.up.sql":   upBody,
				"1_init.down.sql": "DROP TABLE widgets;\n",
			})
			for _, format := range tt.hashFirst {
				hashConvertedApplyDir(c, dir, format)
			}
			writeConvertedApplyDir(c, dir, tt.editAfterHash)

			applyOut, applyErrOut, applyErr := compatApplyConverted(
				dir, "golang-migrate", filepath.Join(tempDir, "converted.db"))
			validateOut, validateErrOut, validateErr := runCompat(
				"migrate", "validate", "--dir", "file://"+dir+"?format=golang-migrate")

			c.Assert(applyErr, qt.IsNotNil)
			c.Assert(validateErr, qt.IsNotNil)
			c.Assert(applyOut, qt.Equals, validateOut)
			c.Assert(applyErrOut, qt.Equals, validateErrOut)
			c.Assert(applyErr.Error(), qt.Equals, validateErr.Error())
		})
	}
}

// TestCompatMigrateApply_ConvertedDirFromProjectConfigIsGated pins that the
// gate follows the layout wherever it was named, not only the `?format=` query.
// Atlas CE reaches the same refusal through `--env local` with
// `migration { format = goose }` in atlas.hcl, measured on the pinned oracle,
// and a gate keyed on the query alone would silently exempt every project that
// configures its layout that way.
//
// The clean row is TestMigrateApplyExecutesGooseProjectUpSectionOnly, which
// hashes the same directory and applies it.
func TestCompatMigrateApply_ConvertedDirFromProjectConfigIsGated(t *testing.T) {
	const gooseBody = "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"

	tests := []struct {
		name string
		// hashFirst names the layouts atlas.sum is written for before the edit
		// below, empty for a directory that was never hashed.
		hashFirst []string
		// editAfterHash rewrites files under migrations/ once atlas.sum has been
		// computed, so the sum no longer describes the directory.
		editAfterHash map[string]string
		wantErr       string
	}{
		{
			name:    "unhashed",
			wantErr: "Error: checksum file not found",
		},
		{
			name:          "tampered",
			hashFirst:     []string{"goose"},
			editAfterHash: map[string]string{"1_create_widgets.sql": gooseBody + "\n-- tampered\n"},
			wantErr:       "Error: checksum mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			writeAtlasApplyProjectMigration(c, "migrations", "1_create_widgets.sql", gooseBody)
			for _, format := range tt.hashFirst {
				hashConvertedApplyDir(c, "migrations", format)
			}
			writeConvertedApplyDir(c, "migrations", tt.editAfterHash)
			dbPath := filepath.Join(root, "apply.db")
			writeAtlasApplyProjectConfig(c, dbPath, "goose", "LINEAR")

			output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

			c.Assert(err, qt.IsNotNil)
			c.Assert(output, qt.Contains, tt.wantErr)
			_, statErr := os.Stat(dbPath)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
		})
	}
}

// TestCompatMigrateApply_ConvertedEmptyCoveredSetReportsNothingToExecute closes
// stokaro/ptah#980. A converted directory with nothing to execute now exits 0
// with "No migration files to execute" instead of the converter's "no
// importable migration files found", matching the pinned community binary
// v1.3.0 on all six rows below.
//
// The table spans the two axes the issue names — three directory shapes, each
// with and without atlas.sum — because hashing first is what would implicate
// the integrity gate rather than the converter, and it does not: all six
// diverged before the fix and all six agree after it.
//
// This is the half of the split #980 asked for. Its twin, a NON-empty covered
// set whose files the converter produces no entry for, deliberately keeps the
// loud refusal and is pinned by
// TestCompatMigrateApply_ConvertedUnreadableCoveredSetStillRefuses below.
func TestCompatMigrateApply_ConvertedEmptyCoveredSetReportsNothingToExecute(t *testing.T) {
	subdirOnly := map[string]string{
		"sub/1_init.sql": "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
	}
	readmeOnly := map[string]string{"README.md": "notes\n"}

	tests := []struct {
		name  string
		files map[string]string
		// hashFirst is the {no atlas.sum, hashed} axis, carried as per-row data
		// rather than a branch in the body: it names the layouts atlas.sum is
		// written for, and the hashed rows go through the same `migrate hash` a
		// user runs, so they exercise the hash-then-apply round trip the issue
		// measured.
		hashFirst []string
	}{
		{name: "empty directory", files: nil},
		{name: "empty directory hashed", files: nil, hashFirst: []string{"goose"}},
		{name: "README only", files: readmeOnly},
		{name: "README only hashed", files: readmeOnly, hashFirst: []string{"goose"}},
		{name: "SQL in a subdirectory only", files: subdirOnly},
		{name: "SQL in a subdirectory only hashed", files: subdirOnly, hashFirst: []string{"goose"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			writeConvertedApplyDir(c, dir, tt.files)
			for _, format := range tt.hashFirst {
				hashConvertedApplyDir(c, dir, format)
			}
			dbPath := filepath.Join(tempDir, "converted.db")

			stdout, stderr, err := compatApplyConverted(dir, "goose", dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "No migration files to execute")
			// Nothing executed means nothing executed: a subdirectory migration
			// the community binary skips must not have created its table.
			c.Assert(sqliteTableCount(c, dbPath, "widgets"), qt.Equals, 0)
		})
	}
}

// TestCompatMigrateApply_ConvertedUnreadableCoveredSetStillRefuses is the guard
// that keeps #980's relaxation from becoming a silent no-op.
//
// A Goose directory holding only `foo.sql` has a NON-empty covered set — the
// community binary applies it, as version "foo" — while Ptah's converter
// produces no entry for it. Reporting "nothing to execute" here would exit 0
// having skipped a migration the source tool runs, which is worse than the
// refusal it replaced. So the covered set, not the converted entry count, is
// what decides, and this directory keeps exiting 1.
func TestCompatMigrateApply_ConvertedUnreadableCoveredSetStillRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	writeConvertedApplyDir(c, dir, map[string]string{"foo.sql": "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"})
	_, _, hashErr := runCompat("migrate", "hash", "--dir", "file://"+dir+"?format=goose")
	c.Assert(hashErr, qt.IsNil)
	dbPath := filepath.Join(tempDir, "converted.db")

	_, _, err := compatApplyConverted(dir, "goose", dbPath)

	c.Assert(err, qt.ErrorMatches, `atlas migrate apply --dir: no importable migration files found in .* for format "goose"`)
}
