package atlas_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
)

// These tests pin stokaro/ptah#845 on `ptah-compat migrate new`: an empty
// migration is created in the SELECTED external layout, under both spellings
// that select it, instead of the verb refusing outright.
//
// Before this change every row that selects an external layout printed one of
// two refusals and created nothing:
//
//	atlas migrate new --dir-format: Atlas accepts --dir-format=golang-migrate,
//	  but Ptah does not implement that directory format yet
//	atlas migrate new --dir: Atlas accepts ?format=goose, but Ptah does not
//	  implement that directory format for this command yet
//
// The file names and the exact bytes below were measured against the pinned
// community binary v1.3.0 on 2026-08-06, by running `migrate new addcol --dir
// file://<empty> --dir-format <format>` with it and dumping the result with
// `od -c`. Running the two binaries on the same empty directory produces the
// same file names once the yyyyMMddHHmmss version is normalized, byte-identical
// file contents, and an atlas.sum covering the same entries; the community
// binary then reads a directory this verb wrote — `migrate validate` exits 0 and
// its own `migrate hash` reproduces the sum byte for byte.

// newConvertedLayout is one external layout and everything a row needs to know
// about what `migrate new` writes for it.
type newConvertedLayout struct {
	format string
	// files are the created file names with the version replaced by <V>.
	files []string
	// covered are the created names the layout's atlas.sum covers, same
	// substitution. It is a strict subset for the two layouts that write a
	// rollback half.
	covered []string
	// body is the exact content of every created file, keyed the same way.
	body map[string]string
}

func newConvertedLayouts() []newConvertedLayout {
	return []newConvertedLayout{
		{
			format:  "golang-migrate",
			files:   []string{"<V>_addcol.down.sql", "<V>_addcol.up.sql"},
			covered: []string{"<V>_addcol.up.sql"},
			body:    map[string]string{"<V>_addcol.down.sql": "", "<V>_addcol.up.sql": ""},
		},
		{
			format:  "flyway",
			files:   []string{"U<V>__addcol.sql", "V<V>__addcol.sql"},
			covered: []string{"V<V>__addcol.sql"},
			body:    map[string]string{"U<V>__addcol.sql": "", "V<V>__addcol.sql": ""},
		},
		{
			format:  "goose",
			files:   []string{"<V>_addcol.sql"},
			covered: []string{"<V>_addcol.sql"},
			body:    map[string]string{"<V>_addcol.sql": "-- +goose Up\n\n-- +goose Down\n"},
		},
		{
			format:  "dbmate",
			files:   []string{"<V>_addcol.sql"},
			covered: []string{"<V>_addcol.sql"},
			body:    map[string]string{"<V>_addcol.sql": "-- migrate:up\n\n-- migrate:down\n"},
		},
		{
			format:  "liquibase",
			files:   []string{"<V>_addcol.sql"},
			covered: []string{"<V>_addcol.sql"},
			body:    map[string]string{"<V>_addcol.sql": "--liquibase formatted sql"},
		},
	}
}

var newConvertedVersionRe = regexp.MustCompile(`[0-9]{14}`)

// newConvertedNames returns dir's entries other than atlas.sum, sorted, with the
// yyyyMMddHHmmss version replaced by <V> so a row can name the file it expects
// without knowing the second it ran in.
func newConvertedNames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, newConvertedVersionRe.ReplaceAllString(entry.Name(), "<V>"))
	}
	names = slices.DeleteFunc(names, func(name string) bool { return name == "atlas.sum" })
	slices.Sort(names)
	return names
}

// newConvertedSumEntries returns the file names dir's atlas.sum covers, with the
// version substitution newConvertedNames applies.
func newConvertedSumEntries(c *qt.C, dir string) []string {
	c.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	var covered []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		name, _, found := strings.Cut(line, " ")
		if !found || strings.HasPrefix(line, "h1:") {
			continue
		}
		covered = append(covered, newConvertedVersionRe.ReplaceAllString(name, "<V>"))
	}
	slices.Sort(covered)
	return covered
}

// newConvertedBodies returns the created files' contents, keyed by the
// version-normalized name.
func newConvertedBodies(c *qt.C, dir string, names []string) map[string]string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	bodies := make(map[string]string, len(names))
	for _, entry := range entries {
		normalized := newConvertedVersionRe.ReplaceAllString(entry.Name(), "<V>")
		if normalized == "atlas.sum" {
			continue
		}
		raw, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
		c.Assert(readErr, qt.IsNil)
		bodies[normalized] = string(raw)
	}
	return bodies
}

// assertMigrateNewArtifacts pins every byte the command writes. The expected
// checksum is computed over the exact covered names after the generated
// version is known, so the assertion remains deterministic across clock ticks
// while still catching a wrong file set, order, name, or body.
func assertMigrateNewArtifacts(c *qt.C, dir string, layout newConvertedLayout) {
	c.Helper()
	c.Assert(newConvertedNames(c, dir), qt.DeepEquals, layout.files)
	c.Assert(newConvertedSumEntries(c, dir), qt.DeepEquals, layout.covered)
	c.Assert(newConvertedBodies(c, dir, layout.files), qt.DeepEquals, layout.body)

	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	actualNames := make(map[string]string, len(entries))
	for _, entry := range entries {
		normalized := newConvertedVersionRe.ReplaceAllString(entry.Name(), "<V>")
		actualNames[normalized] = entry.Name()
	}
	covered := make([]string, len(layout.covered))
	for i, normalized := range layout.covered {
		covered[i] = actualNames[normalized]
		c.Assert(covered[i], qt.Not(qt.Equals), "")
	}
	wantSum, err := migratesum.ComputeAtlasFiles(os.DirFS(dir), covered)
	c.Assert(err, qt.IsNil)
	gotSum, err := os.ReadFile(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	c.Assert(gotSum, qt.DeepEquals, wantSum.Bytes())
}

// TestCompatMigrateNewConverted_WritesTheSelectedLayout is the discriminator for
// #845 on `migrate new`: the created file names, their bytes, and the covered
// set of the atlas.sum written beside them all follow the selected layout.
//
// The rows separate the axis rather than merely exercising it. goose, dbmate and
// liquibase share Atlas's own file NAME, so alone they could not tell "the
// format was read" from "the format was ignored"; golang-migrate and flyway
// write a name and a file COUNT the Atlas layout never writes, and the three
// directive bodies differ from each other and from the Atlas layout's empty
// file. The covered-set assertion is the second separator: golang-migrate and
// flyway each create two files and cover exactly one.
//
// Reverted, every row fails at the first assertion with
// `Atlas accepts --dir-format=<format>, but Ptah does not implement that
// directory format yet`, and the directory is empty.
func TestCompatMigrateNewConverted_WritesTheSelectedLayout(t *testing.T) {
	t.Parallel()
	for _, layout := range newConvertedLayouts() {
		t.Run(layout.format, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := c.TempDir()

			stdout, stderr, err := runCompatExit("migrate", "new", "addcol",
				"--dir", "file://"+dir, "--dir-format", layout.format)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
			assertMigrateNewArtifacts(c, dir, layout)
		})
	}
}

// TestCompatMigrateNewConverted_QuerySpellingWritesTheSelectedLayout is the same
// discriminator through the `?format=` spelling on --dir, which the community
// binary accepts interchangeably with --dir-format.
//
// It is the row removed from TestCompatMigrateDirQuery_FailurePathForeignFormat
// when this landed, inverted: that test asserted `migrate new` refuses the
// query, and the reason it gave — no covered-set computation in front of a write
// — is what #845 supplied.
//
// Reverted, every row prints `Atlas accepts ?format=<format>, but Ptah does not
// implement that directory format for this command yet` and creates nothing.
func TestCompatMigrateNewConverted_QuerySpellingWritesTheSelectedLayout(t *testing.T) {
	t.Parallel()
	for _, layout := range newConvertedLayouts() {
		t.Run(layout.format, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := c.TempDir()

			stdout, stderr, err := runCompatExit("migrate", "new", "addcol",
				"--dir", "file://"+dir+"?format="+layout.format)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
			assertMigrateNewArtifacts(c, dir, layout)
		})
	}
}

// TestCompatMigrateNewConverted_QueryFormatOutranksDirFormatFlag pins the
// precedence between the two spellings on the verb that now WRITES with it.
//
// Measured against the pinned community binary v1.3.0 on 2026-08-06, both ways
// round: `--dir 'file://d?format=flyway' --dir-format goose` writes the Flyway
// pair, and `--dir 'file://d?format=atlas' --dir-format golang-migrate` writes
// the single Atlas file. It is the resolver `migrate apply` and the reading
// verbs share, so the rows exist to catch a writing verb resolving the format a
// second way — which is how the directory that gets gated stops being the
// directory that gets written.
//
// Reverted, both rows print `Ptah does not implement that directory format`.
func TestCompatMigrateNewConverted_QueryFormatOutranksDirFormatFlag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		query     string
		dirFormat string
		want      []string
	}{
		{
			name:      "query_selects_flyway_over_goose",
			query:     "?format=flyway",
			dirFormat: "goose",
			want:      []string{"U<V>__addcol.sql", "V<V>__addcol.sql"},
		},
		{
			name:      "query_selects_atlas_over_golang_migrate",
			query:     "?format=atlas",
			dirFormat: "golang-migrate",
			want:      []string{"<V>_addcol.sql"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := c.TempDir()

			stdout, stderr, err := runCompatExit("migrate", "new", "addcol",
				"--dir", "file://"+dir+tt.query, "--dir-format", tt.dirFormat)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
			c.Assert(newConvertedNames(c, dir), qt.DeepEquals, tt.want)
		})
	}
}

// TestCompatMigrateNewConverted_CreatedDirectoryVerifies is the end-to-end
// property the byte assertions above exist to serve: a directory this verb just
// wrote into is one the same binary's `migrate validate` accepts and
// `migrate apply` runs, for the layout it was written in.
//
// It runs on a POPULATED, already-hashed directory rather than an empty one,
// because that is where a wrong covered set shows: writing the new migration
// without rehashing, or rehashing over the Atlas file set instead of the
// layout's, leaves the pre-existing migrations either uncovered or mismatched.
//
// Reverted, `migrate new` fails first with `Ptah does not implement that
// directory format yet` and neither follow-up runs.
func TestCompatMigrateNewConverted_CreatedDirectoryVerifies(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// seed is the layout's own spelling of one existing migration, written
		// before the directory is hashed.
		seed []newConvertedFile
		// count is what `migrate status` must report as PENDING afterwards: the
		// seeded migration plus the one this verb just created. It is read off
		// the Atlas-mirrored report shape that #1102 landed on this verb,
		// column alignment included, rather than the native block that preceded
		// it.
		count string
	}{
		{
			name: "golang-migrate",
			seed: []newConvertedFile{
				{name: "1_init.up.sql", sql: "CREATE TABLE n1 (id INTEGER PRIMARY KEY);\n"},
				{name: "1_init.down.sql", sql: "DROP TABLE n1;\n"},
			},
			count: "2",
		},
		{
			name: "flyway",
			seed: []newConvertedFile{
				{name: "V1__init.sql", sql: "CREATE TABLE n2 (id INTEGER PRIMARY KEY);\n"},
			},
			count: "2",
		},
		{
			name: "goose",
			seed: []newConvertedFile{
				{name: "1_init.sql", sql: "-- +goose Up\nCREATE TABLE n3 (id INTEGER PRIMARY KEY);\n-- +goose Down\nDROP TABLE n3;\n"},
			},
			count: "2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := c.TempDir()
			for _, file := range tt.seed {
				writeAtlasApplyProjectMigration(c, dir, file.name, file.sql)
			}
			_, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir, "--dir-format", tt.name)
			c.Assert(err, qt.IsNil, qt.Commentf("hash stderr: %s", stderr))

			_, stderr, err = runCompatExit("migrate", "new", "addcol",
				"--dir", "file://"+dir, "--dir-format", tt.name)
			c.Assert(err, qt.IsNil, qt.Commentf("new stderr: %s", stderr))

			_, stderr, err = runCompatExit("migrate", "validate", "--dir", "file://"+dir, "--dir-format", tt.name)
			c.Assert(err, qt.IsNil, qt.Commentf("validate stderr: %s", stderr))

			stdout, stderr, err := runCompatExit("migrate", "status",
				"--dir", "file://"+dir, "--dir-format", tt.name,
				"--url", "sqlite://"+filepath.Join(c.TempDir(), "status.db"))
			c.Assert(err, qt.IsNil, qt.Commentf("status stderr: %s", stderr))
			c.Assert(stdout, qt.Contains, "  -- Pending Files:   "+tt.count+"\n")
		})
	}
}

// TestCompatMigrateNewConverted_UnhashedDirRefusedBeforeWriting pins the
// ordering stokaro/ptah#1086 established, now over the covered set of the
// SELECTED layout: a directory carrying no atlas.sum at all is refused, and
// nothing is created.
//
// Reverted, this still exits 1 — but with `Ptah does not implement that
// directory format yet` instead of the checksum refusal, which is the reason
// the assertion names the checksum text rather than only the exit code.
func TestCompatMigrateNewConverted_UnhashedDirRefusedBeforeWriting(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := c.TempDir()
	writeGolangMigrateInitPair(c, dir)
	before := newConvertedNames(c, dir)

	_, _, err := runCompatExit("migrate", "new", "addcol",
		"--dir", "file://"+dir, "--dir-format", "golang-migrate")

	c.Assert(err, qt.ErrorMatches, "checksum file not found")
	c.Assert(newConvertedNames(c, dir), qt.DeepEquals, before)
}

// TestCompatMigrateNewConverted_DriftedDirRefusedBeforeWriting is the same
// ordering reached through a sum that exists and no longer holds, which is the
// half that makes the layout specific: a golang-migrate pair hashed for the
// golang-migrate layout has an atlas.sum covering only the up file, so editing
// that up file puts the mismatch inside the golang-migrate covered set.
//
// Hashing is what separates this from the unhashed refusal above, so it is
// spelled out here rather than selected by a row.
func TestCompatMigrateNewConverted_DriftedDirRefusedBeforeWriting(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := c.TempDir()
	writeGolangMigrateInitPair(c, dir)
	_, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir, "--dir-format", "golang-migrate")
	c.Assert(err, qt.IsNil, qt.Commentf("hash stderr: %s", stderr))
	writeAtlasApplyProjectMigration(c, dir, "1_init.up.sql", "CREATE TABLE drift (id INTEGER PRIMARY KEY);\n")
	before := newConvertedNames(c, dir)

	_, _, err = runCompatExit("migrate", "new", "addcol",
		"--dir", "file://"+dir, "--dir-format", "golang-migrate")

	c.Assert(err, qt.ErrorMatches, "checksum mismatch")
	c.Assert(newConvertedNames(c, dir), qt.DeepEquals, before)
}

// writeGolangMigrateInitPair writes the up/down pair both refusals start from.
func writeGolangMigrateInitPair(c *qt.C, dir string) {
	c.Helper()
	writeAtlasApplyProjectMigration(c, dir, "1_init.up.sql", "CREATE TABLE g1 (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c, dir, "1_init.down.sql", "DROP TABLE g1;\n")
}

// newConvertedFile is one migration file a fixture writes, in the layout's own
// spelling.
type newConvertedFile struct {
	name string
	sql  string
}

// TestCompatMigrateNewConverted_RefusesWhatItCannotReadBack pins the two
// refusals this path draws NARROWER than the community binary, so neither can
// be widened by accident into a directory the binary cannot read.
//
// Both are measured. `migrate new --dir-format golang-migrate` with no name
// exits 0 there and writes `20260806071553.up.sql`; applying exactly that file
// exits 1 here with `no importable migration files found` on golang-migrate,
// goose, liquibase and dbmate, and on flyway records version
// 5438407949371077319 where the community binary records 20260806071553. A name
// holding a separator exits 1 on BOTH binaries — there with `open
// .../20260806071901_add/col.up.sql: no such file or directory`, here before any
// path is resolved.
//
// Reverted, both rows still exit 1, with `Ptah does not implement that directory
// format yet` — which is why each row asserts the diagnostic and the empty
// directory rather than the exit code alone.
func TestCompatMigrateNewConverted_RefusesWhatItCannotReadBack(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "no_name",
			args:    []string{"migrate", "new"},
			wantErr: `atlas migrate new: migration name is required`,
		},
		{
			name:    "name_with_separator",
			args:    []string{"migrate", "new", "add/col"},
			wantErr: `atlas migrate new: migration name must be a single file name element.*`,
		},
		{
			name:    "edit_flag",
			args:    []string{"migrate", "new", "addcol", "--edit"},
			wantErr: `atlas migrate new --edit: --edit applies only to an atlas directory, but this directory is read as goose`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := c.TempDir()

			args := append(slices.Clone(tt.args), "--dir", "file://"+dir, "--dir-format", "goose")
			_, _, err := runCompatExit(args...)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(newConvertedNames(c, dir), qt.HasLen, 0)
		})
	}
}

// TestCompatMigrateNew_NativeAtlasLayoutWritesSameArtifactsSilently is the
// native-layout half of stokaro/ptah#1235 findings 3.1 and 3.2. The migration
// and atlas.sum stay unchanged while the compatibility-only success report is
// removed.
//
// The atlas layout, and the two spellings that select it, must still take the
// forwarding branch and produce the single empty `<V>_<name>.sql` plus an
// atlas.sum covering it. A dispatcher that sent the atlas layout down the
// converted path would fail here, because
// [atlasmigrateimport.SkeletonFiles] refuses that format outright.
func TestCompatMigrateNew_NativeAtlasLayoutWritesSameArtifactsSilently(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args func(dir string) []string
	}{
		{
			name: "dir_format_flag",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir, "--dir-format", "atlas"}
			},
		},
		{
			name: "dir_query",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir + "?format=atlas"}
			},
		},
		{
			name: "no_selection",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := c.TempDir()

			args := append([]string{"migrate", "new", "addcol"}, tt.args(dir)...)
			stdout, stderr, err := runCompatExit(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
			assertMigrateNewArtifacts(c, dir, newConvertedLayout{
				format:  "atlas",
				files:   []string{"<V>_addcol.sql"},
				covered: []string{"<V>_addcol.sql"},
				body:    map[string]string{"<V>_addcol.sql": ""},
			})
		})
	}
}
