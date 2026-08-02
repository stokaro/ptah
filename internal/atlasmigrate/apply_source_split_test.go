package atlasmigrate_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasmigrateimport"
	"github.com/stokaro/ptah/internal/migratesum"
)

// The apply command captures, gates, then converts (stokaro/ptah#973). These
// tests hold the two properties that split buys, both of which
// ResolveApplySourceForFormat alone cannot provide:
//
//  1. the captured source carries atlas.sum, so there is something to verify;
//  2. conversion consumes the captured bytes, so a failure to parse the source
//     layout is reachable only after the gate has seen those same bytes.

func writeSplitFixture(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	for name, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

func snapshotPaths(c *qt.C, fsys fs.FS) []string {
	c.Helper()
	var names []string
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		c.Assert(err, qt.IsNil)
		if !entry.IsDir() {
			names = append(names, name)
		}
		return nil
	})
	c.Assert(err, qt.IsNil)
	slices.Sort(names)
	return names
}

// TestCaptureApplySourceKeepsTheIntegrityFile pins the capture change the gate
// depends on. Before it, atlas.sum was filtered out by extension and the source
// snapshot had nothing to verify against.
func TestCaptureApplySourceKeepsTheIntegrityFile(t *testing.T) {
	formats := []atlasmigrateimport.Format{
		atlasmigrateimport.FormatGoose,
		atlasmigrateimport.FormatDBMate,
		atlasmigrateimport.FormatLiquibase,
		atlasmigrateimport.FormatFlyway,
		atlasmigrateimport.FormatGolangMigrate,
		atlasmigrateimport.FormatAtlas,
	}

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			c := qt.New(t)
			dir := writeSplitFixture(c, map[string]string{
				"1_init.sql":             "-- +goose Up\nCREATE TABLE seam (id int);\n",
				migratesum.AtlasFileName: "h1:whatever=\n",
			})

			captured, err := atlasmigrate.CaptureApplySource(os.DirFS(dir), format)

			c.Assert(err, qt.IsNil)
			c.Assert(snapshotPaths(c, captured), qt.Contains, migratesum.AtlasFileName)
		})
	}
}

// TestCaptureApplySourceFlywayReachesNestedFiles is the row that separates a
// top-level capture from one wide enough to verify. Atlas CE's Flyway integrity
// file covers sub/V2__nested.sql, so a capture that stopped at the root would
// make the verifier fail to read a file the oracle hashed — the false-refusal
// failure mode this whole change exists to avoid.
//
// The same fixture read as goose must NOT pull the nested file in, because no
// other layout's covered set reaches below the root and capturing more would
// change what a directory means without any oracle saying so.
func TestCaptureApplySourceFlywayReachesNestedFiles(t *testing.T) {
	tests := []struct {
		name    string
		format  atlasmigrateimport.Format
		checker qt.Checker
	}{
		{name: "flyway captures the nested file", format: atlasmigrateimport.FormatFlyway, checker: qt.Contains},
		{name: "goose does not", format: atlasmigrateimport.FormatGoose, checker: qt.Not(qt.Contains)},
		{name: "dbmate does not", format: atlasmigrateimport.FormatDBMate, checker: qt.Not(qt.Contains)},
		{name: "golang-migrate does not", format: atlasmigrateimport.FormatGolangMigrate, checker: qt.Not(qt.Contains)},
		{name: "liquibase does not", format: atlasmigrateimport.FormatLiquibase, checker: qt.Not(qt.Contains)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSplitFixture(c, map[string]string{
				"V1__init.sql":       "CREATE TABLE seam (id int);\n",
				"1_init.up.sql":      "CREATE TABLE seam2 (id int);\n",
				"sub/V2__nested.sql": "CREATE TABLE nested (id int);\n",
			})

			captured, err := atlasmigrate.CaptureApplySource(os.DirFS(dir), tt.format)

			c.Assert(err, qt.IsNil)
			c.Assert(snapshotPaths(c, captured), tt.checker, "sub/V2__nested.sql")
		})
	}
}

// TestCaptureApplySourceFlywaySkipsHiddenDirectories keeps the capture aligned
// with the selection: SumFileNames prunes hidden directories, so covering
// .archive/V1__old.sql would put bytes in the snapshot that nothing hashes and
// invite a future reader to hash them.
func TestCaptureApplySourceFlywaySkipsHiddenDirectories(t *testing.T) {
	c := qt.New(t)
	dir := writeSplitFixture(c, map[string]string{
		"V1__init.sql":         "CREATE TABLE seam (id int);\n",
		".archive/V1__old.sql": "CREATE TABLE old (id int);\n",
	})

	captured, err := atlasmigrate.CaptureApplySource(os.DirFS(dir), atlasmigrateimport.FormatFlyway)

	c.Assert(err, qt.IsNil)
	c.Assert(snapshotPaths(c, captured), qt.Not(qt.Contains), ".archive/V1__old.sql")
}

// TestCaptureApplySourceCoversEverySumFileName is the invariant that keeps the
// two rules from drifting: whatever SumFileNames selects for a format, the
// capture for that format must contain, or the verifier cannot read it.
func TestCaptureApplySourceCoversEverySumFileName(t *testing.T) {
	formats := []atlasmigrateimport.Format{
		atlasmigrateimport.FormatGoose,
		atlasmigrateimport.FormatDBMate,
		atlasmigrateimport.FormatLiquibase,
		atlasmigrateimport.FormatFlyway,
		atlasmigrateimport.FormatGolangMigrate,
	}
	// One directory every layout reads differently, including a nested Flyway
	// migration, an undo file, a baseline, a golang-migrate pair and a hidden
	// directory.
	files := map[string]string{
		"1_init.sql":          "-- +goose Up\nCREATE TABLE a (id int);\n",
		"2_more.up.sql":       "CREATE TABLE b (id int);\n",
		"2_more.down.sql":     "DROP TABLE b;\n",
		"V1__x.sql":           "CREATE TABLE v (id int);\n",
		"B0__base.sql":        "CREATE TABLE base (id int);\n",
		"U1__undo.sql":        "DROP TABLE v;\n",
		"R__view.sql":         "CREATE VIEW r AS SELECT 1;\n",
		"sub/V2__nested.sql":  "CREATE TABLE nested (id int);\n",
		".hidden/V9__old.sql": "CREATE TABLE old (id int);\n",
		"notes.txt":           "not sql\n",
	}

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			c := qt.New(t)
			dir := writeSplitFixture(c, files)

			captured, err := atlasmigrate.CaptureApplySource(os.DirFS(dir), format)
			c.Assert(err, qt.IsNil)

			// The names are selected from the LIVE directory, so the assertion
			// is about the capture rather than about the snapshot agreeing with
			// itself.
			names, err := atlasmigrateimport.SumFileNames(os.DirFS(dir), format)
			c.Assert(err, qt.IsNil)
			c.Assert(len(names) > 0, qt.IsTrue)
			for _, name := range names {
				_, err := fs.ReadFile(captured, name)
				c.Assert(err, qt.IsNil, qt.Commentf("covered file %q is missing from the capture", name))
			}
		})
	}
}

// TestConvertApplySourceReadsOnlyTheCapture pins that conversion consumes the
// captured bytes rather than re-reading the directory, so a file that appears
// after the gate has run cannot be executed by it.
func TestConvertApplySourceReadsOnlyTheCapture(t *testing.T) {
	c := qt.New(t)
	dir := writeSplitFixture(c, map[string]string{
		"1_init.sql": "-- +goose Up\nCREATE TABLE seam (id int);\n",
	})

	captured, err := atlasmigrate.CaptureApplySource(os.DirFS(dir), atlasmigrateimport.FormatGoose)
	c.Assert(err, qt.IsNil)

	// Added after the capture, and after any gate would have run.
	c.Assert(os.WriteFile(filepath.Join(dir, "2_late.sql"),
		[]byte("-- +goose Up\nCREATE TABLE late (id int);\n"), 0o600), qt.IsNil)

	converted, err := atlasmigrate.ConvertApplySource(captured, dir, atlasmigrateimport.FormatGoose)

	c.Assert(err, qt.IsNil)
	c.Assert(snapshotPaths(c, converted), qt.DeepEquals, []string{"1_init.sql"})
}

// TestConvertApplySourceReportsSourceParseFailures pins that the parse failure
// belongs to the conversion half, not the capture half. It is what makes the
// gate's ordering observable: capture succeeds on a directory whose layout
// cannot be read, so the gate gets to speak first.
func TestConvertApplySourceReportsSourceParseFailures(t *testing.T) {
	c := qt.New(t)
	dir := writeSplitFixture(c, map[string]string{
		"1_init.sql": "CREATE TABLE seam (id int);\n",
	})

	captured, err := atlasmigrate.CaptureApplySource(os.DirFS(dir), atlasmigrateimport.FormatGoose)
	c.Assert(err, qt.IsNil, qt.Commentf("capture must not parse the source layout"))

	_, err = atlasmigrate.ConvertApplySource(captured, dir, atlasmigrateimport.FormatGoose)

	c.Assert(err, qt.ErrorMatches, `migration file 1_init\.sql has no "-- \+goose Up" section`)
}

// TestResolveApplySourceForFormatStillComposesTheHalves keeps the composed
// entry point honest: `migrate validate --dev-url` still uses it, so it must
// return exactly what capture-then-convert returns.
func TestResolveApplySourceForFormatStillComposesTheHalves(t *testing.T) {
	c := qt.New(t)
	dir := writeSplitFixture(c, map[string]string{
		"1_init.sql": "-- +goose Up\nCREATE TABLE seam (id int);\n",
	})

	composed, err := atlasmigrate.ResolveApplySourceForFormat(os.DirFS(dir), dir, atlasmigrateimport.FormatGoose)
	c.Assert(err, qt.IsNil)

	captured, err := atlasmigrate.CaptureApplySource(os.DirFS(dir), atlasmigrateimport.FormatGoose)
	c.Assert(err, qt.IsNil)
	split, err := atlasmigrate.ConvertApplySource(captured, dir, atlasmigrateimport.FormatGoose)
	c.Assert(err, qt.IsNil)

	c.Assert(composed.Equal(split), qt.IsTrue)
}

func TestCaptureApplySourceRejectsNilFilesystem(t *testing.T) {
	c := qt.New(t)

	_, err := atlasmigrate.CaptureApplySource(nil, atlasmigrateimport.FormatGoose)

	c.Assert(err, qt.ErrorMatches, "migration directory filesystem is required")
}
