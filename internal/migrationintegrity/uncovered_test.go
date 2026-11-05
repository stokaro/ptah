package migrationintegrity_test

import (
	"bytes"
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migratesum"
	"ptah.run/internal/migrationintegrity"
	"ptah.run/internal/rundiag"
	"ptah.run/migration/migrationfile"
)

// uncoveredWarning is the one warning a run writes for the two files the
// fixture holds outside atlas.sum.
const uncoveredWarning = "warning: 2 SQL files are not covered by atlas.sum and will not run;" +
	" Atlas migrations are top-level files named *.sql; PTAH_LOG_LEVEL=debug lists them\n"

// unhashedAtlasDirectory is a migration directory with one top-level
// migration, one in a subdirectory and one spelled `.SQL`.
func unhashedAtlasDirectory() fstest.MapFS {
	return fstest.MapFS{
		"1_a.sql":     {Data: []byte("CREATE TABLE a (id integer);\n")},
		"sub/2_b.sql": {Data: []byte("CREATE TABLE b (id integer);\n")},
		"3_c.SQL":     {Data: []byte("CREATE TABLE c (id integer);\n")},
	}
}

// atlasDirectory is [unhashedAtlasDirectory] hashed as Atlas hashes it:
// atlas.sum covers 1_a.sql alone.
func atlasDirectory(c *qt.C) fstest.MapFS {
	c.Helper()
	return hashedAtlas(c, unhashedAtlasDirectory())
}

// hashedAtlas writes the Atlas sum of fsys into it.
func hashedAtlas(c *qt.C, fsys fstest.MapFS) fstest.MapFS {
	c.Helper()
	sum, err := migratesum.ComputeWithFormat(fsys, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes()}
	return fsys
}

// gateInRun runs the gate over fsys in format within a run, and returns what
// it wrote when the run ended and what a run of the directory would execute.
func gateInRun(c *qt.C, fsys fstest.MapFS, format migrationfile.DirFormat) (warned string, executed []string) {
	c.Helper()
	report := rundiag.New(rundiag.Options{})
	var notice bytes.Buffer

	_, err := migrationintegrity.GateWithPolicy(rundiag.With(c.Context(), report), &notice, fsys, format,
		migrationintegrity.Policy{}, migrationintegrity.Options{})
	c.Assert(err, qt.IsNil)
	report.Flush()

	files, err := migrationfile.Discover(fsys, format)
	c.Assert(err, qt.IsNil)
	for _, file := range files {
		executed = append(executed, file.Path)
	}
	return notice.String(), executed
}

// A directory atlas.sum governs runs the files atlas.sum covers and no
// others, and the gate says which it leaves out, once for the run.
func TestGateWithPolicy_ReportsTheFilesARunLeavesOut(t *testing.T) {
	tests := []struct {
		name   string
		format migrationfile.DirFormat
	}{
		{name: "the Atlas format", format: migrationfile.DirFormatAtlas},
		{name: "the automatic format, with atlas.sum", format: migrationfile.DirFormatAuto},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			warned, executed := gateInRun(c, atlasDirectory(c), test.format)

			c.Assert(executed, qt.DeepEquals, []string{"1_a.sql"})
			c.Assert(warned, qt.Equals, uncoveredWarning)
		})
	}
}

// The gate lets an unhashed directory through, and in the Atlas format a run
// of it still executes the top-level `.sql` files alone, so it says the same.
func TestGateWithPolicy_ReportsTheFilesAnUnhashedRunLeavesOut(t *testing.T) {
	c := qt.New(t)

	warned, executed := gateInRun(c, unhashedAtlasDirectory(), migrationfile.DirFormatAtlas)

	c.Assert(executed, qt.DeepEquals, []string{"1_a.sql"})
	c.Assert(warned, qt.Equals, uncoveredWarning)
}

// A directory atlas.sum does not govern runs its nested files, so nothing is
// left out and the gate says nothing. The executed set is asserted beside the
// silence, so the two rows above and these two are one rule: a warning exactly
// where a run executes less than the directory holds.
func TestGateWithPolicy_SaysNothingWhereARunLeavesNothingOut(t *testing.T) {
	for _, format := range []migrationfile.DirFormat{migrationfile.DirFormatPtah, migrationfile.DirFormatAuto} {
		t.Run(string(format), func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{
				"0000000001_a.up.sql":       {Data: []byte("CREATE TABLE a (id integer);\n")},
				"0000000001_a.down.sql":     {Data: []byte("DROP TABLE a;\n")},
				"sub/0000000002_b.up.sql":   {Data: []byte("CREATE TABLE b (id integer);\n")},
				"sub/0000000002_b.down.sql": {Data: []byte("DROP TABLE b;\n")},
			}

			warned, executed := gateInRun(c, fsys, format)

			c.Assert(executed, qt.HasLen, 4)
			c.Assert(warned, qt.Equals, "")
		})
	}
}

// One file left out is its own line.
func TestGateWithPolicy_NamesOneFileItLeavesOut(t *testing.T) {
	c := qt.New(t)
	fsys := unhashedAtlasDirectory()
	delete(fsys, "3_c.SQL")
	fsys = hashedAtlas(c, fsys)

	warned, _ := gateInRun(c, fsys, migrationfile.DirFormatAtlas)

	c.Assert(warned, qt.Equals, "warning: sub/2_b.sql is not covered by atlas.sum and will not run;"+
		" Atlas migrations are top-level files named *.sql\n")
}

// A directory the gate refuses raises nothing: the refusal already says the
// directory does not run.
func TestGateWithPolicy_RaisesNothingForARefusedDirectory(t *testing.T) {
	c := qt.New(t)
	fsys := atlasDirectory(c)
	fsys["1_a.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE tampered (id integer);\n")}
	report := rundiag.New(rundiag.Options{})
	var notice bytes.Buffer

	verified, err := migrationintegrity.GateWithPolicy(rundiag.With(c.Context(), report), &notice, fsys,
		migrationfile.DirFormatAtlas, migrationintegrity.Policy{}, migrationintegrity.Options{})
	report.Flush()

	c.Assert(err, qt.ErrorMatches, `(?s)migration sum verification failed:.*`)
	c.Assert(verified, qt.Equals, "")
	c.Assert(notice.String(), qt.Equals, "")
}

// Outside a run each file is its own line at once.
func TestReportUncovered_WritesEachFileWithoutARun(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	migrationintegrity.ReportUncovered(context.Background(), &out, atlasDirectory(c))

	c.Assert(out.String(), qt.Equals,
		"warning: 3_c.SQL is not covered by atlas.sum and will not run; Atlas migrations are top-level files named *.sql\n"+
			"warning: sub/2_b.sql is not covered by atlas.sum and will not run; Atlas migrations are top-level files named *.sql\n")
}
