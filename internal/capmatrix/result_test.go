package capmatrix_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/capmatrix"
)

// TestWriteAndReadResult_HappyPath checks the file the pipeline passes between
// two jobs survives the trip whole: the reporting job classifies from these
// fields and nothing else.
func TestWriteAndReadResult_HappyPath(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(c.TempDir(), "nested", "postgres-17.json")
	written := capmatrix.CellResult{
		Cell: "postgres-17", Tier: 3, Dialect: "postgres", Line: "17", Image: "postgres:17",
		Probe: capmatrix.ProbeOutcome{
			OK: true, Banner: "PostgreSQL 17.10", Version: "17.10", MatchedCell: "postgres-17",
			Rows: 25, Agrees: 25, Decided: 25, Floor: 25,
		},
		Suite: &capmatrix.SuiteOutcome{OK: true, Total: 40, Passed: 40},
	}

	c.Assert(capmatrix.WriteResult(path, written), qt.IsNil)
	read, err := capmatrix.ReadResult(path)
	c.Assert(err, qt.IsNil)
	c.Assert(read, qt.DeepEquals, written)
}

// TestReadResults_HappyPath reads the shape the reporting job actually
// receives: one artifact per cell, each landing in its own subdirectory.
func TestReadResults_HappyPath(t *testing.T) {
	c := qt.New(t)

	dir := c.TempDir()
	c.Assert(capmatrix.WriteResult(filepath.Join(dir, "cell-postgres-17", "r.json"),
		capmatrix.CellResult{Cell: "postgres-17", Probe: capmatrix.ProbeOutcome{OK: true}}), qt.IsNil)
	c.Assert(capmatrix.WriteResult(filepath.Join(dir, "cell-mariadb-11-4", "r.json"),
		capmatrix.CellResult{Cell: "mariadb-11-4", Probe: capmatrix.ProbeOutcome{OK: true}}), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "probe.log"), []byte("not a result"), 0o600), qt.IsNil)

	results, err := capmatrix.ReadResults(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 2)
	c.Assert(results[0].Cell, qt.Equals, "mariadb-11-4")
	c.Assert(results[1].Cell, qt.Equals, "postgres-17")
}

// TestReadResults_FailurePath covers the ways a results directory lies.
//
// Each row carries the files on disk, because that is the only thing that
// varies between them. Writing them from a closure per row spelled the same
// two calls out four times and put a checker in a table row, where the shape of
// the fixture stops being visible at a glance.
func TestReadResults_FailurePath(t *testing.T) {
	c := qt.New(t)

	// Encoded rather than spelled out, so the fixture for the duplicate-cell
	// row cannot drift from CellResult's own JSON shape.
	body, err := json.MarshalIndent(
		capmatrix.CellResult{Cell: "postgres-17", Probe: capmatrix.ProbeOutcome{OK: true}},
		"", "  ",
	)
	c.Assert(err, qt.IsNil)
	oneResult := string(body) + "\n"

	for _, tc := range []struct {
		name string
		// files maps a slash-separated path under the results directory to its
		// content. An empty map is a directory with nothing in it.
		files map[string]string
		// readSubdir is read instead of the results directory itself, empty for
		// the ordinary case.
		readSubdir string
		expect     string
	}{{
		name:   "two results for one cell",
		files:  map[string]string{"a/r.json": oneResult, "b/r.json": oneResult},
		expect: "(?s).*cell postgres-17 has two results.*",
	}, {
		name:   "a result that names no cell",
		files:  map[string]string{"r.json": `{"tier":2}`},
		expect: "(?s).*names no cell.*",
	}, {
		name:   "a result that is not JSON",
		files:  map[string]string{"r.json": "cell: postgres-17"},
		expect: "(?s).*decode .*r.json.*",
	}, {
		name:       "a results directory that does not exist",
		readSubdir: "never-created",
		expect:     "(?s).*read the results under .*",
	}} {
		c.Run(tc.name, func(c *qt.C) {
			dir := filepath.Join(c.TempDir(), "results")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			for name, content := range tc.files {
				path := filepath.Join(dir, filepath.FromSlash(name))
				c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
				c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
			}

			_, err := capmatrix.ReadResults(filepath.Join(dir, tc.readSubdir))

			c.Assert(err, qt.ErrorMatches, tc.expect)
		})
	}
}
