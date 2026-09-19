package main

// White-box testing required: the runner is a program, so it publishes no
// import path and resolvePtah cannot be reached from a black-box test. The
// alternative is asserting on the exit code of a whole page run, which cannot
// tell a refused directory apart from a page that failed for its own reasons.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/exeext"
)

// writePrograms creates an empty file for each named program in a fresh
// directory and returns the path of the first one, which is what a caller
// would pass to --ptah.
func writePrograms(c *qt.C, programs ...string) string {
	c.Helper()
	directory := c.TempDir()
	for _, program := range programs {
		path := filepath.Join(directory, program+exeext.Suffix)
		c.Assert(os.WriteFile(path, []byte("#!/bin/sh\n"), 0o700), qt.IsNil) //nolint:gosec // G306 -- a stand-in for an executable, in a temporary directory
	}
	return filepath.Join(directory, programs[0]+exeext.Suffix)
}

func TestResolvePtah_HappyPath(t *testing.T) {
	t.Run("every published program is beside the one named", func(t *testing.T) {
		c := qt.New(t)
		given := writePrograms(c, publishedPrograms...)

		directory, cleanup, err := resolvePtah(t.Context(), given)

		c.Assert(err, qt.IsNil)
		c.Assert(cleanup, qt.IsNotNil)
		c.Assert(directory, qt.Equals, filepath.Dir(given))
	})
}

func TestResolvePtah_FailurePath(t *testing.T) {
	// One row per program that can be the missing one, because a loop that
	// stopped after the first would pass with the last program unchecked.
	tests := []struct {
		name    string
		present []string
		missing string
	}{
		{name: "only ptah", present: []string{"ptah"}, missing: "ptah-compat"},
		{name: "only ptah-compat", present: []string{"ptah-compat"}, missing: "ptah"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			given := writePrograms(c, test.present...)

			directory, cleanup, err := resolvePtah(t.Context(), given)

			c.Assert(err, qt.ErrorMatches, `(?s)--ptah .*: the pages also run `+
				test.missing+exeext.Suffix+`, which is not in .*`)
			c.Assert(directory, qt.Equals, "")
			c.Assert(cleanup, qt.IsNil)
		})
	}
}
