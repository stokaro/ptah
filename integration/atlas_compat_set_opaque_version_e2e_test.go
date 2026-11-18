//go:build integration

package integration_test

import (
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
)

// setOpaqueVersionFiles is a directory whose last version is not a number.
var setOpaqueVersionFiles = []string{"1_init.sql", "2_more.sql", "R__view.sql"}

// setOpaqueDescription matches the description `migrate set` prints after a
// version, which the comparison below leaves out: it is the revision
// description, held to the community binary by
// TestCompatMigrateRevisionDescriptionMatchesTheCommunityBinaryE2E.
var setOpaqueDescription = regexp.MustCompile(` \([^)\n]*\)\n`)

// prepareSetOpaqueVersion applies the first version of the directory, so a
// `migrate set` has something to move from.
func prepareSetOpaqueVersion(c *qt.C, dir string, run versionOrderRun) {
	c.Helper()
	writeVersionOrderFiles(c, dir, run, setOpaqueVersionFiles)
	applied := run(append([]string{"migrate", "apply", "1"}, versionOrderTarget...)...)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
}

// TestCompatMigrateSetOpaqueVersion_HappyPath sets a native Atlas directory to
// its version R, which the pinned community binary v1.3.0 accepts and which
// marks 2_more.sql and R__view.sql applied.
func TestCompatMigrateSetOpaqueVersion_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	prepareSetOpaqueVersion(c, dir, run)

	got := run(append([]string{"migrate", "set", "R"}, versionOrderTarget...)...)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(setOpaqueDescription.ReplaceAllString(got.Stdout, "\n"), qt.Equals,
		"Current version is R (2 set):\n\n  + 2\n  + R\n\n")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "2", "R"})
}

// TestCompatMigrateSetOpaqueVersion_FailurePath refuses an operand that names
// no file in the binary's words, and changes nothing.
func TestCompatMigrateSetOpaqueVersion_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		operand string
	}{
		{name: "a word", operand: "nope"},
		{name: "an R version no file has", operand: "2R"},
		{name: "zero", operand: "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
			prepareSetOpaqueVersion(c, dir, run)

			got := run(append([]string{"migrate", "set", tt.operand}, versionOrderTarget...)...)

			c.Assert(got.ExitCode, qt.Equals, 1)
			c.Assert(got.Stderr, qt.Equals, "Error: migration with version \""+tt.operand+"\" not found\n")
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1"})
		})
	}
}

// TestCompatMigrateSetOpaqueVersionMatchesTheCommunityBinaryE2E runs each
// operand on the pinned community binary and on ptah-compat and holds the
// summary, the refusal and the revision table to the community ones.
func TestCompatMigrateSetOpaqueVersionMatchesTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, operand := range []string{"R", "nope", "0"} {
		t.Run(operand, func(t *testing.T) {
			c := qt.New(t)
			communityDir, compatDir := c.TempDir(), c.TempDir()
			community := referenceVersionOrderRunner(c, communityDir, reference, environmentWithoutPtahVariables())
			compat := compatVersionOrderRunner(c, compatDir, environmentWithoutPtahVariables())
			prepareSetOpaqueVersion(c, communityDir, community)
			prepareSetOpaqueVersion(c, compatDir, compat)

			args := append([]string{"migrate", "set", operand}, versionOrderTarget...)
			want, got := community(args...), compat(args...)

			c.Assert(got.ExitCode, qt.Equals, want.ExitCode)
			c.Assert(setOpaqueDescription.ReplaceAllString(got.Stdout, "\n"), qt.Equals,
				setOpaqueDescription.ReplaceAllString(want.Stdout, "\n"))
			c.Assert(firstLine(got.Stderr), qt.Equals, firstLine(want.Stderr))
			c.Assert(revisionTableVersions(c, filepath.Join(compatDir, "app.db")), qt.DeepEquals,
				revisionTableVersions(c, filepath.Join(communityDir, "app.db")))
		})
	}
}
