//go:build integration

package integration_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// lintNameOrderFiles hold numbered files whose name order differs from their
// number order, a version that is not a number and two repeatables.
var lintNameOrderFiles = []string{"9_a.sql", "10_b.sql", "v1_x.sql", "R__v.sql", "1R__w.sql", "2_c.sql"}

// lintNameOrderArgs lints the last n files of the directory, over an in-memory
// SQLite dev database.
func lintNameOrderArgs(n string, extra ...string) []string {
	return append([]string{
		"migrate", "lint", "--dir", "file://migrations", "--dev-url", "sqlite://file?mode=memory", "--latest", n,
	}, extra...)
}

// lintNameOrderTemplate prints the files a lint selected, in report order.
const lintNameOrderTemplate = `{{ range .Files }}{{ .Name }} {{ end }}`

// analyzingLines keeps the lines of a lint report that name what it analyzed,
// leaving out the timings, which differ on every run.
func analyzingLines(report string) []string {
	return slices.DeleteFunc(strings.Split(report, "\n"), func(line string) bool {
		return !strings.Contains(line, "nalyzing")
	})
}

// TestCompatAtlasLintReportsFilesInNameOrderE2E reports the files of an Atlas
// directory in file-name order, as the pinned community binary does:
// 10_b.sql before 9_a.sql, and v1_x.sql last. The header names the file
// before the selection in that order as its start.
func TestCompatAtlasLintReportsFilesInNameOrderE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	writeVersionOrderFiles(c, dir, run, lintNameOrderFiles)

	all := run(lintNameOrderArgs("6", "--format", lintNameOrderTemplate)...)
	last := run(lintNameOrderArgs("2")...)

	c.Assert(all.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", all.Stderr))
	c.Assert(all.Stdout, qt.Equals, "10_b.sql 1R__w.sql 2_c.sql 9_a.sql R__v.sql v1_x.sql ")
	c.Assert(last.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", last.Stderr))
	c.Assert(analyzingLines(last.Stdout), qt.DeepEquals, []string{
		"Analyzing changes from version 9 to v1 (2 migrations in total):",
		"  -- analyzing version R",
		"  -- analyzing version v1",
	})
}

// TestCompatAtlasLintNameOrderMatchesTheCommunityBinaryE2E holds the report
// order and the header of both selections to the pinned community binary
// .
func TestCompatAtlasLintNameOrderMatchesTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	c := qt.New(t)
	communityDir, compatDir := c.TempDir(), c.TempDir()
	community := referenceVersionOrderRunner(c, communityDir, reference, environmentWithoutPtahVariables())
	compat := compatVersionOrderRunner(c, compatDir, environmentWithoutPtahVariables())
	writeVersionOrderFiles(c, communityDir, community, lintNameOrderFiles)
	writeVersionOrderFiles(c, compatDir, compat, lintNameOrderFiles)

	for _, n := range []string{"6", "2"} {
		c.Assert(analyzingLines(compat(lintNameOrderArgs(n)...).Stdout), qt.DeepEquals,
			analyzingLines(community(lintNameOrderArgs(n)...).Stdout), qt.Commentf("--latest %s", n))
		c.Assert(compat(lintNameOrderArgs(n, "--format", lintNameOrderTemplate)...).Stdout, qt.Equals,
			community(lintNameOrderArgs(n, "--format", lintNameOrderTemplate)...).Stdout, qt.Commentf("--latest %s", n))
	}
}
