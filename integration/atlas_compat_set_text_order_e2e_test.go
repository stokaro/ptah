//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// setTextOrderCases set the revision to first and then to 2, with the rows
// atlas_schema_revisions records after each, measured on the pinned community
// binary v1.3.0. Atlas orders versions as text: 10 sorts between 1 and 2, so a
// set to 2 keeps 10, and v1 sorts above every number.
var setTextOrderCases = []struct {
	name       string
	files      []string
	first      string
	firstSet   string
	firstRows  []string
	secondSet  string
	secondRows []string
}{
	{
		name:       "a version that is not a number",
		files:      []string{"1_a.sql", "v1_x.sql", "2_b.sql", "10_c.sql"},
		first:      "v1",
		firstSet:   "Current version is v1 (4 set):\n\n  + 1 (a)\n  + 10 (c)\n  + 2 (b)\n  + v1 (x)\n\n",
		firstRows:  []string{"1", "10", "2", "v1"},
		secondSet:  "Current version is 2 (1 removed):\n\n  - v1 (x)\n\n",
		secondRows: []string{"1", "10", "2"},
	},
	{
		name:       "numbers alone",
		files:      []string{"1_a.sql", "2_b.sql", "10_c.sql"},
		first:      "10",
		firstSet:   "Current version is 10 (2 set):\n\n  + 1 (a)\n  + 10 (c)\n\n",
		firstRows:  []string{"1", "10"},
		secondSet:  "Current version is 2 (1 set):\n\n  + 2 (b)\n\n",
		secondRows: []string{"1", "10", "2"},
	},
}

// TestCompatAtlasSetTextOrderE2E sets the revision as Atlas orders versions.
func TestCompatAtlasSetTextOrderE2E(t *testing.T) {
	for _, tt := range setTextOrderCases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
			writeVersionOrderFiles(c, dir, run, tt.files)
			database := filepath.Join(dir, "app.db")

			first := run(append([]string{"migrate", "set", tt.first}, versionOrderTarget...)...)
			firstRows := revisionTableVersions(c, database)
			second := run(append([]string{"migrate", "set", "2"}, versionOrderTarget...)...)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.Stderr))
			c.Assert(first.Stdout, qt.Equals, tt.firstSet)
			c.Assert(firstRows, qt.DeepEquals, tt.firstRows)
			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", second.Stderr))
			c.Assert(second.Stdout, qt.Equals, tt.secondSet)
			c.Assert(revisionTableVersions(c, database), qt.DeepEquals, tt.secondRows)
		})
	}
}

// TestCompatAtlasSetTextOrderMatchesTheCommunityBinaryE2E holds both sets,
// the rows they leave and the status report after them to the pinned
// community binary v1.3.0.
func TestCompatAtlasSetTextOrderMatchesTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, tt := range setTextOrderCases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			communityDir, compatDir := c.TempDir(), c.TempDir()
			community := referenceVersionOrderRunner(c, communityDir, reference, environmentWithoutPtahVariables())
			compat := compatVersionOrderRunner(c, compatDir, environmentWithoutPtahVariables())
			writeVersionOrderFiles(c, communityDir, community, tt.files)
			writeVersionOrderFiles(c, compatDir, compat, tt.files)

			for _, version := range []string{tt.first, "2"} {
				set := append([]string{"migrate", "set", version}, versionOrderTarget...)
				communitySet, compatSet := community(set...), compat(set...)
				c.Assert(compatSet.ExitCode, qt.Equals, communitySet.ExitCode)
				c.Assert(compatSet.Stdout, qt.Equals, communitySet.Stdout)
				c.Assert(revisionTableVersions(c, filepath.Join(compatDir, "app.db")), qt.DeepEquals,
					revisionTableVersions(c, filepath.Join(communityDir, "app.db")))
			}
			status := append(append([]string{"migrate", "status"}, versionOrderTarget...),
				"--format", `{{ .Current }}|{{ .Next }}|{{ json .Pending }}|{{ .Error }}`)
			c.Assert(compat(status...).Stdout, qt.Equals, community(status...).Stdout)
		})
	}
}
