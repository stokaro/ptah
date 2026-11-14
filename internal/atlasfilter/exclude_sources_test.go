package atlasfilter_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasfilter"
)

// sourcedValues are the exclude values a run read from one source.
type sourcedValues struct {
	source string
	values []string
}

// TestUnmatchedExcludeError_NamesTheSourceOfEachSelector keeps the diagnostic
// pointing at the place the operator wrote each selector. A selector the
// project file lists, named as an --exclude the command line never carried,
// sends the reader looking for a flag that is not there.
func TestUnmatchedExcludeError_NamesTheSourceOfEachSelector(t *testing.T) {
	tests := []struct {
		name      string
		sources   []sourcedValues
		selectors []string
		want      string
	}{
		{
			name:      "the project file",
			sources:   []sourcedValues{{source: "ptah.yaml", values: []string{"nosuch_file"}}},
			selectors: []string{"nosuch_file"},
			want:      `the exclude selection from ptah.yaml matched no objects: "nosuch_file"`,
		},
		{
			name:      "an environment variable, split as the parser splits it",
			sources:   []sourcedValues{{source: "PTAH_EXCLUDE", values: []string{"first, second"}}},
			selectors: []string{"first", "second"},
			want:      `the exclude selection from PTAH_EXCLUDE matched no objects: "first", "second"`,
		},
		{
			name:      "a flag of another name",
			sources:   []sourcedValues{{source: "--drift-exclude", values: []string{"nosuch"}}},
			selectors: []string{"nosuch"},
			want:      `the --drift-exclude selection matched no objects: "nosuch"`,
		},
		{
			name: "each source in the order it first appears",
			sources: []sourcedValues{
				{source: "config/atlas.hcl", values: []string{"from_file", "other_file"}},
				{source: "--exclude", values: []string{"from_flag"}},
			},
			selectors: []string{"from_file", "from_flag", "other_file"},
			want: `the exclude selection from config/atlas.hcl matched no objects: "from_file", "other_file"; ` +
				`the --exclude selection matched no objects: "from_flag"`,
		},
		{
			name: "a selector two sources name keeps the first",
			sources: []sourcedValues{
				{source: "ptah.yaml", values: []string{"shared"}},
				{source: "PTAH_EXCLUDE", values: []string{"shared"}},
			},
			selectors: []string{"shared"},
			want:      `the exclude selection from ptah.yaml matched no objects: "shared"`,
		},
		{
			name:      "a selector no source names is --exclude's",
			sources:   []sourcedValues{{source: "ptah.yaml", values: []string{"listed"}}},
			selectors: []string{"unlisted"},
			want:      `the --exclude selection matched no objects: "unlisted"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sources := atlasfilter.ExcludeSources{}
			for _, source := range test.sources {
				sources.Add(source.source, source.values...)
			}

			err := &atlasfilter.UnmatchedExcludeError{Selectors: test.selectors, Sources: sources}

			c.Assert(err.Error(), qt.Equals, test.want)
		})
	}
}

// TestReportUnmatched_NamesTheSourceOfEachSelector holds the warning the
// native planners write to the same wording as the error.
func TestReportUnmatched_NamesTheSourceOfEachSelector(t *testing.T) {
	c := qt.New(t)
	sources := atlasfilter.ExcludeSources{}
	sources.Add("ptah.yaml", "nothing_here")
	var out bytes.Buffer

	atlasfilter.ReportUnmatched(&out, sources,
		atlasfilter.ExcludeReport{Unmatched: []string{"nothing_here", "external_audit"}},
		atlasfilter.ExcludeReport{Unmatched: []string{"nothing_here"}},
	)

	c.Assert(out.String(), qt.Equals, "Warning: the exclude selection from ptah.yaml matched no objects: \"nothing_here\".\n")
}
