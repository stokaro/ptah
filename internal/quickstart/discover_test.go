package quickstart_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/quickstart"
)

// TestDiscover_HappyPath reads a tree and returns only the pages that opt in.
func TestDiscover_HappyPath(t *testing.T) {
	c := qt.New(t)

	pages, err := quickstart.Discover("testdata/pages")

	c.Assert(err, qt.IsNil)
	c.Assert(pages, qt.HasLen, 1)
	c.Assert(pages[0].Path, qt.Equals, optedInPage)
	c.Assert(pages[0].Title, qt.Equals, "A fixture quick start")
}

// TestCheckFloors_FailurePath is the guard against a run that discovers nothing
// and reports what a complete run reports.
//
// Each floor names the count it found, because "the pages moved" and "the
// extractor stopped matching" look the same from a bare failure.
func TestCheckFloors_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		shell   quickstart.Shell
		wantErr string
	}{
		{
			name:  "bash",
			shell: quickstart.Bash,
			wantErr: `(?s).*found 1 page\(s\) with quickstart: true in their frontmatter, expected at least 2.*` +
				`publishes 3 bash step\(s\), expected at least 6.*` +
				`asserts 2 bash output block\(s\), expected at least 4.*`,
		},
		{
			name:    "powershell",
			shell:   quickstart.PowerShell,
			wantErr: `(?s).*publishes 3 powershell step\(s\), expected at least 6.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pages, err := quickstart.Discover("testdata/pages")
			c.Assert(err, qt.IsNil)

			c.Assert(quickstart.CheckFloors(pages, test.shell, quickstart.DefaultFloors()), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestCheckFloors_HappyPath is the control: the same pages clear floors they
// actually meet, so the failures above come from the counts and not from a
// check that always fails.
func TestCheckFloors_HappyPath(t *testing.T) {
	c := qt.New(t)

	pages, err := quickstart.Discover("testdata/pages")
	c.Assert(err, qt.IsNil)

	floors := quickstart.Floors{Pages: 1, Steps: 3, Expectations: 2}
	c.Assert(quickstart.CheckFloors(pages, quickstart.Bash, floors), qt.IsNil)
	c.Assert(quickstart.CheckFloors(pages, quickstart.PowerShell, floors), qt.IsNil)
}
