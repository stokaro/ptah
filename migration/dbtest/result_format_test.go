package dbtest_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/dbtest"
)

// atlasFormatDocument renders a one-case suite whose `exec` names the layout
// given, or names none when layout is empty.
//
// The two shapes have to come from one place: the happy path's "an author who
// names nothing gets CSV" row and the failure path's `format = ""` row differ
// by whether the attribute is written at all, and a fixture that could not
// express both would be asserting the wrong distinction.
func atlasFormatDocument(attribute string) []byte {
	return fmt.Appendf(nil, `
test "schema" "layout" {
  exec {
    sql    = "SELECT 1 AS a"
    output = "1"
%s
  }
}
`, attribute)
}

// TestParseAtlasTestCases_ResultFormat_HappyPath pins what each accepted
// spelling of `format` resolves to.
//
// The unnamed row is the one worth keeping: CSV is the documented zero value,
// so an author who writes no `format` must reach the runner with the same
// layout as one who writes `format = "csv"`, and a refusal added for the
// misspelled case must not start refusing silence.
func TestParseAtlasTestCases_ResultFormat_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		attribute string
		want      dbtest.ResultLayout
	}{
		{name: "no format named at all", attribute: "", want: ""},
		{name: "csv named explicitly", attribute: `    format = "csv"`, want: dbtest.ResultLayoutCSV},
		{name: "table named explicitly", attribute: `    format = "table"`, want: dbtest.ResultLayoutTable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cases, err := dbtest.ParseAtlasTestCases(
				atlasFormatDocument(test.attribute),
				"s.test.hcl",
				dbtest.AtlasTestKindSchema,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(cases, qt.HasLen, 1)
			c.Assert(cases[0].Steps, qt.HasLen, 1)
			c.Assert(cases[0].Steps[0].Assert, qt.IsNotNil)
			c.Assert(cases[0].Steps[0].Assert.ResultLayout, qt.Equals, test.want)
		})
	}
}

// TestParseAtlasTestCases_ResultFormat_FailurePath is the refusal this file
// exists for.
//
// The adapter mapped anything that was not `table` onto CSV, so a layout
// nothing renders reached the runner as the default and the case was compared
// against a layout its author did not ask for. Measured before the fix, on the
// compatibility binary: `format = "tabel"` and `format = "json"` each reported
// PASS and exit 0 against a CSV-shaped expectation.
//
// The message carries the file and line because a suite holds many `exec`
// blocks and a refusal naming none of them sends the reader looking.
func TestParseAtlasTestCases_ResultFormat_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		attribute string
		wantErr   string
	}{
		{
			name:      "a misspelling of an accepted layout",
			attribute: `    format = "tabel"`,
			wantErr:   `s\.test\.hcl:3: ` + "`exec`" + ` takes ` + "`format`" + ` "csv" or "table", got "tabel"`,
		},
		{
			name:      "a layout this package does not render",
			attribute: `    format = "json"`,
			wantErr:   `s\.test\.hcl:3: ` + "`exec`" + ` takes ` + "`format`" + ` "csv" or "table", got "json"`,
		},
		{
			name:      "an explicitly empty layout",
			attribute: `    format = ""`,
			wantErr:   `s\.test\.hcl:3: ` + "`exec`" + ` takes ` + "`format`" + ` "csv" or "table", got ""`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cases, err := dbtest.ParseAtlasTestCases(
				atlasFormatDocument(test.attribute),
				"s.test.hcl",
				dbtest.AtlasTestKindSchema,
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(cases, qt.HasLen, 0)
		})
	}
}
