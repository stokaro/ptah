package dbtest_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/dbtest"
)

func TestFilterCases_HappyPath(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{
		{Name: "users/create", Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
		{Name: "users/delete", Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
		{Name: "projects/create", Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
	}
	tests := []struct {
		name    string
		pattern string
		want    []dbtest.Case
	}{
		{
			name: "empty pattern selects all cases",
			want: cases,
		},
		{
			name:    "regular expression selects matching case names",
			pattern: `^users/`,
			want:    cases[:2],
		},
		{
			name:    "unmatched pattern returns an empty selection",
			pattern: `^missing$`,
			want:    []dbtest.Case{},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := dbtest.FilterCases(cases, tt.pattern)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

func TestFilterCases_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := dbtest.FilterCases(nil, `[`)

	c.Assert(err, qt.ErrorMatches, `compile test case pattern "\[":.*`)
	c.Assert(got, qt.IsNil)
}
