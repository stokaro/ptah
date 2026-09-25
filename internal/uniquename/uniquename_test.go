package uniquename_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/uniquename"
)

// Next keeps a free base, and otherwise numbers from 2, skipping a numbered
// form another object already holds.
func TestNext_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		base  string
		taken []string
		want  string
	}{
		{name: "a free base is kept", base: "Doc", taken: []string{"Order"}, want: "Doc"},
		{name: "a taken base takes 2", base: "Doc", taken: []string{"Doc"}, want: "Doc2"},
		{name: "a taken 2 takes 3", base: "Doc", taken: []string{"Doc", "Doc2"}, want: "Doc3"},
		{name: "a gap is filled from 2", base: "Doc", taken: []string{"Doc", "Doc3"}, want: "Doc2"},
		{name: "a base ending in a digit takes the next suffix after it", base: "Doc2", taken: []string{"Doc2"}, want: "Doc22"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := uniquename.Next(test.base, func(name string) bool { return slices.Contains(test.taken, name) })

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
