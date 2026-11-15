package mysqlviewsecurity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlviewsecurity"
)

// TestLost pins which declarations take INVOKER from a view: only one that
// names no mode, over a view held as INVOKER.
func TestLost(t *testing.T) {
	tests := []struct {
		name, held, declared string
		want                 bool
	}{
		{name: "no mode over invoker", held: "INVOKER", declared: "", want: true},
		{name: "a blank mode over invoker", held: "invoker", declared: "  ", want: true},
		{name: "no mode over definer", held: "DEFINER", declared: "", want: false},
		{name: "no mode over a view the catalog reports no mode for", held: "", declared: "", want: false},
		{name: "a declared invoker", held: "INVOKER", declared: "invoker", want: false},
		{name: "a declared definer", held: "INVOKER", declared: "DEFINER", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysqlviewsecurity.Lost(test.held, test.declared)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
