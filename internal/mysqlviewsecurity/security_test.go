package mysqlviewsecurity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlviewsecurity"
)

// TestKept pins the mode a view takes from its declaration: the one the
// declaration names, and the one the view holds where it names none.
func TestKept(t *testing.T) {
	tests := []struct {
		name, declared, held, want string
	}{
		{name: "no mode keeps invoker", declared: "", held: "INVOKER", want: "INVOKER"},
		{name: "no mode keeps definer", declared: "", held: "DEFINER", want: "DEFINER"},
		{name: "a blank mode is no mode", declared: "  ", held: "invoker", want: "INVOKER"},
		{name: "a declared definer replaces invoker", declared: "definer", held: "INVOKER", want: "DEFINER"},
		{name: "a declared invoker replaces definer", declared: "INVOKER", held: "DEFINER", want: "INVOKER"},
		{name: "a view the catalog reports no mode for", declared: "", held: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysqlviewsecurity.Kept(test.declared, test.held)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
