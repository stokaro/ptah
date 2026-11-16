package definer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/definer"
)

// TestReplace pins what writing an object again does to its definer: the same
// account keeps it, another account takes it, and a fact the read did not
// record is unknown rather than the same account.
func TestReplace(t *testing.T) {
	tests := []struct {
		name             string
		definer, account string
		want             definer.Replacement
	}{
		{name: "the connected account", definer: "app@%", account: "app@%", want: definer.Kept},
		{name: "another user", definer: "owner@%", account: "app@%", want: definer.Changed},
		{name: "the same user on another host", definer: "app@localhost", account: "app@%", want: definer.Changed},
		{name: "a user name holding an at sign", definer: "a@b@%", account: "a@b@%", want: definer.Kept},
		{name: "no definer", definer: "", account: "app@%", want: definer.Unknown},
		{name: "no connected account", definer: "app@%", account: "", want: definer.Unknown},
		{name: "neither", definer: "", account: "", want: definer.Unknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := definer.Replace(test.definer, test.account)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestReplacement_ZeroValueIsUnknown pins the zero value, so a verdict a
// caller never computed refuses rather than reads as the same account.
func TestReplacement_ZeroValueIsUnknown(t *testing.T) {
	c := qt.New(t)
	var zero definer.Replacement

	c.Assert(zero, qt.Equals, definer.Unknown)
}
