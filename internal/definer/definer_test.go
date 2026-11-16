package definer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/definer"
)

// TestReplace pins what writing an object again does to its definer: the same
// account keeps it, another account takes it, and a fact the read did not
// record is unknown rather than the same account. The account is compared as
// the server compares it, the user name exactly and the host without ASCII
// case; both servers write a host in lowercase, so the capital rows stand for
// a reader that spells it otherwise.
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
		{name: "a host in capitals", definer: "app@LOCALHOST", account: "app@localhost", want: definer.Kept},
		{name: "a host in mixed case", definer: "app@Build.Example.COM", account: "app@build.example.com", want: definer.Kept},
		{name: "a user name in capitals", definer: "APP@localhost", account: "app@localhost", want: definer.Changed},
		{name: "a user name holding an at sign and a host in capitals", definer: "a@B@LOCALHOST", account: "a@B@localhost", want: definer.Kept},
		{name: "a user name differing in case before an at sign", definer: "a@B@localhost", account: "a@b@localhost", want: definer.Changed},
		{name: "a Kelvin sign in the host", definer: "app@\u212Aube", account: "app@kube", want: definer.Changed},
		{name: "a host outside ASCII spelled the same", definer: "app@h\u00f6st", account: "app@h\u00f6st", want: definer.Kept},
		{name: "a role name with no host", definer: "auditor", account: "auditor@%", want: definer.Changed},
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
