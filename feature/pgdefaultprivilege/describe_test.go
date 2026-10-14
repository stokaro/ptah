package pgdefaultprivilege_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/feature/pgdefaultprivilege"
)

func TestChangeDescriptionPreservesRightsAndOptions(t *testing.T) {
	for _, test := range []struct {
		name   string
		change *pgdefaultprivilege.Change
		want   string
	}{
		{name: "nil", want: "invalid default privilege change"},
		{name: "empty", change: &pgdefaultprivilege.Change{}, want: "invalid default privilege change"},
		{name: "grants sorted", change: &pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "select"}, {Privilege: "INSERT", WithOption: true}}}, want: "grant INSERT WITH GRANT OPTION, SELECT"},
		{name: "revokes retain grant options", change: &pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}, want: "revoke SELECT WITH GRANT OPTION"},
		{name: "option changes", change: &pgdefaultprivilege.Change{GrantOptionsAdded: []string{"select"}, GrantOptionsRevoked: []string{"update"}}, want: "grant option SELECT; revoke grant option UPDATE"},
		{name: "restore", change: &pgdefaultprivilege.Change{Restore: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}, Revoked: []string{"INSERT"}}}, want: "restore grants [SELECT WITH GRANT OPTION]; revoked [INSERT]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := test.change.Copy()
			c.Assert(test.change.String(), qt.Equals, test.want)
			c.Assert(test.change, qt.DeepEquals, before)
		})
	}
}
