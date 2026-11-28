package unreadroles_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/unreadroles"
)

// TestReport_HappyPath writes a note for each unread state a read records,
// with its cause and, for a principal, the roles that hold it.
func TestReport_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		schema catalog.Database
		want   string
	}{
		{name: "nothing unread"},
		{
			name: "a principal",
			schema: catalog.Database{
				RoleMemberships: []catalog.RoleMembership{{Role: "legacy", Member: "lr"}, {Role: "legacy", Member: "lr2"}},
				UnreadRoles:     []catalog.UnreadRole{{Name: "legacy", Cause: "it is a login"}},
			},
			want: "note: 1 principal a role holds whose privileges this read did not read in full: legacy, held by lr, lr2: it is a login\n",
		},
		{
			name: "principals, one every account holds",
			schema: catalog.Database{
				RoleMemberships: []catalog.RoleMembership{{Role: "nm@localhost", Member: "nmh"}, {Role: "rh@localhost", Member: "PUBLIC"}},
				UnreadRoles: []catalog.UnreadRole{
					{Name: "nm@localhost", Cause: "two accounts read as this one name"},
					{Name: "rh@localhost", Cause: "it is the account 'rh'@'localhost'"},
				},
			},
			want: "note: 2 principals roles hold whose privileges this read did not read in full:\n" +
				"  nm@localhost, held by nmh: two accounts read as this one name\n" +
				"  rh@localhost, held by every account: it is the account 'rh'@'localhost'\n",
		},
		{
			name: "a mandatory role beside a role named PUBLIC",
			schema: catalog.Database{
				PublicIsRole:            true,
				EveryAccountHoldsUnread: &catalog.EveryAccountUnread{Roles: []string{"mand6@localhost"}, Cause: "a role named PUBLIC hides them"},
				UnreadRoles:             []catalog.UnreadRole{{Name: "mand6@localhost", Cause: "it is the account 'mand6'@'localhost'"}},
			},
			want: "note: every account holds the roles mand6@localhost whose memberships this read does not state: a role named PUBLIC hides them\n" +
				"note: 1 principal a role holds whose privileges this read did not read in full: " +
				"mand6@localhost, held by every account: it is the account 'mand6'@'localhost'\n",
		},
		{
			name: "a principal a role named PUBLIC holds",
			schema: catalog.Database{
				PublicIsRole:    true,
				RoleMemberships: []catalog.RoleMembership{{Role: "legacy", Member: "PUBLIC"}},
				UnreadRoles:     []catalog.UnreadRole{{Name: "legacy", Cause: "it is a login"}},
			},
			want: "note: 1 principal a role holds whose privileges this read did not read in full: legacy, held by PUBLIC: it is a login\n",
		},
		{
			name: "mandatory roles the read named",
			schema: catalog.Database{EveryAccountHoldsUnread: &catalog.EveryAccountUnread{
				Roles: []string{"mand", "mand6"}, Cause: "a role named PUBLIC hides which roles every account holds",
			}},
			want: "note: every account holds the roles mand, mand6 whose memberships this read does not state: " +
				"a role named PUBLIC hides which roles every account holds\n",
		},
		{
			name: "mandatory roles the read could not name",
			schema: catalog.Database{EveryAccountHoldsUnread: &catalog.EveryAccountUnread{
				Cause: "the read could not read @@GLOBAL.mandatory_roles: denied",
			}},
			want: "note: every account holds roles whose memberships this read does not state: " +
				"the read could not read @@GLOBAL.mandatory_roles: denied\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			unreadroles.Report(&out, &test.schema)

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestReport_NoStream writes nothing, and does not panic, without a stream or
// a read.
func TestReport_NoStream(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	schema := &catalog.Database{UnreadRoles: []catalog.UnreadRole{{Name: "legacy", Cause: "it is a login"}}}

	unreadroles.Report(nil, schema)
	unreadroles.Report(&out, nil)

	c.Assert(out.String(), qt.Equals, "")
}
