package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
)

// TestToDBSchema_RoleMembershipOptions describes a declared membership the way
// a catalog read reports it: the admin option always, and the inherit and set
// options only where the declaration states them, since a declaration that
// states none leaves the server's default.
func TestToDBSchema_RoleMembershipOptions(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Roles: []schemamodel.Role{
		{Name: "readers", Inherit: true},
		{
			Name: "app", Inherit: true, MemberOf: []string{"readers", "writers"},
			MemberOptions: map[string]schemamodel.RoleMembershipOptions{
				"writers": {Admin: new(true), Inherit: new(false), Set: new(true)},
			},
		},
	}}

	got, err := goschematodb.ToDBSchema(t.Context(), db, platform.Postgres, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(got.RoleMemberships, qt.DeepEquals, []catalog.RoleMembership{
		{Role: "readers", Member: "app"},
		{Role: "writers", Member: "app", AdminOption: true, InheritOption: new(false), SetOption: new(true)},
	})
}
