package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_RoleMembershipOptions reads the options a
// server holds the way a declaration states them: an option that differs from
// the default is stated, and a membership that holds the defaults is a plain
// member_of entry, so a read declaration compares equal to the server it came
// from.
func TestConvertDBSchemaToGoSchema_RoleMembershipOptions(t *testing.T) {
	c := qt.New(t)
	source := &catalog.Database{
		Roles: []catalog.Role{
			{Name: "readers", Inherit: true},
			{Name: "writers", Inherit: true},
			{Name: "audit", Inherit: true},
			{Name: "app", Inherit: true},
			{Name: "quiet", Inherit: false},
		},
		RoleMemberships: []catalog.RoleMembership{
			{Role: "readers", Member: "app", InheritOption: new(true), SetOption: new(true)},
			{Role: "writers", Member: "app", AdminOption: true, InheritOption: new(false), SetOption: new(true)},
			{Role: "audit", Member: "app", InheritOption: new(true), SetOption: new(false)},
			{Role: "readers", Member: "quiet", InheritOption: new(false), SetOption: new(true)},
		},
	}

	database, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), source, "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	byName := make(map[string]schemamodel.Role)
	for _, role := range database.Roles {
		byName[role.Name] = role
	}
	c.Assert(byName["app"].MemberOf, qt.DeepEquals, []string{"readers", "writers", "audit"})
	c.Assert(byName["app"].MemberOptions, qt.DeepEquals, map[string]schemamodel.RoleMembershipOptions{
		"writers": {Admin: new(true), Inherit: new(false)},
		"audit":   {Set: new(false)},
	})
	// The default of inherit is the member's own attribute, so a membership of
	// a role that does not inherit holds the default when its option is false.
	c.Assert(byName["quiet"].MemberOf, qt.DeepEquals, []string{"readers"})
	c.Assert(byName["quiet"].MemberOptions, qt.IsNil)
}
