package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// TestRenderRoleMembershipOptionsRoundTrip renders a membership that states an
// option as a `membership` block and one that states none in `member_of`, and
// reads both back as they were. A membership written only in `member_of` could
// not carry an option, so without the block an option read from a server was
// lost on the way out.
func TestRenderRoleMembershipOptionsRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Roles: []schemamodel.Role{
		{
			Name: "app", Login: true, Inherit: true, MemberOf: []string{"auditors", "readers", "writers"},
			MemberOptions: map[string]schemamodel.RoleMembershipOptions{
				"readers": {Admin: new(true)},
				"writers": {Inherit: new(false), Set: new(true)},
			},
		},
		{Name: "auditors", Inherit: true},
		{Name: "readers", Inherit: true},
		{Name: "writers", Inherit: true},
	}}

	rendered, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.ParseWithOptions(t.Context(), rendered.Data, "schema.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, "  member_of = [role.auditors]\n"+
		"  membership {\n    role = role.readers\n    admin = true\n  }\n"+
		"  membership {\n    role = role.writers\n    inherit = false\n    set = true\n  }\n")
	c.Assert(parsed.Roles[0].MemberOf, qt.DeepEquals, []string{"auditors", "readers", "writers"})
	c.Assert(parsed.Roles[0].MemberOptions, qt.DeepEquals, db.Roles[0].MemberOptions)
}
