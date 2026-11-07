package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// TestRenderRoleGroupsRoundTrip renders a YDB group as `group = true` and the
// groups a role joins as `member_of`, a declared group by its block and an
// undeclared one, such as YDB's DATA-READERS, by its name, and reads both back
// as they were. Without them a group read back as a user, which YDB cannot
// turn into a group, and every membership was lost.
func TestRenderRoleGroupsRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Roles: []schemamodel.Role{
		{Name: "alice", Login: true, Inherit: true, MemberOf: []string{"readers", "DATA-READERS"}},
		{Name: "readers", Group: true, Inherit: true},
	}}

	rendered, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.ParseWithOptions(t.Context(), rendered.Data, "schema.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains,
		"role \"alice\" {\n  login = true\n  inherit = true\n  member_of = [role.readers, \"DATA-READERS\"]\n}\n")
	c.Assert(string(rendered.Data), qt.Contains, "role \"readers\" {\n  inherit = true\n  group = true\n}\n")
	c.Assert(parsed.Roles, qt.DeepEquals, db.Roles)
}
