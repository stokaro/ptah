package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// TestRenderedDefaultPrivilegeRoundTrips is the loop an operator runs:
// `schema inspect > out.hcl` then `schema apply --to file://out.hcl`. A family
// that does not survive it is dropped in silence, and the comparison then reads
// the silence as a request to remove the object.
//
// Every part of the identity is asserted on the way back, the grantor included.
// It is the part a `permission` block cannot carry, and the whole reason this
// family has a block of its own.
func TestRenderedDefaultPrivilegeRoundTrips(t *testing.T) {
	tests := []struct {
		name  string
		roles []schemamodel.Role
	}{
		{
			name:  "roles the document declares",
			roles: []schemamodel.Role{{Name: "app_owner"}, {Name: "app_reader"}},
		},
		{
			// A reference resolves only where the block it names exists, so a
			// filtered export writes the quoted spelling instead. The identity
			// has to survive that too, or `--exclude '*[type=role]'` would cost
			// every default privilege in the document.
			name: "roles the document does not declare",
		},
		{
			name:  "a grantee the document does not declare",
			roles: []schemamodel.Role{{Name: "app_owner"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedTable("public")
			db.Roles = test.roles
			db.DefaultPrivileges = []schemamodel.DefaultPrivilege{{
				Grantor:    "app_owner",
				Schema:     "public",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{
					{Privilege: "SELECT"},
					{Privilege: "INSERT", WithOption: true},
				},
				Comment: "reader sees new tables",
			}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
			c.Assert(err, qt.IsNil)

			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.DefaultPrivileges, qt.HasLen, 1)
			c.Assert(parsed.DefaultPrivileges[0].Grantor, qt.Equals, "app_owner")
			c.Assert(parsed.DefaultPrivileges[0].Schema, qt.Equals, "public")
			c.Assert(parsed.DefaultPrivileges[0].ObjectType, qt.Equals, "TABLES")
			c.Assert(parsed.DefaultPrivileges[0].Grantee, qt.Equals, "app_reader")
			c.Assert(parsed.DefaultPrivileges[0].Comment, qt.Equals, "reader sees new tables")
			c.Assert(parsed.DefaultPrivileges[0].Privileges, qt.DeepEquals,
				[]schemamodel.PrivilegeGrant{
					{Privilege: "SELECT"},
					{Privilege: "INSERT", WithOption: true},
				})
		})
	}
}

// TestRenderDefaultPrivilegeReportsNoLoss is the reason the family is not routed
// through the `permission` block.
//
// That block reports a grantor as an export loss, and a loss diagnostic is what
// `ptah schema export --cleanup-go-annotations` turns into a refusal to clean.
// A family whose grantor is part of its identity would make every such run
// refuse. This block writes the grantor, so it has nothing to report.
func TestRenderDefaultPrivilegeReportsNoLoss(t *testing.T) {
	c := qt.New(t)

	rendered, err := atlashclrender.Render(&schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Roles:   []schemamodel.Role{{Name: "app_owner"}, {Name: "app_reader"}},
		DefaultPrivileges: []schemamodel.DefaultPrivilege{{
			Grantor:    "app_owner",
			Schema:     "public",
			ObjectType: "TABLES",
			Grantee:    "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	// The document carries the grantor, which is the other half: a render that
	// stayed silent by writing nothing would satisfy the assertion above.
	c.Assert(string(rendered.Data), qt.Contains, "default_privilege {")
	c.Assert(string(rendered.Data), qt.Contains, "app_owner")
}

// TestRenderDefaultPrivilegeDropsAnIncompleteDeclaration pins the other
// direction. The PostgreSQL renderer refuses a statement missing any part of the
// identity, so a block written anyway would round trip into a schema that cannot
// be applied -- and the diagnostic is what keeps the drop from being silent.
func TestRenderDefaultPrivilegeDropsAnIncompleteDeclaration(t *testing.T) {
	tests := []struct {
		name      string
		privilege schemamodel.DefaultPrivilege
	}{
		{
			name: "no grantor",
			privilege: schemamodel.DefaultPrivilege{
				Schema: "public", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
		},
		{
			name: "no schema",
			privilege: schemamodel.DefaultPrivilege{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
		},
		{
			name: "no object type",
			privilege: schemamodel.DefaultPrivilege{
				Grantor: "app_owner", Schema: "public", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
		},
		{
			name: "no grantee",
			privilege: schemamodel.DefaultPrivilege{
				Grantor: "app_owner", Schema: "public", ObjectType: "TABLES",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
		},
		{
			name: "no privilege",
			privilege: schemamodel.DefaultPrivilege{
				Grantor: "app_owner", Schema: "public", ObjectType: "TABLES",
				Grantee: "app_reader",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			rendered, err := atlashclrender.Render(&schemamodel.Database{
				Schemas:           []schemamodel.Schema{{Name: "public"}},
				DefaultPrivileges: []schemamodel.DefaultPrivilege{test.privilege},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(string(rendered.Data), qt.Not(qt.Contains), "default_privilege {")
			c.Assert(diagnosticMessages(rendered.Diagnostics), qt.Contains,
				"default privilege requires a grantor, a schema, an object type, "+
					"a grantee, and at least one privilege")
		})
	}
}

// TestRenderDefaultPrivilegeIsDeterministic pins that two renders of one schema
// produce identical bytes whatever order the declarations arrive in. Without the
// sort, a document Ptah writes twice differs between runs and every diff of an
// exported schema carries noise nobody wrote.
func TestRenderDefaultPrivilegeIsDeterministic(t *testing.T) {
	c := qt.New(t)

	first := renderDefaultPrivileges(c, "b_owner", "a_owner")
	second := renderDefaultPrivileges(c, "a_owner", "b_owner")

	c.Assert(first, qt.Equals, second)
}

// renderDefaultPrivileges renders one declaration per named grantor, in the
// order given.
func renderDefaultPrivileges(c *qt.C, grantors ...string) string {
	c.Helper()
	db := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}}
	for _, grantor := range grantors {
		db.DefaultPrivileges = append(db.DefaultPrivileges, schemamodel.DefaultPrivilege{
			Grantor:    grantor,
			Schema:     "public",
			ObjectType: "TABLES",
			Grantee:    "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		})
	}
	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	return string(rendered.Data)
}
