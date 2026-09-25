package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// TestRenderedRevokedDefaultPrivilegeRoundTrips carries a default privilege's
// revoked list through `schema inspect > out.hcl` and back, alone and beside a
// granted list. Dropped on the way, the document would let the database keep
// a default the schema file revoked.
func TestRenderedRevokedDefaultPrivilegeRoundTrips(t *testing.T) {
	tests := []struct {
		name       string
		privileges []schemamodel.PrivilegeGrant
		revoked    []string
	}{
		{name: "revoked only", revoked: []string{"INSERT", "UPDATE"}},
		{name: "granted and revoked", privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}}, revoked: []string{"DELETE"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := inspectedTable("public")
			db.DefaultPrivileges = []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "public", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: test.privileges, Revoked: test.revoked,
			}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.DefaultPrivileges, qt.HasLen, 1)
			c.Assert(parsed.DefaultPrivileges[0].Revoked, qt.DeepEquals, test.revoked)
			c.Assert(parsed.DefaultPrivileges[0].Privileges, qt.HasLen, len(test.privileges))
		})
	}
}

// TestParseDefaultPrivilegeRevoked_FailurePath pins the blocks refused: one
// granting and revoking nothing, and one granting and revoking the same
// privilege, which a document with no statement order cannot resolve.
func TestParseDefaultPrivilegeRevoked_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "nothing granted or revoked",
			body:    `for_role = "o"` + "\n" + `schema = schema.public` + "\n" + `object_type = "TABLES"` + "\n" + `to = "r"`,
			wantErr: `(?s).*default_privilege requires privileges or revoked.*`,
		},
		{
			name: "one privilege granted and revoked",
			body: `for_role = "o"` + "\n" + `schema = schema.public` + "\n" + `object_type = "TABLES"` + "\n" + `to = "r"` + "\n" +
				`privileges = ["SELECT"]` + "\n" + `revoked = ["SELECT"]`,
			wantErr: `(?s).*default privilege SELECT on TABLES in schema public for role o is both granted to and revoked from "r"; declare one or the other.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := atlashcl.Parse([]byte("schema \"public\" {}\n\ndefault_privilege {\n"+test.body+"\n}\n"), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(parsed, qt.IsNil)
		})
	}
}
