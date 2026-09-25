package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// TestRenderedRevokeRoundTrips is the loop an operator runs: `schema inspect
// > out.hcl` then `schema apply --to file://out.hcl`. A revoke that did not
// survive it would come back as nothing, and nothing lets the database keep a
// privilege the schema says is absent.
func TestRenderedRevokeRoundTrips(t *testing.T) {
	tests := []struct {
		name    string
		revoked schemamodel.Grant
		want    schemamodel.Grant
	}{
		{
			name:    "PUBLIC on a table",
			revoked: schemamodel.Grant{Role: "PUBLIC", Privileges: []string{"TRUNCATE"}, OnTable: "public.t", Comment: "nobody truncates"},
			want:    schemamodel.Grant{Role: "PUBLIC", Privileges: []string{"TRUNCATE"}, OnTable: "public.t", Comment: "nobody truncates"},
		},
		{
			name:    "a role the document does not declare, on a schema",
			revoked: schemamodel.Grant{Role: "wpmgr_app", Privileges: []string{"CREATE"}, OnSchema: "public"},
			want:    schemamodel.Grant{Role: "wpmgr_app", Privileges: []string{"CREATE"}, OnSchema: "public"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := inspectedTable("public")
			db.RevokedGrants = []schemamodel.Grant{test.revoked}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)

			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{test.want})
			c.Assert(parsed.Grants, qt.HasLen, 0)
		})
	}
}

// TestParseRevoke_FailurePath pins the `revoke` blocks refused, the
// contradiction with a `permission` block among them: a document has no
// statement order, so neither block could win.
func TestParseRevoke_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "no privileges",
			body:    "revoke {\n  from = \"PUBLIC\"\n  for = table.t\n  privileges = []\n}\n",
			wantErr: `(?s).*revoke requires privileges.*`,
		},
		{
			name:    "no grantee",
			body:    "revoke {\n  for = table.t\n  privileges = [\"SELECT\"]\n}\n",
			wantErr: `(?s).*revoke requires from.*`,
		},
		{
			name:    "a grant option has no meaning on a revoke",
			body:    "revoke {\n  from = \"PUBLIC\"\n  for = table.t\n  privileges = [\"SELECT\"]\n  grantable = true\n}\n",
			wantErr: `(?s).*grantable.*`,
		},
		{
			name: "the same privilege granted and revoked",
			body: "permission {\n  to = \"app\"\n  for = table.t\n  privileges = [\"SELECT\"]\n}\n" +
				"revoke {\n  from = \"app\"\n  for = table.t\n  privileges = [\"SELECT\"]\n}\n",
			wantErr: `(?s).*SELECT on TABLE public.t is both granted to and revoked from "app"; declare one or the other.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := "schema \"public\" {}\n\ntable \"t\" {\n  schema = schema.public\n  column \"id\" {\n    type = int\n  }\n}\n\n" + test.body

			parsed, err := atlashcl.Parse([]byte(document), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(parsed, qt.IsNil)
		})
	}
}
