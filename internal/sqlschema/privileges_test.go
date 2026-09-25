package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// privilege is one grant or revoked grant as these rows compare it: who, what,
// and on which object, with the target spelled by [schemamodel.Grant.TargetKey]
// so a routine's argument types are part of it.
type privilege struct {
	Role       string
	Target     string
	Privileges []string
	WithOption bool
}

func privileges(grants []schemamodel.Grant) []privilege {
	if len(grants) == 0 {
		return nil
	}
	out := make([]privilege, 0, len(grants))
	for _, grant := range grants {
		out = append(out, privilege{
			Role: grant.Role, Target: grant.TargetKey(), Privileges: grant.Privileges, WithOption: grant.WithOption,
		})
	}
	return out
}

// TestRead_GrantAndRevoke_HappyPath pins how a schema file's GRANT and REVOKE
// statements compose. A schema file is a script: the later statement about one
// privilege of one grantee on one object wins, and a REVOKE that follows no
// GRANT still says the privilege is absent, because PostgreSQL hands out
// privileges nobody granted -- EXECUTE to PUBLIC on a new function, and what
// ALTER DEFAULT PRIVILEGES gives a role on a new table.
func TestRead_GrantAndRevoke_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantGrants  []privilege
		wantRevoked []privilege
	}{
		{
			name: "the SECURITY DEFINER pair wpmgr writes",
			sql: "REVOKE ALL ON FUNCTION purge_workspace(uuid) FROM PUBLIC;\n" +
				"GRANT EXECUTE ON FUNCTION purge_workspace(uuid) TO wpmgr_app;",
			wantGrants:  []privilege{{Role: "wpmgr_app", Target: "ROUTINE purge_workspace(uuid)", Privileges: []string{"EXECUTE"}}},
			wantRevoked: []privilege{{Role: "PUBLIC", Target: "ROUTINE purge_workspace(uuid)", Privileges: []string{"EXECUTE"}}},
		},
		{
			name: "a table revoke with no grant before it is kept, one privilege each",
			sql:  "REVOKE INSERT, UPDATE, DELETE ON plugin_signatures FROM wpmgr_app;",
			wantRevoked: []privilege{
				{Role: "wpmgr_app", Target: "TABLE plugin_signatures", Privileges: []string{"INSERT"}},
				{Role: "wpmgr_app", Target: "TABLE plugin_signatures", Privileges: []string{"UPDATE"}},
				{Role: "wpmgr_app", Target: "TABLE plugin_signatures", Privileges: []string{"DELETE"}},
			},
		},
		{
			name:        "a revoke after a grant takes the privilege out of it",
			sql:         "GRANT SELECT, INSERT ON t TO r;\nREVOKE INSERT ON t FROM r;",
			wantGrants:  []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}}},
			wantRevoked: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"INSERT"}}},
		},
		{
			name:       "a grant after a revoke leaves the privilege held",
			sql:        "REVOKE SELECT ON t FROM r;\nGRANT SELECT ON t TO r;",
			wantGrants: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}}},
		},
		{
			name: "REVOKE ALL on a table names every table privilege",
			sql:  "REVOKE ALL PRIVILEGES ON TABLE t FROM r;",
			wantRevoked: []privilege{
				{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"INSERT"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"UPDATE"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"DELETE"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"TRUNCATE"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"REFERENCES"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"TRIGGER"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"MAINTAIN"}},
			},
		},
		{
			name:       "GRANT OPTION FOR clears the option and keeps the privilege",
			sql:        "GRANT SELECT ON t TO r WITH GRANT OPTION;\nREVOKE GRANT OPTION FOR SELECT ON t FROM r;",
			wantGrants: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}}},
		},
		{
			name:       "GRANT ALL on a routine is EXECUTE",
			sql:        "GRANT ALL ON PROCEDURE archive(integer) TO r;",
			wantGrants: []privilege{{Role: "r", Target: "ROUTINE archive(integer)", Privileges: []string{"EXECUTE"}}},
		},
		{
			name:        "PUBLIC in lower case and quoted lower case is the keyword",
			sql:         "revoke execute on function f() from public;\nGRANT EXECUTE ON FUNCTION g() TO \"public\";",
			wantGrants:  []privilege{{Role: "PUBLIC", Target: "ROUTINE g()", Privileges: []string{"EXECUTE"}}},
			wantRevoked: []privilege{{Role: "PUBLIC", Target: "ROUTINE f()", Privileges: []string{"EXECUTE"}}},
		},
		{
			name:       "FUNCTION and ROUTINE name the same routine",
			sql:        "REVOKE EXECUTE ON FUNCTION f(uuid) FROM PUBLIC;\nGRANT EXECUTE ON ROUTINE f(UUID) TO PUBLIC;",
			wantGrants: []privilege{{Role: "PUBLIC", Target: "ROUTINE f(uuid)", Privileges: []string{"EXECUTE"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(privileges(database.Grants), qt.DeepEquals, test.wantGrants)
			c.Assert(privileges(database.RevokedGrants), qt.DeepEquals, test.wantRevoked)
		})
	}
}

// TestRead_GrantAndRevoke_FailurePath pins the compositions refused rather than
// approximated. Each has a reading the model cannot state: a privilege held
// without its grant option when the file never granted it, and one privilege
// taken out of an ALL whose members depend on the server version.
func TestRead_GrantAndRevoke_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "GRANT OPTION FOR with no grant in the file",
			sql:     "REVOKE GRANT OPTION FOR SELECT ON t FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR SELECT ON TABLE t FROM r names a grant this schema does not make: .*`,
		},
		{
			name:    "a revoke of one privilege after GRANT ALL on a table",
			sql:     "GRANT ALL ON t TO r;\nREVOKE TRUNCATE ON t FROM r;",
			wantErr: `REVOKE ON TABLE t FROM r follows a GRANT ALL on it, .*name the privileges in the GRANT`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr)
			c.Assert(database.Grants, qt.IsNil)
		})
	}
}
