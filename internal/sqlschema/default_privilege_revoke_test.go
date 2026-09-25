package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

const adp = "ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app "

// defaultPrivilege is one default privilege declaration as these rows compare
// it: the grantee, what is granted with its option, and what is revoked.
type defaultPrivilege struct {
	Grantee    string
	ObjectType string
	Privileges []schemamodel.PrivilegeGrant
	Revoked    []string
}

func defaultPrivileges(declarations []schemamodel.DefaultPrivilege) []defaultPrivilege {
	out := make([]defaultPrivilege, 0, len(declarations))
	for _, declaration := range declarations {
		out = append(out, defaultPrivilege{
			Grantee: declaration.Grantee, ObjectType: declaration.ObjectType,
			Privileges: declaration.Privileges, Revoked: declaration.Revoked,
		})
	}
	return out
}

func granted(names ...string) []schemamodel.PrivilegeGrant {
	grants := make([]schemamodel.PrivilegeGrant, 0, len(names))
	for _, name := range names {
		grants = append(grants, schemamodel.PrivilegeGrant{Privilege: name})
	}
	return grants
}

// TestRead_DefaultPrivilegeRevoke_HappyPath pins how ALTER DEFAULT PRIVILEGES
// ... REVOKE composes with the statements before it. It was read and dropped,
// so a file that granted and then revoked kept the grant (stokaro/ptah#3580).
func TestRead_DefaultPrivilegeRevoke_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []defaultPrivilege
	}{
		{
			name: "a revoke after a grant takes the privilege out of it",
			sql: adp + "GRANT SELECT, INSERT ON TABLES TO app_reader;\n" +
				adp + "REVOKE INSERT ON TABLES FROM app_reader;",
			want: []defaultPrivilege{{
				Grantee: "app_reader", ObjectType: "TABLES", Privileges: granted("SELECT"), Revoked: []string{"INSERT"},
			}},
		},
		{
			name: "a revoke with no grant before it is kept",
			sql:  adp + "REVOKE EXECUTE ON FUNCTIONS FROM app_reader;",
			want: []defaultPrivilege{{
				Grantee: "app_reader", ObjectType: "FUNCTIONS", Privileges: make([]schemamodel.PrivilegeGrant, 0), Revoked: []string{"EXECUTE"},
			}},
		},
		{
			name: "a grant after a revoke leaves the privilege granted",
			sql: adp + "REVOKE SELECT ON TABLES FROM app_reader;\n" +
				adp + "GRANT SELECT ON TABLES TO app_reader;",
			want: []defaultPrivilege{{Grantee: "app_reader", ObjectType: "TABLES", Privileges: granted("SELECT")}},
		},
		{
			name: "REVOKE ALL then one GRANT leaves that grant and revokes the rest",
			sql: adp + "REVOKE ALL ON SEQUENCES FROM app_reader;\n" +
				adp + "GRANT USAGE ON SEQUENCES TO app_reader;",
			want: []defaultPrivilege{{
				Grantee: "app_reader", ObjectType: "SEQUENCES", Privileges: granted("USAGE"), Revoked: []string{"SELECT", "UPDATE"},
			}},
		},
		{
			name: "GRANT OPTION FOR clears the option and keeps the privilege",
			sql: adp + "GRANT SELECT ON TABLES TO app_reader WITH GRANT OPTION;\n" +
				adp + "REVOKE GRANT OPTION FOR SELECT ON TABLES FROM app_reader;",
			want: []defaultPrivilege{{Grantee: "app_reader", ObjectType: "TABLES", Privileges: granted("SELECT")}},
		},
		{
			name: "PUBLIC in lower case is the keyword on both sides",
			sql: adp + "GRANT USAGE ON TYPES TO public;\n" +
				adp + "REVOKE USAGE ON TYPES FROM PUBLIC;",
			want: []defaultPrivilege{{
				Grantee: "PUBLIC", ObjectType: "TYPES", Privileges: make([]schemamodel.PrivilegeGrant, 0), Revoked: []string{"USAGE"},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(defaultPrivileges(database.DefaultPrivileges), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_DefaultPrivilegeRevoke_FailurePath pins the one form refused: the
// model cannot say a default privilege is held without its grant option
// unless the file grants it.
func TestRead_DefaultPrivilegeRevoke_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(adp+"REVOKE GRANT OPTION FOR SELECT ON TABLES FROM app_reader;"), platform.Postgres)

	c.Assert(err, qt.ErrorMatches, `(?s).*ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app REVOKE GRANT OPTION FOR SELECT ON TABLES FROM app_reader names a default privilege this schema does not grant: .*`)
	c.Assert(database.DefaultPrivileges, qt.IsNil)
}
