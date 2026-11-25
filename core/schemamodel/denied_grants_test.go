package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// deny is a DENY of privilege on t to app, on columns where any are named.
func deny(privilege string, columns ...string) schemamodel.Grant {
	return schemamodel.Grant{Role: "app", Privileges: []string{privilege}, OnTable: "t", Columns: columns}
}

// TestValidateDeniedGrants_HappyPath pins the denies that hold beside the
// grants and revokes of a schema: a different privilege, grantee or column, a
// grant on a column under a DENY on its table, and a DENY on a column under a
// grant on its table, which SQL Server keeps as two rows.
func TestValidateDeniedGrants_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		db   *schemamodel.Database
	}{
		{name: "a deny alone", db: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{deny("SELECT")}}},
		{name: "a deny beside a grant of another privilege", db: &schemamodel.Database{
			DeniedGrants: []schemamodel.Grant{deny("UPDATE")}, Grants: []schemamodel.Grant{deny("SELECT")},
		}},
		{name: "a deny beside a grant to another role", db: &schemamodel.Database{
			DeniedGrants: []schemamodel.Grant{deny("SELECT")},
			Grants:       []schemamodel.Grant{{Role: "other", Privileges: []string{"SELECT"}, OnTable: "t"}},
		}},
		{name: "a deny on a table and a grant on its column", db: &schemamodel.Database{
			DeniedGrants: []schemamodel.Grant{deny("SELECT")}, Grants: []schemamodel.Grant{deny("SELECT", "name")},
		}},
		{name: "a deny on a column and a grant on its table", db: &schemamodel.Database{
			DeniedGrants: []schemamodel.Grant{deny("SELECT", "secret")}, Grants: []schemamodel.Grant{deny("SELECT")},
		}},
		{name: "a deny on one column and a grant on another", db: &schemamodel.Database{
			DeniedGrants: []schemamodel.Grant{deny("SELECT", "secret")}, Grants: []schemamodel.Grant{deny("SELECT", "name")},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemamodel.ValidateDeniedGrants(test.db), qt.IsNil)
		})
	}
}

// TestValidateDeniedGrants_FailurePath pins the denies a schema cannot keep:
// one that names no privilege or carries a grant option, one the schema also
// grants or revokes, and a DENY on a column under a DENY on the whole table,
// which adds no row on SQL Server.
func TestValidateDeniedGrants_FailurePath(t *testing.T) {
	withOption := deny("SELECT")
	withOption.WithOption = true
	tests := []struct {
		name    string
		db      *schemamodel.Database
		wantErr string
	}{
		{
			name:    "no privilege",
			db:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{{Role: "app", OnTable: "t"}}},
			wantErr: `a DENY to "app" on TABLE t names no privilege`,
		},
		{
			name:    "a grant option",
			db:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{withOption}},
			wantErr: `a DENY to "app" on TABLE t carries WITH GRANT OPTION, which a DENY has not`,
		},
		{
			name:    "also granted",
			db:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{deny("SELECT")}, Grants: []schemamodel.Grant{deny("SELECT")}},
			wantErr: `SELECT on TABLE t is both denied and granted to "app"; declare one or the other`,
		},
		{
			name: "also granted as ALL",
			db: &schemamodel.Database{
				DeniedGrants: []schemamodel.Grant{deny("DELETE")}, Grants: []schemamodel.Grant{deny("ALL")},
			},
			wantErr: `DELETE on TABLE t is both denied and granted to "app"; declare one or the other`,
		},
		{
			name:    "also revoked",
			db:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{deny("SELECT")}, RevokedGrants: []schemamodel.Grant{deny("SELECT")}},
			wantErr: `SELECT on TABLE t is both denied to and revoked from "app"; declare one or the other`,
		},
		{
			name: "a column deny under a deny on the table",
			db: &schemamodel.Database{
				DeniedGrants: []schemamodel.Grant{deny("SELECT"), deny("SELECT", "secret")},
			},
			wantErr: `SELECT on t is denied to "app" on the whole table, so a DENY on column secret adds nothing; declare one or the other`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemamodel.ValidateDeniedGrants(test.db), qt.ErrorMatches, test.wantErr)
		})
	}
}
