package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// TestValidateGrantOptionConsistency_HappyPath pins the declarations that
// never reach a real conflict: a dialect where the grant option is per
// privilege, disjoint objects, disjoint roles, agreeing declarations, or
// dialect scopes that keep the two grants from ever landing on the same
// target together.
func TestValidateGrantOptionConsistency_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		db      *schemamodel.Database
		dialect string
	}{
		{name: "nil database", db: nil, dialect: "mysql"},
		{
			name: "postgres keeps the grant option per privilege",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t"},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "t", WithOption: true},
			}},
			dialect: "postgres",
		},
		{
			name: "different tables",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "customers", WithOption: true},
			}},
			dialect: "mysql",
		},
		{
			name: "different roles",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t"},
				{Role: "writer", Privileges: []string{"INSERT"}, OnTable: "t", WithOption: true},
			}},
			dialect: "mariadb",
		},
		{
			name: "both declarations want the option",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", WithOption: true},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "t", WithOption: true},
			}},
			dialect: "mysql",
		},
		{
			name: "neither declaration wants the option",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t"},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "t"},
			}},
			dialect: "mysql",
		},
		{
			name: "disjoint dialect scopes never reach the same target",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Dialects: []string{"postgres"}},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "t", WithOption: true, Dialects: []string{"mysql"}},
			}},
			dialect: "mysql",
		},
		{
			name: "mariadb-only and mysql-only declarations never share a target",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Dialects: []string{"mariadb"}},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "t", WithOption: true, Dialects: []string{"mysql"}},
			}},
			dialect: "mariadb",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemamodel.ValidateGrantOptionConsistency(test.db, test.dialect), qt.IsNil)
		})
	}
}

// TestValidateGrantOptionConsistency_FailurePath pins the refusals: a role and
// object where one declaration asks for WITH GRANT OPTION and another does
// not, on a target where MySQL and MariaDB keep exactly one grant option per
// object rather than per privilege (stokaro/ptah-operator#481).
func TestValidateGrantOptionConsistency_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		db      *schemamodel.Database
		dialect string
		wantErr string
	}{
		{
			name: "a table, on mysql",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "orders", WithOption: true},
			}},
			dialect: "mysql",
			wantErr: `invalid schema diff: role "reader" is granted INSERT on TABLE orders WITH GRANT OPTION and SELECT on the same ` +
				`object without it; MYSQL keeps the grant option at the whole object, not at one privilege, so ` +
				`the two declarations can never both hold -- declare WITH GRANT OPTION for every grant on this ` +
				`object, or for none of them`,
		},
		{
			name: "a table, on mariadb, names MariaDB in the message",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "orders", WithOption: true},
			}},
			dialect: "mariadb",
			wantErr: `invalid schema diff: role "reader" is granted INSERT on TABLE orders WITH GRANT OPTION and SELECT on the same ` +
				`object without it; MARIADB keeps the grant option at the whole object, not at one privilege, ` +
				`so the two declarations can never both hold -- declare WITH GRANT OPTION for every grant on ` +
				`this object, or for none of them`,
		},
		{
			name: "a schema",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "app", WithOption: true},
				{Role: "reader", Privileges: []string{"INSERT"}, OnSchema: "app"},
			}},
			dialect: "mysql",
			wantErr: `invalid schema diff: role "reader" is granted SELECT on SCHEMA app WITH GRANT OPTION and INSERT on the same ` +
				`object without it;.*`,
		},
		{
			name: "column-scoped grants share the whole table's grant option",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders", Columns: []string{"total"}, WithOption: true},
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders", Columns: []string{"status"}},
			}},
			dialect: "mysql",
			wantErr: `invalid schema diff: role "reader" is granted SELECT on TABLE orders WITH GRANT OPTION and SELECT on the same ` +
				`object without it;.*`,
		},
		{
			name: "a table-wide grant and a column grant on it disagree too",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
				{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "orders", Columns: []string{"status"}, WithOption: true},
			}},
			dialect: "mysql",
			wantErr: `invalid schema diff: role "reader" is granted UPDATE on TABLE orders WITH GRANT OPTION and SELECT on the same ` +
				`object without it;.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemamodel.ValidateGrantOptionConsistency(test.db, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
