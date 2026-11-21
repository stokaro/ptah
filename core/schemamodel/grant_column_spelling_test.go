package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// TestValidateGrantColumnSpellings_HappyPath pins the declarations that name a
// column one way, or two columns, or keep the spellings apart by role, object
// or dialect: PostgreSQL compares a column name byte for byte, so Name and
// name are two columns there.
func TestValidateGrantColumnSpellings_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		db      *schemamodel.Database
		dialect string
	}{
		{name: "nil database", db: nil, dialect: "mysql"},
		{
			name: "one spelling",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"name"}},
				{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"name", "id"}},
			}},
			dialect: "mysql",
		},
		{
			name: "two roles",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"Name"}},
				{Role: "writer", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"name"}},
			}},
			dialect: "mariadb",
		},
		{
			name: "a grant and a revoke spelled alike",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"name"}}},
				RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"name"}}},
			},
			dialect: "mysql",
		},
		{
			name: "two revokes in two spellings",
			db: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"Name"}},
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"name"}},
			}},
			dialect: "mysql",
		},
		{
			name: "a revoke of other privileges in another spelling",
			db: &schemamodel.Database{
				Grants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"name"}}},
				RevokedGrants: []schemamodel.Grant{
					{Role: "reader", Privileges: []string{"UPDATE", "INSERT"}, OnTable: "t", Columns: []string{"Name"}},
				},
			},
			dialect: "mariadb",
		},
		{
			name: "a revoke from another role",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"Name"}}},
				RevokedGrants: []schemamodel.Grant{{Role: "writer", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"name"}}},
			},
			dialect: "mysql",
		},
		{
			name: "two tables",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"Name"}},
				{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "u", Columns: []string{"name"}},
			}},
			dialect: "mysql",
		},
		{
			name: "postgres keeps the spellings apart",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"Name"}},
				{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"name"}},
			}},
			dialect: "postgres",
		},
		{
			name: "the dialect scope keeps them apart",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"Name"}, Dialects: []string{"postgres"}},
				{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"name"}},
			}},
			dialect: "mysql",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemamodel.ValidateGrantColumnSpellings(test.db, test.dialect), qt.IsNil)
		})
	}
}

// TestValidateGrantColumnSpellings_FailurePath pins the refusals: one role, one
// object and one column of it spelled two ways, on a target that takes the two
// for one column.
func TestValidateGrantColumnSpellings_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		db      *schemamodel.Database
		dialect string
		wantErr string
	}{
		{
			name: "two grants, on mysql",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"Name"}},
				{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"name"}},
			}},
			dialect: "mysql",
			wantErr: `invalid schema diff: role "reader" is granted privileges on one column of TABLE t as "Name" and as "name"; ` +
				`MYSQL compares column names without case, so the two are one column, and a privilege granted under one ` +
				`spelling and revoked under the other stays in effect -- spell the column one way`,
		},
		{
			name: "one grant, on mariadb",
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"NAME", "Name"}},
			}},
			dialect: "mariadb",
			wantErr: `invalid schema diff: role "reader" is granted privileges on one column of TABLE t as "NAME" and as "Name"; ` +
				`MARIADB compares column names without case.*`,
		},
		{
			name: "a grant and a revoke, on mysql",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"Name"}}},
				RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"name"}}},
			},
			dialect: "mysql",
			wantErr: `invalid schema diff: role "reader" is granted SELECT on one column of TABLE t as "Name" and has it revoked as "name"; ` +
				`MYSQL compares column names without case, so the two name one privilege the declaration both grants and ` +
				`revokes -- declare one or the other`,
		},
		{
			name: "a grant and a revoke of every privilege, on mariadb",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "reader", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"name"}}},
				RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"ALL"}, OnTable: "t", Columns: []string{"NAME"}}},
			},
			dialect: "mariadb",
			wantErr: `invalid schema diff: role "reader" is granted UPDATE on one column of TABLE t as "name" and has it revoked as "NAME"; ` +
				`MARIADB compares column names without case.*`,
		},
		{
			name: "a grant of every privilege and a revoke, on mysql",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "reader", Privileges: []string{"ALL"}, OnTable: "t", Columns: []string{"Name"}}},
				RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "t", Columns: []string{"name"}}},
			},
			dialect: "mysql",
			wantErr: `invalid schema diff: role "reader" is granted INSERT on one column of TABLE t as "Name" and has it revoked as "name"; ` +
				`MYSQL compares column names without case.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemamodel.ValidateGrantColumnSpellings(test.db, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
