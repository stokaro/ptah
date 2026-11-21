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
