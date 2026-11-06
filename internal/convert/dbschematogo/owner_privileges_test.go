package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_AnOwnersDefaultPrivilegeIsNotDescribed is what an
// object's owner holds because it owns the object. No statement wrote it and a new
// object has it, so a description states neither a grant nor a revoke for it.
func TestConvertDBSchemaToGoSchema_AnOwnersDefaultPrivilegeIsNotDescribed(t *testing.T) {
	c := qt.New(t)
	db := convertOwnerPrivileges(c, &catalog.Database{Grants: []catalog.Grant{
		{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", Implicit: true},
		{Role: "owner", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "s", Implicit: true},
		{Role: "owner", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app", Implicit: true},
	}})

	c.Assert(db.Grants, qt.HasLen, 0)
	c.Assert(db.RevokedGrants, qt.HasLen, 0)
}

// TestConvertDBSchemaToGoSchema_AnOwnersRevokedPrivilegeIsARevoke keeps what an
// owner gave up. Without the revoke a description replays the object with its owner
// holding the privilege, and a comparison with a database where it holds the
// privilege has nothing to plan.
func TestConvertDBSchemaToGoSchema_AnOwnersRevokedPrivilegeIsARevoke(t *testing.T) {
	tests := []struct {
		name  string
		read  catalog.Database
		grant catalog.Grant
		want  schemamodel.Grant
	}{
		{
			name:  "a table",
			grant: catalog.Grant{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
			want:  schemamodel.Grant{Role: "owner", Privileges: []string{"SELECT"}, OnTable: "t"},
		},
		{
			name:  "a table in another schema",
			grant: catalog.Grant{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", Schema: "app", ObjectName: "t"},
			want:  schemamodel.Grant{Role: "owner", Privileges: []string{"SELECT"}, OnTable: "app.t"},
		},
		{
			name:  "a sequence",
			grant: catalog.Grant{Role: "owner", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "s"},
			want:  schemamodel.Grant{Role: "owner", Privileges: []string{"USAGE"}, OnSequence: "s"},
		},
		{
			name: "a sequence a column owns, under the name the replay gives it",
			read: catalog.Database{Tables: []catalog.Table{{
				Name:    "products",
				Columns: []catalog.Column{{Name: "id", OwnedSequence: "items_id_seq"}},
			}}},
			grant: catalog.Grant{Role: "owner", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "items_id_seq"},
			want:  schemamodel.Grant{Role: "owner", Privileges: []string{"USAGE"}, OnSequence: "products_id_seq"},
		},
		{
			name:  "a schema",
			grant: catalog.Grant{Role: "owner", Privilege: "CREATE", ObjectType: "SCHEMA", ObjectName: "app"},
			want:  schemamodel.Grant{Role: "owner", Privileges: []string{"CREATE"}, OnSchema: "app"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			read := test.read
			test.grant.Implicit, test.grant.IsPartialRevoke = true, true
			read.Grants = []catalog.Grant{test.grant}

			db := convertOwnerPrivileges(c, &read)

			c.Assert(db.Grants, qt.HasLen, 0)
			c.Assert(db.RevokedGrants, qt.HasLen, 1)
			c.Assert(db.RevokedGrants[0].Role, qt.Equals, test.want.Role)
			c.Assert(db.RevokedGrants[0].Privileges, qt.DeepEquals, test.want.Privileges)
			c.Assert(db.RevokedGrants[0].OnTable, qt.Equals, test.want.OnTable)
			c.Assert(db.RevokedGrants[0].OnSequence, qt.Equals, test.want.OnSequence)
			c.Assert(db.RevokedGrants[0].OnSchema, qt.Equals, test.want.OnSchema)
		})
	}
}

func convertOwnerPrivileges(c *qt.C, read *catalog.Database) *schemamodel.Database {
	c.Helper()
	db, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), read, platform.Postgres, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return db
}
