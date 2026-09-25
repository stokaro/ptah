package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_RoutineGrants pins how a catalog read of
// routine privileges is described. An implicit row is the privilege a routine
// holds because it exists, which its own declaration already implies, so it is
// left out. A routine whose ACL was written and holds no PUBLIC EXECUTE had it
// revoked, and the description says so: without the revoked grant, a database
// built from the description would give the privilege back.
func TestConvertDBSchemaToGoSchema_RoutineGrants(t *testing.T) {
	tests := []struct {
		name        string
		rows        []catalog.Grant
		wantGrants  []schemamodel.Grant
		wantRevoked []schemamodel.Grant
	}{
		{
			name: "a routine nobody granted on is described by nothing",
			rows: []catalog.Grant{{
				Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public",
				ObjectName: "purge", Arguments: "p_id uuid", Implicit: true,
			}},
			wantGrants:  make([]schemamodel.Grant, 0),
			wantRevoked: make([]schemamodel.Grant, 0),
		},
		{
			name: "the wpmgr shape: the app role granted, PUBLIC revoked",
			rows: []catalog.Grant{
				{Role: "owner", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "purge", Arguments: "p_id uuid"},
				{Role: "wpmgr_app", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "purge", Arguments: "p_id uuid"},
			},
			wantGrants: []schemamodel.Grant{
				{Role: "owner", Privileges: []string{"EXECUTE"}, OnRoutine: "public.purge", RoutineArguments: "p_id uuid", RoutineKind: "FUNCTION"},
				{Role: "wpmgr_app", Privileges: []string{"EXECUTE"}, OnRoutine: "public.purge", RoutineArguments: "p_id uuid", RoutineKind: "FUNCTION"},
			},
			wantRevoked: []schemamodel.Grant{
				{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "public.purge", RoutineArguments: "p_id uuid", RoutineKind: "FUNCTION"},
			},
		},
		{
			name: "a written ACL that still grants PUBLIC revokes nothing",
			rows: []catalog.Grant{
				{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "PROCEDURE", Schema: "public", ObjectName: "archive", Arguments: ""},
			},
			wantGrants: []schemamodel.Grant{
				{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "public.archive", RoutineKind: "PROCEDURE"},
			},
			wantRevoked: make([]schemamodel.Grant, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db := dbschematogo.ConvertDBSchemaToGoSchema(&catalog.Database{Grants: test.rows}, platform.Postgres)

			c.Assert(db.Grants, qt.DeepEquals, test.wantGrants)
			c.Assert(db.RevokedGrants, qt.DeepEquals, test.wantRevoked)
		})
	}
}
