package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

// routineDocument declares one function and one procedure, with the given
// privileges on the function.
func routineDocument(grants, revoked []schemamodel.Grant) *schemamodel.Database {
	return &schemamodel.Database{
		Functions: []schemamodel.Function{
			{Name: "purge", Parameters: "p_id uuid", Returns: "void", Language: "sql", Body: "SELECT 1"},
			{Name: "archive", Kind: "procedure", Parameters: "n integer", Language: "sql", Body: "SELECT 1"},
		},
		Grants:        grants,
		RevokedGrants: revoked,
	}
}

// TestToDBSchema_ImplicitPublicExecute pins the EXECUTE PUBLIC holds on every
// routine a database built from the document would carry. It is what a file
// the current side of a file-to-file comparison stands for: without it a
// document that revokes the privilege plans no REVOKE against one that does not.
func TestToDBSchema_ImplicitPublicExecute(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		db      *schemamodel.Database
		want    []catalog.Grant
	}{
		{
			name:    "every routine on PostgreSQL",
			dialect: platform.Postgres,
			db:      routineDocument(nil, nil),
			want: []catalog.Grant{
				{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "purge", Arguments: "p_id uuid", Implicit: true},
				{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "PROCEDURE", ObjectName: "archive", Arguments: "n integer", Implicit: true},
			},
		},
		{
			name:    "a revoke on the routine takes it away",
			dialect: platform.Postgres,
			db: routineDocument(nil, []schemamodel.Grant{
				{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "purge", RoutineArguments: "uuid"},
			}),
			want: []catalog.Grant{
				{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "PROCEDURE", ObjectName: "archive", Arguments: "n integer", Implicit: true},
			},
		},
		{
			name:    "an explicit grant replaces the implicit row",
			dialect: platform.Postgres,
			db: routineDocument([]schemamodel.Grant{
				{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: "public.purge", RoutineArguments: "uuid", RoutineKind: "FUNCTION"},
			}, []schemamodel.Grant{
				{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "purge", RoutineArguments: "uuid"},
			}),
			want: []catalog.Grant{
				{Role: "app", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "purge", Arguments: "uuid"},
				{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "PROCEDURE", ObjectName: "archive", Arguments: "n integer", Implicit: true},
			},
		},
		{
			name:    "another dialect gives PUBLIC nothing",
			dialect: platform.MySQL,
			db:      routineDocument(nil, nil),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := goschematodb.ToDBSchema(test.db, test.dialect)

			c.Assert(got.Grants, qt.DeepEquals, test.want)
		})
	}
}

// TestCompareSchemas_PlansTheRevokeOfTheImplicitPrivilege drives the public
// path that joins the conversion to the comparator: a document revoking
// PUBLIC's EXECUTE, compared with one that says nothing about it, plans the
// REVOKE, and the same document compared with itself plans nothing.
func TestCompareSchemas_PlansTheRevokeOfTheImplicitPrivilege(t *testing.T) {
	c := qt.New(t)
	revoking := routineDocument(nil, []schemamodel.Grant{
		{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "purge", RoutineArguments: "uuid"},
	})

	diff := schemadiff.CompareSchemas(revoking, routineDocument(nil, nil), platform.Postgres)
	same := schemadiff.CompareSchemas(revoking, revoking, platform.Postgres)

	c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
	c.Assert(diff.GrantsRemoved[0].Role, qt.Equals, "PUBLIC")
	c.Assert(diff.GrantsRemoved[0].ObjectName, qt.Equals, "purge")
	c.Assert(same.GrantsRemoved, qt.HasLen, 0)
	c.Assert(same.GrantsAdded, qt.HasLen, 0)
}
