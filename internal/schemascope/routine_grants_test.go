package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemascope"
)

// TestFilterScopesRoutineGrantsOnBothSidesTheSameWay pins that a privilege on
// a routine rides the routine's schema on both sides of a comparison, and that
// a revoked grant is scoped with the grants. A privilege kept on one side and
// dropped on the other reads as one to grant or to revoke.
func TestFilterScopesRoutineGrantsOnBothSidesTheSameWay(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: "app.purge", RoutineArguments: "uuid"},
			{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: "other.purge", RoutineArguments: "uuid"},
		},
		RevokedGrants: []schemamodel.Grant{
			{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "app.purge", RoutineArguments: "uuid"},
			{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "other.purge", RoutineArguments: "uuid"},
		},
	}
	database := &catalog.Database{Grants: []catalog.Grant{
		{Role: "app", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "app", ObjectName: "purge", Arguments: "p uuid"},
		{Role: "app", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "other", ObjectName: "purge", Arguments: "p uuid"},
	}}

	generated := schemascope.FilterGenerated(desired, []string{"app"})
	read := schemascope.FilterDatabase(database, []string{"app"})

	c.Assert(generated.Grants, qt.DeepEquals, desired.Grants[:1])
	c.Assert(generated.RevokedGrants, qt.DeepEquals, desired.RevokedGrants[:1])
	c.Assert(read.Grants, qt.DeepEquals, database.Grants[:1])
}
