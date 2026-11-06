package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// grantRemovals compares a database holding held with a desired state that holds
// none of it and names no role.
func grantRemovals(database *catalog.Database, desiredComplete bool) []difftypes.GrantRef {
	diff := &difftypes.SchemaDiff{}
	compare.GrantsWithOptions(&schemamodel.Database{}, database, diff, identifier.ForDialect("postgres"),
		compare.GrantOptions{DesiredComplete: desiredComplete})
	return diff.GrantsRemoved
}

// TestGrantsWithOptions_ACompleteDesiredStateRemovesWhatItLeavesOut is a grant on
// only the --from side of two server reads. It is a difference whoever holds it,
// and a role the other side happens not to name is the common case.
func TestGrantsWithOptions_ACompleteDesiredStateRemovesWhatItLeavesOut(t *testing.T) {
	tests := []struct {
		name  string
		grant catalog.Grant
		want  difftypes.GrantRef
	}{
		{
			name:  "a role the desired state does not name",
			grant: catalog.Grant{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
			want:  difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
		},
		{
			name:  "PUBLIC on a table",
			grant: catalog.Grant{Role: "PUBLIC", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
			want:  difftypes.GrantRef{Role: "PUBLIC", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
		},
		{
			name:  "PUBLIC on a schema",
			grant: catalog.Grant{Role: "PUBLIC", Privilege: "CREATE", ObjectType: "SCHEMA", ObjectName: "app"},
			want:  difftypes.GrantRef{Role: "PUBLIC", Privilege: "CREATE", ObjectType: "SCHEMA", ObjectName: "app"},
		},
		{
			name:  "a routine grant to another role than PUBLIC",
			grant: catalog.Grant{Role: "reader", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f"},
			want:  difftypes.GrantRef{Role: "reader", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{Grants: []catalog.Grant{test.grant}}

			c.Assert(grantRemovals(database, true), qt.DeepEquals, []difftypes.GrantRef{test.want})
		})
	}
}

// TestGrantsWithOptions_ACompleteDesiredStateLeavesWhatItHoldsByDefault is a
// privilege the other side holds with no grant saying so. Revoking one to match
// takes it from a side that holds it.
func TestGrantsWithOptions_ACompleteDesiredStateLeavesWhatItHoldsByDefault(t *testing.T) {
	tests := []struct {
		name  string
		grant catalog.Grant
	}{
		{
			name:  "the table's owner",
			grant: catalog.Grant{Role: "app_owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", Implicit: true},
		},
		{
			name:  "the owner of a table in another schema",
			grant: catalog.Grant{Role: "extra_owner", Privilege: "SELECT", ObjectType: "TABLE", Schema: "extra", ObjectName: "t", Implicit: true},
		},
		{
			name:  "the schema's owner",
			grant: catalog.Grant{Role: "schema_owner", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app", Implicit: true},
		},
		{
			name:  "PUBLIC's EXECUTE on a function",
			grant: catalog.Grant{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f"},
		},
		{
			name:  "PUBLIC's EXECUTE on a procedure",
			grant: catalog.Grant{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "PROCEDURE", ObjectName: "p"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{Grants: []catalog.Grant{test.grant}}

			c.Assert(grantRemovals(database, true), qt.HasLen, 0)
		})
	}
}

// TestGrantsWithOptions_ACompleteDesiredStateStillRevokesPublicExecute is the
// read of a routine whose PUBLIC EXECUTE was revoked. The desired state says so
// in a revoked grant, which reaches the row [publicExecuteByDefault] leaves alone.
func TestGrantsWithOptions_ACompleteDesiredStateStillRevokesPublicExecute(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	desired := &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{
		Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineKind: "FUNCTION",
	}}}
	database := &catalog.Database{Grants: []catalog.Grant{
		{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f"},
	}}

	compare.GrantsWithOptions(desired, database, diff, identifier.ForDialect("postgres"), compare.GrantOptions{DesiredComplete: true})

	c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f"},
	})
}

// TestGrantsWithOptions_ADeclarationLeavesARoleItDoesNotManage is the same grant
// against a desired state a person wrote. A file that says nothing about a role
// makes no claim about what the role may do, so its grant stays.
func TestGrantsWithOptions_ADeclarationLeavesARoleItDoesNotManage(t *testing.T) {
	tests := []struct {
		name string
		role string
	}{
		{name: "a role", role: "reader"},
		{name: "PUBLIC", role: "PUBLIC"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{Grants: []catalog.Grant{
				{Role: test.role, Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
			}}

			c.Assert(grantRemovals(database, false), qt.HasLen, 0)
		})
	}
}
