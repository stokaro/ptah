package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// plannedGrant is a planned grant as these rows compare it.
type plannedGrant struct {
	Role       string
	Privilege  string
	ObjectType string
	ObjectName string
	Arguments  string
}

func plannedGrants(refs []difftypes.GrantRef) []plannedGrant {
	if len(refs) == 0 {
		return nil
	}
	out := make([]plannedGrant, 0, len(refs))
	for _, ref := range refs {
		out = append(out, plannedGrant{
			Role: ref.Role, Privilege: ref.Privilege, ObjectType: ref.ObjectType,
			ObjectName: ref.ObjectName, Arguments: ref.Arguments,
		})
	}
	return out
}

func publicExecute(name, arguments string) schemamodel.Grant {
	return schemamodel.Grant{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: name, RoutineArguments: arguments}
}

// implicitPublicExecute is the row the PostgreSQL reader reports for a routine
// whose ACL nobody wrote.
func implicitPublicExecute(name, arguments string) catalog.Grant {
	return catalog.Grant{
		Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public",
		ObjectName: name, Arguments: arguments, Implicit: true,
	}
}

// TestGrants_RevokedGrants pins what a revoked grant plans. A revoke asserts
// the privilege is absent whoever holds it, so it reaches grantees the
// managed-role removal never looks at -- PUBLIC, and a role the schema does
// not declare -- and a privilege nobody granted, such as the EXECUTE PUBLIC
// holds on every new function.
func TestGrants_RevokedGrants(t *testing.T) {
	tests := []struct {
		name        string
		desired     schemamodel.Database
		database    catalog.Database
		wantAdded   []plannedGrant
		wantRemoved []plannedGrant
	}{
		{
			name: "the implicit PUBLIC EXECUTE on an existing function is revoked",
			desired: schemamodel.Database{
				RevokedGrants: []schemamodel.Grant{publicExecute("purge", "uuid")},
			},
			database: catalog.Database{Grants: []catalog.Grant{implicitPublicExecute("purge", "p_id uuid")}},
			wantRemoved: []plannedGrant{{
				Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "public.purge", Arguments: "p_id uuid",
			}},
		},
		{
			name: "a function that no longer holds it plans nothing",
			desired: schemamodel.Database{
				RevokedGrants: []schemamodel.Grant{publicExecute("purge", "uuid")},
			},
			database: catalog.Database{Grants: []catalog.Grant{{
				Role: "owner", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public",
				ObjectName: "purge", Arguments: "p_id uuid",
			}}},
		},
		{
			name: "another overload is not the revoked routine",
			desired: schemamodel.Database{
				RevokedGrants: []schemamodel.Grant{publicExecute("purge", "uuid")},
			},
			database: catalog.Database{Grants: []catalog.Grant{implicitPublicExecute("purge", "p_id text")}},
		},
		{
			name: "a table privilege an unmanaged role holds through default privileges",
			desired: schemamodel.Database{
				RevokedGrants: []schemamodel.Grant{{Role: "wpmgr_app", Privileges: []string{"DELETE"}, OnTable: "plugin_signatures"}},
			},
			database: catalog.Database{Grants: []catalog.Grant{
				{Role: "wpmgr_app", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "plugin_signatures"},
				{Role: "wpmgr_app", Privilege: "DELETE", ObjectType: "TABLE", Schema: "public", ObjectName: "plugin_signatures"},
			}},
			wantRemoved: []plannedGrant{{
				Role: "wpmgr_app", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "public.plugin_signatures",
			}},
		},
		{
			name: "a function the plan creates is revoked in the same plan",
			desired: schemamodel.Database{
				Functions:     []schemamodel.Function{{Name: "purge", Parameters: "p_id uuid"}},
				RevokedGrants: []schemamodel.Grant{publicExecute("purge", "uuid")},
			},
			wantRemoved: []plannedGrant{{
				Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "purge", Arguments: "uuid",
			}},
		},
		{
			name: "a table the plan creates is revoked in the same plan",
			desired: schemamodel.Database{
				Tables:        []schemamodel.Table{{Name: "plugin_signatures"}},
				RevokedGrants: []schemamodel.Grant{{Role: "wpmgr_app", Privileges: []string{"DELETE"}, OnTable: "plugin_signatures"}},
			},
			wantRemoved: []plannedGrant{{
				Role: "wpmgr_app", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "plugin_signatures",
			}},
		},
		{
			name: "an object neither side has plans nothing",
			desired: schemamodel.Database{
				RevokedGrants: []schemamodel.Grant{publicExecute("purge", "uuid")},
			},
		},
		{
			name: "a routine grant matches the catalog row whatever the argument spelling",
			desired: schemamodel.Database{
				Grants: []schemamodel.Grant{{
					Role: "wpmgr_app", Privileges: []string{"EXECUTE"}, OnRoutine: "purge",
					RoutineArguments: "UUID", RoutineKind: "ROUTINE",
				}},
			},
			database: catalog.Database{Grants: []catalog.Grant{{
				Role: "wpmgr_app", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public",
				ObjectName: "purge", Arguments: "p_id uuid",
			}}},
		},
		{
			name: "a routine grant the database lacks is added with its arguments",
			desired: schemamodel.Database{
				Grants: []schemamodel.Grant{{Role: "wpmgr_app", Privileges: []string{"EXECUTE"}, OnRoutine: "purge", RoutineArguments: "uuid"}},
			},
			database: catalog.Database{Grants: []catalog.Grant{implicitPublicExecute("purge", "p_id uuid")}},
			wantAdded: []plannedGrant{{
				Role: "wpmgr_app", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "purge", Arguments: "uuid",
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(&test.desired, &test.database, diff, identifier.ForDialect(platform.Postgres))

			c.Assert(plannedGrants(diff.GrantsAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(plannedGrants(diff.GrantsRemoved), qt.DeepEquals, test.wantRemoved)
		})
	}
}
