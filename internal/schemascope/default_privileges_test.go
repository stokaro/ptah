package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemascope"
)

// The two fixtures hold the same three default privileges, one per side of a
// comparison. Each row sits in a different schema and carries a different object
// type, so the object type identifies which schema's row survived -- a default
// privilege has no name of its own, and its schema is exactly what a schema
// allow-list decides about.
func generatedDefaultPrivilegeFixture() *schemamodel.Database {
	return &schemamodel.Database{
		DefaultPrivileges: []schemamodel.DefaultPrivilege{
			{
				StructName: "AuthDefaults", Grantor: "app_owner", Schema: "auth",
				ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
			{
				StructName: "BillingDefaults", Grantor: "app_owner", Schema: "billing",
				ObjectType: "SEQUENCES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
			},
			{
				StructName: "ConnectedDefaults", Grantor: "app_owner",
				ObjectType: "FUNCTIONS", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "EXECUTE"}},
			},
		},
	}
}

func databaseDefaultPrivilegeFixture() *catalog.Database {
	return &catalog.Database{
		DefaultPrivileges: []catalog.DefaultPrivilege{
			{
				Grantor: "app_owner", Schema: "auth", ObjectType: "TABLES",
				Grantee: "app_reader", Privilege: "SELECT",
			},
			{
				Grantor: "app_owner", Schema: "billing", ObjectType: "SEQUENCES",
				Grantee: "app_reader", Privilege: "USAGE",
			},
			{
				Grantor: "app_owner", ObjectType: "FUNCTIONS",
				Grantee: "app_reader", Privilege: "EXECUTE",
			},
		},
	}
}

func generatedDefaultPrivilegeTypes(privileges []schemamodel.DefaultPrivilege) []string {
	types := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		types = append(types, privilege.ObjectType)
	}
	return types
}

func databaseDefaultPrivilegeTypes(privileges []catalog.DefaultPrivilege) []string {
	types := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		types = append(types, privilege.ObjectType)
	}
	return types
}

// TestFilterScopesDefaultPrivilegesOnBothSidesTheSameWay drives both
// projections from one row, because a schema scope that keeps a default
// privilege on one side and drops it on the other invents work: kept live and
// dropped desired reads as a privilege to revoke, and the other way round as one
// to grant. Neither is something the operator asked for by naming a schema.
//
// Without the keep lines the projection carries every schema's defaults through
// unfiltered, which is not a failure anywhere: `--schema auth` then describes
// billing's defaults as part of auth.
func TestFilterScopesDefaultPrivilegesOnBothSidesTheSameWay(t *testing.T) {
	tests := []struct {
		name          string
		schemas       []string
		defaultSchema string
		want          []string
	}{
		{
			name:    "one named schema keeps its own defaults",
			schemas: []string{"auth"},
			want:    []string{"TABLES"},
		},
		{
			name:    "two named schemas keep both",
			schemas: []string{"auth", "billing"},
			want:    []string{"TABLES", "SEQUENCES"},
		},
		{
			name:          "the connected schema claims the unqualified row",
			schemas:       []string{"public"},
			defaultSchema: "public",
			want:          []string{"FUNCTIONS"},
		},
		{
			name:    "with no default schema the unqualified row belongs to no named schema",
			schemas: []string{"public"},
			want:    make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			gotGenerated := schemascope.FilterGeneratedWithDefaultSchema(
				generatedDefaultPrivilegeFixture(), test.schemas, test.defaultSchema)
			gotDatabase := schemascope.FilterDatabaseWithDefaultSchema(
				databaseDefaultPrivilegeFixture(), test.schemas, test.defaultSchema)

			c.Assert(generatedDefaultPrivilegeTypes(gotGenerated.DefaultPrivileges),
				qt.DeepEquals, test.want)
			c.Assert(databaseDefaultPrivilegeTypes(gotDatabase.DefaultPrivileges),
				qt.DeepEquals, test.want)
		})
	}
}

// TestFilterWithoutSchemasLeavesDefaultPrivilegesAlone is the control on the
// table above: an empty allow-list is not a narrow scope, it is no scope, and a
// projection that dropped the family there would satisfy every row that expects
// something gone.
func TestFilterWithoutSchemasLeavesDefaultPrivilegesAlone(t *testing.T) {
	c := qt.New(t)

	gotGenerated := schemascope.FilterGenerated(generatedDefaultPrivilegeFixture(), nil)
	gotDatabase := schemascope.FilterDatabase(databaseDefaultPrivilegeFixture(), nil)

	c.Assert(generatedDefaultPrivilegeTypes(gotGenerated.DefaultPrivileges),
		qt.DeepEquals, []string{"TABLES", "SEQUENCES", "FUNCTIONS"})
	c.Assert(databaseDefaultPrivilegeTypes(gotDatabase.DefaultPrivileges),
		qt.DeepEquals, []string{"TABLES", "SEQUENCES", "FUNCTIONS"})
}
