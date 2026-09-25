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

// tableRows is what a catalog read reports for role on public.t, one row per
// privilege.
func tableRows(role string, withOption bool, privileges ...string) []catalog.Grant {
	rows := make([]catalog.Grant, 0, len(privileges))
	for _, privilege := range privileges {
		rows = append(rows, catalog.Grant{
			Role: role, Privilege: privilege, ObjectType: "TABLE", Schema: "public", ObjectName: "t", WithOption: withOption,
		})
	}
	return rows
}

// portableTable is what GRANT ALL on a table reports on PostgreSQL 16, and
// fullTable what it reports on PostgreSQL 17 and later.
var (
	portableTable = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}
	fullTable     = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}
)

// TestGrants_All pins how a declared ALL compares with a catalog read, which
// reports GRANT ALL as one row per privilege. Without the rule a declared ALL
// matched no row: it was planned again on every run, and for a role the schema
// manages every row it stood for was planned for removal (stokaro/ptah#3579).
func TestGrants_All(t *testing.T) {
	tests := []struct {
		name               string
		desired            schemamodel.Database
		database           []catalog.Grant
		wantAdded          []plannedGrant
		wantRemoved        []plannedGrant
		wantOptionsAdded   int
		wantOptionsRevoked int
	}{
		{
			name:     "PostgreSQL 16 reports seven rows",
			desired:  schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}}},
			database: tableRows("app", false, portableTable...),
		},
		{
			name: "PostgreSQL 17 reports eight, and a managed role keeps all of them",
			desired: schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "app"}},
				Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}},
			},
			database: tableRows("app", false, fullTable...),
		},
		{
			name:      "a missing privilege plans GRANT ALL",
			desired:   schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}}},
			database:  tableRows("app", false, "SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "TRIGGER"),
			wantAdded: []plannedGrant{{Role: "app", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "t"}},
		},
		{
			name:     "a file's ALL row matches a file's ALL",
			desired:  schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}}},
			database: tableRows("app", false, "ALL"),
		},
		{
			name:    "ALL on a schema",
			desired: schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnSchema: "app"}}},
			database: []catalog.Grant{
				{Role: "app", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app"},
				{Role: "app", Privilege: "CREATE", ObjectType: "SCHEMA", ObjectName: "app"},
			},
		},
		{
			name: "ALL on a function",
			desired: schemamodel.Database{Grants: []schemamodel.Grant{{
				Role: "app", Privileges: []string{"ALL"}, OnRoutine: "f", RoutineArguments: "uuid",
			}}},
			database: []catalog.Grant{{
				Role: "app", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "f", Arguments: "p uuid",
			}},
		},
		{
			name: "a privilege ALL does not name is still removed from a managed role",
			desired: schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "app"}},
				Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnSchema: "app"}},
			},
			database: []catalog.Grant{
				{Role: "app", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app"},
				{Role: "app", Privilege: "CREATE", ObjectType: "SCHEMA", ObjectName: "app"},
				{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "t"},
			},
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "public.t"}},
		},
		{
			name: "ALL with the option against rows without it",
			desired: schemamodel.Database{Grants: []schemamodel.Grant{{
				Role: "app", Privileges: []string{"ALL"}, OnTable: "t", WithOption: true,
			}}},
			database:         tableRows("app", false, portableTable...),
			wantOptionsAdded: 1,
		},
		{
			name: "ALL without the option takes it from every row of a managed role",
			desired: schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "app"}},
				Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}},
			},
			database:           tableRows("app", true, portableTable...),
			wantOptionsRevoked: 7,
		},
		{
			name: "a revoked ALL removes each row the database reports",
			desired: schemamodel.Database{RevokedGrants: []schemamodel.Grant{{
				Role: "app", Privileges: []string{"ALL"}, OnTable: "t",
			}}},
			database: tableRows("app", false, "SELECT", "MAINTAIN"),
			wantRemoved: []plannedGrant{
				{Role: "app", Privilege: "MAINTAIN", ObjectType: "TABLE", ObjectName: "public.t"},
				{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "public.t"},
			},
		},
		{
			name: "every table privilege revoked on a created table is one REVOKE ALL",
			desired: schemamodel.Database{
				Tables:        []schemamodel.Table{{Name: "t"}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: fullTable, OnTable: "t"}},
			},
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "t"}},
		},
		{
			name: "some table privileges revoked on a created table are written out",
			desired: schemamodel.Database{
				Tables:        []schemamodel.Table{{Name: "t"}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"DELETE", "TRUNCATE"}, OnTable: "t"}},
			},
			wantRemoved: []plannedGrant{
				{Role: "app", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "t"},
				{Role: "app", Privilege: "TRUNCATE", ObjectType: "TABLE", ObjectName: "t"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(&test.desired, &catalog.Database{Grants: test.database}, diff,
				identifier.ForDialect(platform.Postgres))

			c.Assert(plannedGrants(diff.GrantsAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(plannedGrants(diff.GrantsRemoved), qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.GrantOptionsAdded, qt.HasLen, test.wantOptionsAdded)
			c.Assert(diff.GrantOptionsRevoked, qt.HasLen, test.wantOptionsRevoked)
		})
	}
}
