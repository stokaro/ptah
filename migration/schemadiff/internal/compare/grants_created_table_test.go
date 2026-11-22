package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// createsT2 is a schema that creates table t2 and revokes revoked on it.
func createsT2(revoked schemamodel.Grant) schemamodel.Database {
	return schemamodel.Database{Tables: []schemamodel.Table{{Name: "t2"}}, RevokedGrants: []schemamodel.Grant{revoked}}
}

// readNoWideGrants is a read that establishes the grants on every table and
// found none, holding grants.
func readNoWideGrants(grants ...catalog.Grant) catalog.Database {
	return catalog.Database{Grants: grants, WideGrants: make([]catalog.WideGrant, 0)}
}

// TestGrantsWithOptions_TableRevokesTakeOnlyTableGrants pins that a revoke on
// a table the plan creates is left out only where it has nothing to take. On
// MySQL, MariaDB and Oracle a REVOKE on a table takes only a grant on that
// table and the server refuses one where there is none, so planning it fails
// the apply. A grant on the database, on every table or to PUBLIC keeps the
// privilege, and a read that does not say whether one exists cannot tell, so
// the revoke is planned there and the server answers for the role.
func TestGrantsWithOptions_TableRevokesTakeOnlyTableGrants(t *testing.T) {
	selectOnT2 := schemamodel.Grant{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t2"}
	selectOnDatabase := catalog.Grant{Role: "app", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "d1"}
	revokedSelect := []plannedGrant{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t2"}}
	declaresApp := createsT2(selectOnT2)
	declaresApp.Roles = []schemamodel.Role{{Name: "app"}}
	tests := []struct {
		name        string
		dialect     string
		onlyOnTable bool
		desired     schemamodel.Database
		database    catalog.Database
		wantRemoved []plannedGrant
	}{
		{
			name:        "a created table nothing else covers plans no revoke",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(selectOnT2),
			database:    readNoWideGrants(),
		},
		{
			name:        "nor does a column of one",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(schemamodel.Grant{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t2", Columns: []string{"id"}}),
			database:    readNoWideGrants(),
		},
		{
			name:        "a database grant of another privilege covers nothing",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(selectOnT2),
			database:    readNoWideGrants(catalog.Grant{Role: "app", Privilege: "INSERT", ObjectType: "SCHEMA", ObjectName: "d1"}),
		},
		{
			name:        "nor does a database grant to another role",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(selectOnT2),
			database:    readNoWideGrants(catalog.Grant{Role: "other", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "d1"}),
		},
		{
			name:        "nor a database grant the plan removes from a role the schema declares",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     declaresApp,
			database:    readNoWideGrants(selectOnDatabase),
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "d1"}},
		},
		{
			name:        "a read that does not say which grants cover every table plans the revoke",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(selectOnT2),
			wantRemoved: revokedSelect,
		},
		{
			name:        "a grant on the database keeps the privilege and plans the revoke",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(selectOnT2),
			database:    readNoWideGrants(selectOnDatabase),
			wantRemoved: revokedSelect,
		},
		{
			name:        "so does one to PUBLIC",
			dialect:     platform.MariaDB,
			onlyOnTable: true,
			desired:     createsT2(selectOnT2),
			database:    readNoWideGrants(catalog.Grant{Role: "PUBLIC", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "d1"}),
			wantRemoved: revokedSelect,
		},
		{
			name:        "so does a grant on every table",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(selectOnT2),
			database: catalog.Database{WideGrants: []catalog.WideGrant{
				{Role: "app", Privilege: "SELECT", Grant: "SELECT ON *.*"},
			}},
			wantRemoved: revokedSelect,
		},
		{
			name:        "and a revoke of ALL is kept by any privilege on the database",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(schemamodel.Grant{Role: "app", Privileges: []string{"ALL"}, OnTable: "t2"}),
			database:    readNoWideGrants(selectOnDatabase),
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "t2"}},
		},
		{
			name:        "a privilege the read reports under the created table's name is revoked",
			dialect:     platform.MySQL,
			onlyOnTable: true,
			desired:     createsT2(selectOnT2),
			database:    readNoWideGrants(catalog.Grant{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t2"}),
			wantRemoved: revokedSelect,
		},
		{
			name:        "a routine the plan creates is revoked all the same",
			dialect:     platform.Postgres,
			onlyOnTable: true,
			desired: schemamodel.Database{
				Functions:     []schemamodel.Function{{Name: "purge", Parameters: "p_id uuid"}},
				RevokedGrants: []schemamodel.Grant{publicExecute("purge", "uuid")},
			},
			database:    readNoWideGrants(),
			wantRemoved: []plannedGrant{{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "purge", Arguments: "uuid"}},
		},
		{
			name:        "a target where a revoke takes more than a table grant plans the revoke",
			dialect:     platform.Postgres,
			desired:     createsT2(selectOnT2),
			database:    readNoWideGrants(),
			wantRemoved: revokedSelect,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithOptions(&test.desired, &test.database, diff,
				identifier.ForDialect(test.dialect), compare.GrantOptions{TableRevokesTakeOnlyTableGrants: test.onlyOnTable})

			c.Assert(plannedGrants(diff.GrantsAdded), qt.IsNil)
			c.Assert(plannedGrants(diff.GrantsRemoved), qt.DeepEquals, test.wantRemoved)
		})
	}
}
