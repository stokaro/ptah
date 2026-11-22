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

// Each list is what GRANT ALL reports on a target, one row per privilege.
var (
	mysqlTable = []string{
		"ALTER", "CREATE", "CREATE VIEW", "DELETE", "DROP", "INDEX",
		"INSERT", "REFERENCES", "SELECT", "SHOW VIEW", "TRIGGER", "UPDATE",
	}
	mysqlDatabase = append([]string{
		"ALTER ROUTINE", "CREATE ROUTINE", "CREATE TEMPORARY TABLES", "EVENT", "EXECUTE", "LOCK TABLES",
	}, mysqlTable...)
	mariadbTable          = append([]string{"DELETE HISTORY"}, mysqlTable...)
	mariadb1011Database   = append([]string{"DELETE HISTORY"}, mysqlDatabase...)
	mariadbNewestDatabase = append([]string{"SHOW CREATE ROUTINE"}, mariadb1011Database...)
	sqlserverTable        = []string{"DELETE", "INSERT", "REFERENCES", "SELECT", "UPDATE"}
	oracleTable           = []string{
		"ALTER", "DEBUG", "DELETE", "FLASHBACK", "INSERT", "ON COMMIT REFRESH", "QUERY REWRITE", "READ", "SELECT", "UPDATE",
	}
	oracleView = []string{
		"DEBUG", "DELETE", "FLASHBACK", "INSERT", "MERGE VIEW", "ON COMMIT REFRESH", "QUERY REWRITE", "READ", "SELECT", "UPDATE",
	}
)

// unqualifiedRows is what a catalog read reports for role on table t, one row
// per privilege, with no schema, as the declaration names t.
func unqualifiedRows(role string, privileges ...string) []catalog.Grant {
	rows := tableRows(role, false, privileges...)
	for i := range rows {
		rows[i].Schema = ""
	}
	return rows
}

// schemaRows is what a catalog read reports for role on the schema or
// database app, one row per privilege.
func schemaRows(role string, privileges ...string) []catalog.Grant {
	rows := make([]catalog.Grant, 0, len(privileges))
	for _, privilege := range privileges {
		rows = append(rows, catalog.Grant{Role: role, Privilege: privilege, ObjectType: "SCHEMA", ObjectName: "app"})
	}
	return rows
}

// sequenceRows is what a catalog read reports for role on sequence s, one
// row per privilege.
func sequenceRows(role string, privileges ...string) []catalog.Grant {
	rows := make([]catalog.Grant, 0, len(privileges))
	for _, privilege := range privileges {
		rows = append(rows, catalog.Grant{Role: role, Privilege: privilege, ObjectType: "SEQUENCE", ObjectName: "s"})
	}
	return rows
}

// Each grant gives app ALL on table t, on schema app or on sequence s.
var (
	allOnTable    = schemamodel.Grant{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}
	allOnSchema   = schemamodel.Grant{Role: "app", Privileges: []string{"ALL"}, OnSchema: "app"}
	allOnSequence = schemamodel.Grant{Role: "app", Privileges: []string{"ALL"}, OnSequence: "s"}
)

// managedGrant is a schema that creates app and declares grant.
func managedGrant(grant schemamodel.Grant) schemamodel.Database {
	return schemamodel.Database{Roles: []schemamodel.Role{{Name: "app"}}, Grants: []schemamodel.Grant{grant}}
}

// TestGrantsWithOptions_AllOnTarget pins that a declared ALL compares with the
// rows the target's own GRANT ALL reports. Read as PostgreSQL's list, a MySQL,
// MariaDB, SQL Server or Oracle GRANT ALL is never held, so it is planned on
// every run, and a role the schema manages loses every privilege PostgreSQL's
// ALL does not name.
func TestGrantsWithOptions_AllOnTarget(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		desired     schemamodel.Database
		database    []catalog.Grant
		wantAdded   []plannedGrant
		wantRemoved []plannedGrant
	}{
		{
			name:     "MySQL reports twelve rows for a table",
			dialect:  platform.MySQL,
			desired:  schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}}},
			database: unqualifiedRows("app", mysqlTable...),
		},
		{
			name:     "a role the schema manages keeps every MySQL table privilege",
			dialect:  platform.MySQL,
			desired:  managedGrant(allOnTable),
			database: unqualifiedRows("app", mysqlTable...),
		},
		{
			name:     "a role the schema manages keeps every MySQL database privilege",
			dialect:  platform.MySQL,
			desired:  managedGrant(allOnSchema),
			database: schemaRows("app", mysqlDatabase...),
		},
		{
			name:    "a missing MySQL privilege plans GRANT ALL",
			dialect: platform.MySQL,
			desired: schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}}},
			database: unqualifiedRows("app",
				"ALTER", "CREATE", "CREATE VIEW", "DELETE", "DROP", "INDEX", "INSERT", "REFERENCES", "SELECT", "TRIGGER", "UPDATE",
			),
			wantAdded: []plannedGrant{{Role: "app", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "t"}},
		},
		{
			name:      "MariaDB's ALL on a table includes DELETE HISTORY",
			dialect:   platform.MariaDB,
			desired:   schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}}},
			database:  unqualifiedRows("app", mysqlTable...),
			wantAdded: []plannedGrant{{Role: "app", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "t"}},
		},
		{
			name:     "a role the schema manages keeps every MariaDB table privilege",
			dialect:  platform.MariaDB,
			desired:  managedGrant(allOnTable),
			database: unqualifiedRows("app", mariadbTable...),
		},
		{
			name:     "MariaDB 10.11 holds ALL on a database without SHOW CREATE ROUTINE",
			dialect:  platform.MariaDB,
			desired:  managedGrant(allOnSchema),
			database: schemaRows("app", mariadb1011Database...),
		},
		{
			name:     "a role the schema manages keeps SHOW CREATE ROUTINE on MariaDB 11.8",
			dialect:  platform.MariaDB,
			desired:  managedGrant(allOnSchema),
			database: schemaRows("app", mariadbNewestDatabase...),
		},
		{
			name:     "SQL Server reports five rows for a table",
			dialect:  platform.SQLServer,
			desired:  schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}}},
			database: unqualifiedRows("app", sqlserverTable...),
		},
		{
			name:        "a role the schema manages loses CONTROL, which SQL Server's ALL does not grant",
			dialect:     platform.SQLServer,
			desired:     managedGrant(allOnTable),
			database:    unqualifiedRows("app", append([]string{"CONTROL"}, sqlserverTable...)...),
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "CONTROL", ObjectType: "TABLE", ObjectName: "t"}},
		},
		{
			name:     "Oracle reports ten rows for a table",
			dialect:  platform.Oracle,
			desired:  schemamodel.Database{Grants: []schemamodel.Grant{allOnTable}},
			database: unqualifiedRows("app", oracleTable...),
		},
		{
			name:     "Oracle reports MERGE VIEW in place of ALTER for a view, read as a table",
			dialect:  platform.Oracle,
			desired:  schemamodel.Database{Grants: []schemamodel.Grant{allOnTable}},
			database: unqualifiedRows("app", oracleView...),
		},
		{
			name:     "a role the schema manages keeps MERGE VIEW on an Oracle view",
			dialect:  platform.Oracle,
			desired:  managedGrant(allOnTable),
			database: unqualifiedRows("app", oracleView...),
		},
		{
			name:    "a missing Oracle privilege plans GRANT ALL",
			dialect: platform.Oracle,
			desired: schemamodel.Database{Grants: []schemamodel.Grant{allOnTable}},
			database: unqualifiedRows("app",
				"ALTER", "DEBUG", "DELETE", "FLASHBACK", "INSERT", "ON COMMIT REFRESH", "QUERY REWRITE", "SELECT", "UPDATE",
			),
			wantAdded: []plannedGrant{{Role: "app", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "t"}},
		},
		{
			name:     "Oracle reports two rows for a sequence",
			dialect:  platform.Oracle,
			desired:  managedGrant(allOnSequence),
			database: sequenceRows("app", "ALTER", "SELECT"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, found := platform.Lookup(test.dialect)
			c.Assert(found, qt.IsTrue)
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithOptions(&test.desired, &catalog.Database{Grants: test.database}, diff,
				identifier.ForDialect(test.dialect), compare.GrantOptions{AllPrivileges: target.AllPrivileges})

			c.Assert(plannedGrants(diff.GrantsAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(plannedGrants(diff.GrantsRemoved), qt.DeepEquals, test.wantRemoved)
		})
	}
}
