package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestCompareWithDatabaseInfoHoldsTheTargetsGrantAll wires the dialect's own
// ALL lists into the comparison entry point: a role the schema creates and
// grants ALL on orders, holding what the target's GRANT ALL reports, plans
// nothing. Read as PostgreSQL's list, the comparison plans GRANT ALL on MySQL,
// MariaDB, SQL Server and Oracle, and on MySQL, MariaDB and Oracle also
// revokes every privilege PostgreSQL's ALL does not name.
func TestCompareWithDatabaseInfoHoldsTheTargetsGrantAll(t *testing.T) {
	tests := []struct {
		dialect    string
		privileges []string
	}{
		{dialect: "postgres", privileges: []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}},
		{dialect: "mysql", privileges: []string{
			"ALTER", "CREATE", "CREATE VIEW", "DELETE", "DROP", "INDEX",
			"INSERT", "REFERENCES", "SELECT", "SHOW VIEW", "TRIGGER", "UPDATE",
		}},
		{dialect: "mariadb", privileges: []string{
			"ALTER", "CREATE", "CREATE VIEW", "DELETE", "DELETE HISTORY", "DROP", "INDEX",
			"INSERT", "REFERENCES", "SELECT", "SHOW VIEW", "TRIGGER", "UPDATE",
		}},
		{dialect: "sqlserver", privileges: []string{"DELETE", "INSERT", "REFERENCES", "SELECT", "UPDATE"}},
		{dialect: "oracle", privileges: []string{
			"ALTER", "DEBUG", "DELETE", "FLASHBACK", "INSERT", "ON COMMIT REFRESH", "QUERY REWRITE", "READ", "SELECT", "UPDATE",
		}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "reader", Inherit: true}},
				Grants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"ALL"}, OnTable: "orders"}},
			}
			database := &catalog.Database{Roles: []catalog.Role{{Name: "reader", Inherit: true}}}
			for _, privilege := range test.privileges {
				database.Grants = append(database.Grants, catalog.Grant{Role: "reader", Privilege: privilege, ObjectType: "TABLE", ObjectName: "orders"})
			}

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), desired, database, catalog.ServerInfo{Dialect: test.dialect}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfoHoldsAGrantAllOnARoutine drives the comparison
// entry point with a role the schema creates and grants ALL on procedure
// touch, holding what the target's GRANT ALL reports on a procedure, read
// under TABLE as SQL Server and Oracle report it. It plans nothing. Compared
// with the table's list, the comparison plans GRANT ALL and revokes EXECUTE.
func TestCompareWithDatabaseInfoHoldsAGrantAllOnARoutine(t *testing.T) {
	tests := []struct {
		dialect    string
		privileges []string
	}{
		{dialect: "sqlserver", privileges: []string{"EXECUTE"}},
		{dialect: "oracle", privileges: []string{"DEBUG", "EXECUTE"}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			touch := schemamodel.Function{Name: "touch", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN NULL; END;"}
			desired := &schemamodel.Database{
				Roles:     []schemamodel.Role{{Name: "runner", Inherit: true}},
				Functions: []schemamodel.Function{touch},
				Grants:    []schemamodel.Grant{{Role: "runner", Privileges: []string{"ALL"}, OnTable: "touch"}},
			}
			database := &catalog.Database{
				Roles:     []catalog.Role{{Name: "runner", Inherit: true}},
				Functions: []catalog.Function{{Name: "touch", Kind: schemamodel.FunctionKindProcedure, Body: "BEGIN NULL; END;"}},
			}
			for _, privilege := range test.privileges {
				database.Grants = append(database.Grants, catalog.Grant{Role: "runner", Privilege: privilege, ObjectType: "TABLE", ObjectName: "touch"})
			}

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), desired, database, catalog.ServerInfo{Dialect: test.dialect}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
		})
	}
}
