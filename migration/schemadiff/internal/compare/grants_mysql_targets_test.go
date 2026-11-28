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

// mysqlShop is the semantics of a MySQL session that selected the database
// shop, as the MySQL connector states it.
func mysqlShop() identifier.Semantics {
	semantics := identifier.ForDialect(platform.MySQL)
	semantics.DefaultSchema = "shop"
	return semantics
}

// mysqlGrantOptions are the options the schema comparison passes for MySQL.
func mysqlGrantOptions() compare.GrantOptions {
	target, _ := platform.Lookup(platform.MySQL)
	return compare.GrantOptions{
		AllPrivileges:     target.AllPrivileges,
		RoutineNamesFold:  target.Comparison.RoutineNamesFold,
		RoutineKindsApart: target.Grammar.Statements.RoutineGrantsByName,
	}
}

// TestGrantsWithOptions_MySQLDatabaseGrant pins that a grant on the database
// itself, `GRANT ... ON *`, is the grant the read reports on the database the
// session selected, by its name or, after [portablegrants.Relative], as a
// grant on the database itself. Keyed apart, a declared grant on the database
// is planned on every run and the held one revoked by every plan. A grant on
// another database stays another grant.
func TestGrantsWithOptions_MySQLDatabaseGrant(t *testing.T) {
	onDatabase := schemamodel.Grant{Role: "app", Privileges: []string{"INSERT"}, OnDatabase: true}
	tests := []struct {
		name        string
		desired     schemamodel.Database
		database    []catalog.Grant
		wantAdded   []plannedGrant
		wantRemoved []plannedGrant
	}{
		{
			name:     "the read names the database",
			desired:  managedGrant(onDatabase),
			database: []catalog.Grant{{Role: "app", Privilege: "INSERT", ObjectType: "SCHEMA", ObjectName: "shop"}},
		},
		{
			name:     "the read names the database the session selected",
			desired:  managedGrant(onDatabase),
			database: []catalog.Grant{{Role: "app", Privilege: "INSERT", ObjectType: "DATABASE"}},
		},
		{
			name:        "a grant on another database",
			desired:     managedGrant(onDatabase),
			database:    []catalog.Grant{{Role: "app", Privilege: "INSERT", ObjectType: "SCHEMA", ObjectName: "other"}},
			wantAdded:   []plannedGrant{{Role: "app", Privilege: "INSERT", ObjectType: "DATABASE"}},
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "INSERT", ObjectType: "SCHEMA", ObjectName: "other"}},
		},
		{
			name:        "a held grant the schema leaves out",
			desired:     schemamodel.Database{Roles: []schemamodel.Role{{Name: "app"}}},
			database:    []catalog.Grant{{Role: "app", Privilege: "INSERT", ObjectType: "DATABASE"}},
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "INSERT", ObjectType: "DATABASE"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			database := &catalog.Database{
				CurrentDatabase: "shop", Roles: []catalog.Role{{Name: "app"}}, Grants: test.database,
			}

			compare.GrantsWithOptions(&test.desired, database, diff, mysqlShop(), mysqlGrantOptions())

			c.Assert(plannedGrants(diff.GrantsAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(plannedGrants(diff.GrantsRemoved), qt.DeepEquals, test.wantRemoved)
		})
	}
}

// TestGrantsWithOptions_MySQLRoutineGrant pins how a grant on a routine
// compares on MySQL. A routine name compares without case, so a grant that
// spells the function f1 as F1 is the grant the read reports on f1. A function
// and a procedure of one name are two routines, so a grant on one is not a
// grant on the other. ALL on a routine is EXECUTE and ALTER ROUTINE.
func TestGrantsWithOptions_MySQLRoutineGrant(t *testing.T) {
	executeOn := func(kind, name string) schemamodel.Grant {
		return schemamodel.Grant{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: name, RoutineKind: kind}
	}
	tests := []struct {
		name        string
		desired     schemamodel.Database
		database    []catalog.Grant
		wantAdded   []plannedGrant
		wantRemoved []plannedGrant
	}{
		{
			name:     "another case of the function",
			desired:  managedGrant(executeOn("FUNCTION", "F1")),
			database: []catalog.Grant{{Role: "app", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "shop", ObjectName: "f1"}},
		},
		{
			name:        "the procedure of the function's name",
			desired:     managedGrant(executeOn("FUNCTION", "x")),
			database:    []catalog.Grant{{Role: "app", Privilege: "EXECUTE", ObjectType: "PROCEDURE", Schema: "shop", ObjectName: "x"}},
			wantAdded:   []plannedGrant{{Role: "app", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "x"}},
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "EXECUTE", ObjectType: "PROCEDURE", ObjectName: "shop.x"}},
		},
		{
			name: "ALL on a procedure",
			desired: managedGrant(schemamodel.Grant{
				Role: "app", Privileges: []string{"ALL"}, OnRoutine: "x", RoutineKind: "PROCEDURE",
			}),
			database: []catalog.Grant{
				{Role: "app", Privilege: "ALTER ROUTINE", ObjectType: "PROCEDURE", Schema: "shop", ObjectName: "x"},
				{Role: "app", Privilege: "EXECUTE", ObjectType: "PROCEDURE", Schema: "shop", ObjectName: "x"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			database := &catalog.Database{
				CurrentDatabase: "shop",
				Roles:           []catalog.Role{{Name: "app"}},
				Functions: []catalog.Function{
					{Name: "f1", Returns: "int"},
					{Name: "x", Returns: "int"},
					{Name: "x", Kind: schemamodel.FunctionKindProcedure},
				},
				Grants: test.database,
			}

			compare.GrantsWithOptions(&test.desired, database, diff, mysqlShop(), mysqlGrantOptions())

			c.Assert(plannedGrants(diff.GrantsAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(plannedGrants(diff.GrantsRemoved), qt.DeepEquals, test.wantRemoved)
		})
	}
}
