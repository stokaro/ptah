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

// routineRows is what a catalog read reports for role on routine rt, one row
// per privilege, under TABLE: SQL Server and Oracle grant on a routine with the
// statement they grant on a table, and the readers report it that way.
func routineRows(role string, privileges ...string) []catalog.Grant {
	rows := make([]catalog.Grant, 0, len(privileges))
	for _, privilege := range privileges {
		rows = append(rows, catalog.Grant{Role: role, Privilege: privilege, ObjectType: "TABLE", ObjectName: "rt"})
	}
	return rows
}

// Each routine is rt as a catalog read reports it. collectionRT is an Oracle
// function returning a nested-table collection, whose return type the read
// reports as table.
var (
	procedureRT     = catalog.Function{Name: "rt", Kind: schemamodel.FunctionKindProcedure}
	functionRT      = catalog.Function{Name: "rt", Returns: "int"}
	tableFunctionRT = catalog.Function{Name: "rt", Returns: "table", ReturnsTable: true}
	collectionRT    = catalog.Function{Name: "rt", Returns: "table"}
	elsewhereRT     = catalog.Function{Schema: "sales", Name: "rt", Kind: schemamodel.FunctionKindProcedure}
)

// TestGrantsWithOptions_AllOnARoutineByName pins that a declared ALL on a name
// the catalog reads as a routine compares with the routine's list, on a target
// that grants on a routine by its name. Compared with the table's list, a SQL
// Server or Oracle GRANT ALL on a procedure or a function is never held, and
// a role the schema manages has EXECUTE revoked on every run.
func TestGrantsWithOptions_AllOnARoutineByName(t *testing.T) {
	allOnRT := schemamodel.Grant{Role: "app", Privileges: []string{"ALL"}, OnTable: "rt"}
	grantAll := []plannedGrant{{Role: "app", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "rt"}}
	tests := []struct {
		name        string
		dialect     string
		function    catalog.Function
		database    []catalog.Grant
		wantAdded   []plannedGrant
		wantRemoved []plannedGrant
	}{
		{
			name:     "SQL Server reports EXECUTE for a procedure",
			dialect:  platform.SQLServer,
			function: procedureRT,
			database: routineRows("app", "EXECUTE"),
		},
		{
			name:     "SQL Server reports EXECUTE and REFERENCES for a scalar function",
			dialect:  platform.SQLServer,
			function: functionRT,
			database: routineRows("app", "EXECUTE", "REFERENCES"),
		},
		{
			name:     "SQL Server reports the table's rows for a table-valued function",
			dialect:  platform.SQLServer,
			function: tableFunctionRT,
			database: routineRows("app", sqlserverTable...),
		},
		{
			name:      "a scalar function without REFERENCES plans GRANT ALL",
			dialect:   platform.SQLServer,
			function:  functionRT,
			database:  routineRows("app", "EXECUTE"),
			wantAdded: grantAll,
		},
		{
			name:        "a role the schema manages loses CONTROL on a procedure",
			dialect:     platform.SQLServer,
			function:    procedureRT,
			database:    routineRows("app", "CONTROL", "EXECUTE"),
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "CONTROL", ObjectType: "TABLE", ObjectName: "rt"}},
		},
		{
			name:        "a procedure in another schema leaves the name a table",
			dialect:     platform.SQLServer,
			function:    elsewhereRT,
			database:    routineRows("app", "EXECUTE"),
			wantAdded:   grantAll,
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt"}},
		},
		{
			name:     "Oracle reports DEBUG and EXECUTE for a procedure",
			dialect:  platform.Oracle,
			function: procedureRT,
			database: routineRows("app", "DEBUG", "EXECUTE"),
		},
		{
			name:     "Oracle reports DEBUG and EXECUTE for a function",
			dialect:  platform.Oracle,
			function: functionRT,
			database: routineRows("app", "DEBUG", "EXECUTE"),
		},
		{
			name:     "an Oracle function returning a nested table reports DEBUG and EXECUTE",
			dialect:  platform.Oracle,
			function: collectionRT,
			database: routineRows("app", "DEBUG", "EXECUTE"),
		},
		{
			name:      "an Oracle function without DEBUG plans GRANT ALL",
			dialect:   platform.Oracle,
			function:  functionRT,
			database:  routineRows("app", "EXECUTE"),
			wantAdded: grantAll,
		},
		{
			name:        "PostgreSQL grants on a routine by its kind, so the name stays a table",
			dialect:     platform.Postgres,
			function:    functionRT,
			database:    routineRows("app", "EXECUTE"),
			wantAdded:   grantAll,
			wantRemoved: []plannedGrant{{Role: "app", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, found := platform.Lookup(test.dialect)
			c.Assert(found, qt.IsTrue)
			diff := &difftypes.SchemaDiff{}
			database := &catalog.Database{Grants: test.database, Functions: []catalog.Function{test.function}}

			compare.GrantsWithOptions(new(managedGrant(allOnRT)), database, diff,
				identifier.ForDialect(test.dialect), compare.GrantOptions{AllPrivileges: target.AllPrivileges})

			c.Assert(plannedGrants(diff.GrantsAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(plannedGrants(diff.GrantsRemoved), qt.DeepEquals, test.wantRemoved)
		})
	}
}
