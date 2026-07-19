package planner_test

import (
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	dbtypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// multiTenantRLSSchema builds an RLS-heavy schema of the shape reported in
// issue #59: several tables, each with a row-level-security policy. Table
// names are deliberately declared out of alphabetical order so that any
// order-sensitive code path has to sort rather than rely on input order.
func multiTenantRLSSchema() *goschema.Database {
	tables := []struct {
		table      string
		structName string
	}{
		{"locations", "Location"},
		{"users", "User"},
		{"areas", "Area"},
		{"tenants", "Tenant"},
		{"files", "File"},
		{"commodities", "Commodity"},
	}

	db := &goschema.Database{}
	for _, tbl := range tables {
		db.Tables = append(db.Tables, goschema.Table{Name: tbl.table, StructName: tbl.structName})
		db.Fields = append(db.Fields,
			goschema.Field{StructName: tbl.structName, Name: "id", Type: "TEXT", Primary: true},
			goschema.Field{StructName: tbl.structName, Name: "tenant_id", Type: "TEXT"},
		)
		db.RLSPolicies = append(db.RLSPolicies, goschema.RLSPolicy{
			StructName:      tbl.structName,
			Name:            tbl.table + "_tenant_isolation",
			Table:           tbl.table,
			PolicyFor:       "ALL",
			ToRoles:         "app_role",
			UsingExpression: "tenant_id = get_current_tenant_id()",
		})
		db.RLSEnabledTables = append(db.RLSEnabledTables, goschema.RLSEnabledTable{
			StructName: tbl.structName,
			Table:      tbl.table,
		})
	}
	db.Roles = append(db.Roles,
		goschema.Role{Name: "app_role", Login: true, CreateDB: true, Inherit: true},
		goschema.Role{Name: "readonly_role", Login: true, CreateRole: true, Inherit: true},
	)
	return db
}

// driftedDatabase builds a database-side schema that has drifted from the
// generated one in every order-sensitive way at once: columns with 2+ changed
// properties (the "Modify column" comment ranges over the Changes map), roles
// with 2+ changed attributes (ALTER ROLE operation order), removed policies
// (the disable-RLS warning comments) and removed constraints.
func driftedDatabase(gen *goschema.Database) *dbtypes.DBSchema {
	db := &dbtypes.DBSchema{}
	for _, tbl := range gen.Tables {
		db.Tables = append(db.Tables, dbtypes.DBTable{
			Name: tbl.Name,
			Columns: []dbtypes.DBColumn{
				{Name: "id", DataType: "text", IsNullable: "NO", IsPrimaryKey: true},
				// Generated side declares TEXT NOT NULL -> both a type and a
				// nullability change on the same column.
				{Name: "tenant_id", DataType: "varchar", IsNullable: "YES"},
			},
		})
		// Present only in the database -> ConstraintsRemoved.
		db.Constraints = append(db.Constraints, dbtypes.DBConstraint{
			Name:      "obsolete_" + tbl.Name + "_check",
			TableName: tbl.Name,
			Type:      "CHECK",
		})
		// Present only in the database -> RLSPoliciesRemoved (drives the
		// disable-RLS warning comments in the postgres planner).
		db.RLSPolicies = append(db.RLSPolicies, dbtypes.DBRLSPolicy{
			Name:  tbl.Name + "_zombie_policy",
			Table: tbl.Name,
		})
	}
	// Both roles drift in 2+ attributes -> multi-entry RoleDiff.Changes.
	db.Roles = append(db.Roles,
		dbtypes.DBRole{Name: "app_role", Login: false, CreateDB: false, Inherit: true},
		dbtypes.DBRole{Name: "readonly_role", Login: false, CreateRole: false, Inherit: true},
	)
	return db
}

// TestGenerateSchemaDiffSQL_Deterministic guards against issue #59: planning
// the same schema diff repeatedly must produce byte-identical SQL. Go
// randomizes map iteration order, so any statement emitted while ranging over
// a map (the ENABLE ROW LEVEL SECURITY statements, the "Modify column"
// comments built from colDiff.Changes, the ALTER ROLE operations built from
// roleDiff.Changes) permutes between runs unless the keys are sorted first.
func TestGenerateSchemaDiffSQL_Deterministic(t *testing.T) {
	gen := multiTenantRLSSchema()

	scenarios := []struct {
		name string
		db   *dbtypes.DBSchema
	}{
		{name: "fresh database", db: &dbtypes.DBSchema{}},
		{name: "drifted database", db: driftedDatabase(gen)},
	}

	for _, dialect := range []string{"postgres", "mysql", "mariadb"} {
		for _, scenario := range scenarios {
			t.Run(dialect+"/"+scenario.name, func(t *testing.T) {
				c := qt.New(t)

				first, err := planner.GenerateSchemaDiffSQL(schemadiff.CompareWithDialect(gen, scenario.db, dialect), gen, dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(first, qt.Not(qt.Equals), "")

				for i := range 100 {
					sql, err := planner.GenerateSchemaDiffSQL(schemadiff.CompareWithDialect(gen, scenario.db, dialect), gen, dialect)
					c.Assert(err, qt.IsNil)
					c.Assert(sql, qt.Equals, first, qt.Commentf("iteration %d produced different SQL", i))
				}
			})
		}
	}
}

// TestGenerateSchemaDiffSQL_DriftedFixtureCoverage pins that the drifted
// fixture really exercises the order-sensitive paths the determinism test is
// guarding: a multi-property column change and multi-attribute role changes.
// Without this, a fixture regression could hollow the test out silently.
func TestGenerateSchemaDiffSQL_DriftedFixtureCoverage(t *testing.T) {
	c := qt.New(t)

	gen := multiTenantRLSSchema()
	diff := schemadiff.CompareWithDialect(gen, driftedDatabase(gen), "postgres")

	multiChangeColumns := 0
	for _, tableDiff := range diff.TablesModified {
		for _, colDiff := range tableDiff.ColumnsModified {
			if len(colDiff.Changes) >= 2 {
				multiChangeColumns++
			}
		}
	}
	c.Assert(multiChangeColumns > 1, qt.IsTrue,
		qt.Commentf("fixture must produce 2+ columns with 2+ changes each, got %d", multiChangeColumns))

	multiChangeRoles := 0
	for _, roleDiff := range diff.RolesModified {
		if len(roleDiff.Changes) >= 2 {
			multiChangeRoles++
		}
	}
	c.Assert(multiChangeRoles > 1, qt.IsTrue,
		qt.Commentf("fixture must produce 2+ roles with 2+ changes each, got %d", multiChangeRoles))

	c.Assert(len(diff.RLSPoliciesRemoved) > 1, qt.IsTrue)
	c.Assert(len(diff.ConstraintsRemoved) > 1, qt.IsTrue)
}

// TestGenerateSchemaDiffSQL_EnableRLSSorted pins the ENABLE ROW LEVEL
// SECURITY statements to alphabetical table order — a stronger guarantee than
// run-to-run stability alone.
func TestGenerateSchemaDiffSQL_EnableRLSSorted(t *testing.T) {
	c := qt.New(t)

	gen := multiTenantRLSSchema()
	sql, err := planner.GenerateSchemaDiffSQL(schemadiff.Compare(gen, &dbtypes.DBSchema{}), gen, "postgres")
	c.Assert(err, qt.IsNil)

	var enableStmts []string
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.Contains(line, "ENABLE ROW LEVEL SECURITY") {
			enableStmts = append(enableStmts, line)
		}
	}
	c.Assert(enableStmts, qt.HasLen, len(gen.Tables))
	c.Assert(sort.StringsAreSorted(enableStmts), qt.IsTrue,
		qt.Commentf("ENABLE RLS statements not in sorted table order:\n%s", strings.Join(enableStmts, "\n")))
}
