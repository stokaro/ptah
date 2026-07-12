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
	return db
}

// TestGenerateSchemaDiffSQL_Deterministic guards against issue #59: planning
// the same schema diff repeatedly must produce byte-identical SQL. Go
// randomizes map iteration order, so any statement emitted while ranging over
// a map (historically the ENABLE ROW LEVEL SECURITY statements in the
// postgres planner) permutes between runs unless the keys are sorted first.
func TestGenerateSchemaDiffSQL_Deterministic(t *testing.T) {
	c := qt.New(t)

	gen := multiTenantRLSSchema()

	first := planner.GenerateSchemaDiffSQL(schemadiff.Compare(gen, &dbtypes.DBSchema{}), gen, "postgres")
	c.Assert(first, qt.Contains, "ENABLE ROW LEVEL SECURITY")

	for i := range 100 {
		sql := planner.GenerateSchemaDiffSQL(schemadiff.Compare(gen, &dbtypes.DBSchema{}), gen, "postgres")
		c.Assert(sql, qt.Equals, first, qt.Commentf("iteration %d produced different SQL", i))
	}
}

// TestGenerateSchemaDiffSQL_EnableRLSSorted pins the ENABLE ROW LEVEL
// SECURITY statements to alphabetical table order — a stronger guarantee than
// run-to-run stability alone.
func TestGenerateSchemaDiffSQL_EnableRLSSorted(t *testing.T) {
	c := qt.New(t)

	gen := multiTenantRLSSchema()
	sql := planner.GenerateSchemaDiffSQL(schemadiff.Compare(gen, &dbtypes.DBSchema{}), gen, "postgres")

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
