package schemadiff_test

import (
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// driftedSchemas builds a generated/database schema pair where every
// "modified" diff category has at least two entries: modified tables (with
// both added and modified columns), modified enums, modified functions,
// modified RLS policies, and added constraints. All source collections are
// declared out of alphabetical order so ordering must come from sorting, not
// from input order.
func driftedSchemas() (*goschema.Database, *types.DBSchema) {
	tableNames := []struct {
		table      string
		structName string
	}{
		{"locations", "Location"},
		{"users", "User"},
		{"areas", "Area"},
		{"tenants", "Tenant"},
	}

	gen := &goschema.Database{}
	db := &types.DBSchema{}

	for _, tbl := range tableNames {
		gen.Tables = append(gen.Tables, goschema.Table{Name: tbl.table, StructName: tbl.structName})
		gen.Fields = append(gen.Fields,
			goschema.Field{StructName: tbl.structName, Name: "id", Type: "TEXT", Primary: true},
			// Nullable in the database but NOT NULL in the generated schema -> ColumnsModified.
			goschema.Field{StructName: tbl.structName, Name: "flag", Type: "TEXT"},
			// Missing from the database -> ColumnsAdded.
			goschema.Field{StructName: tbl.structName, Name: "extra", Type: "TEXT", Nullable: true},
		)
		db.Tables = append(db.Tables, types.DBTable{
			Name: tbl.table,
			Columns: []types.DBColumn{
				{Name: "id", DataType: "text", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "flag", DataType: "text", IsNullable: "YES"},
			},
		})
		// Present only in the generated schema -> ConstraintsAdded.
		gen.Constraints = append(gen.Constraints, goschema.Constraint{
			StructName:      tbl.structName,
			Name:            tbl.table + "_flag_check",
			Type:            "CHECK",
			Table:           tbl.table,
			CheckExpression: "flag <> ''",
		})
	}

	for _, enumName := range []string{"enum_status", "enum_kind", "enum_area_type"} {
		gen.Enums = append(gen.Enums, goschema.Enum{Name: enumName, Values: []string{"a", "b", "c"}})
		// Missing value "c" -> EnumsModified.
		db.Enums = append(db.Enums, types.DBEnum{Name: enumName, Values: []string{"a", "b"}})
	}

	for _, fnName := range []string{"set_tenant_context", "get_current_tenant_id", "audit_row"} {
		gen.Functions = append(gen.Functions, goschema.Function{
			Name: fnName, Returns: "TEXT", Language: "plpgsql", Body: "BEGIN RETURN 'x'; END;",
		})
		// Different return type -> FunctionsModified.
		db.Functions = append(db.Functions, types.DBFunction{
			Name: fnName, Returns: "VOID", Language: "plpgsql", Body: "BEGIN RETURN 'x'; END;",
		})
	}

	for _, tbl := range []string{"users", "tenants", "areas"} {
		policyName := tbl + "_tenant_isolation"
		gen.RLSPolicies = append(gen.RLSPolicies, goschema.RLSPolicy{
			Name: policyName, Table: tbl, PolicyFor: "ALL", ToRoles: "app_role",
			UsingExpression: "tenant_id = get_current_tenant_id()",
		})
		// Different FOR clause -> RLSPoliciesModified.
		db.RLSPolicies = append(db.RLSPolicies, types.DBRLSPolicy{
			Name: policyName, Table: tbl, PolicyFor: "SELECT", ToRoles: "app_role",
			UsingExpression: "tenant_id = get_current_tenant_id()",
		})
	}

	return gen, db
}

// TestCompare_Deterministic guards against issue #59: comparing the same
// schema pair repeatedly must produce an identical diff. The compare layer
// builds its lists while ranging over maps, so every list — including the
// *Modified ones — must be sorted before being returned.
func TestCompare_Deterministic(t *testing.T) {
	c := qt.New(t)

	gen, db := driftedSchemas()
	first := schemadiff.Compare(gen, db)

	// Sanity-check that the fixtures actually exercise every category this
	// test is guarding, so a fixture regression can't silently hollow it out.
	c.Assert(len(first.TablesModified) > 1, qt.IsTrue)
	c.Assert(len(first.TablesModified[0].ColumnsModified) > 0, qt.IsTrue)
	c.Assert(len(first.EnumsModified) > 1, qt.IsTrue)
	c.Assert(len(first.FunctionsModified) > 1, qt.IsTrue)
	c.Assert(len(first.RLSPoliciesModified) > 1, qt.IsTrue)
	c.Assert(len(first.ConstraintsAdded) > 1, qt.IsTrue)
	c.Assert(len(first.ConstraintsAddedWithTables) > 1, qt.IsTrue)

	for i := range 100 {
		c.Assert(schemadiff.Compare(gen, db), qt.DeepEquals, first,
			qt.Commentf("iteration %d produced a different diff", i))
	}
}

// TestCompare_ModifiedListsSorted pins the ordering contract: all diff lists
// come out sorted by their natural key, not merely stable.
func TestCompare_ModifiedListsSorted(t *testing.T) {
	c := qt.New(t)

	gen, db := driftedSchemas()
	diff := schemadiff.Compare(gen, db)

	c.Assert(sort.SliceIsSorted(diff.TablesModified, func(i, j int) bool {
		return diff.TablesModified[i].TableName < diff.TablesModified[j].TableName
	}), qt.IsTrue)
	c.Assert(sort.SliceIsSorted(diff.EnumsModified, func(i, j int) bool {
		return diff.EnumsModified[i].EnumName < diff.EnumsModified[j].EnumName
	}), qt.IsTrue)
	c.Assert(sort.SliceIsSorted(diff.FunctionsModified, func(i, j int) bool {
		return diff.FunctionsModified[i].FunctionName < diff.FunctionsModified[j].FunctionName
	}), qt.IsTrue)
	c.Assert(sort.SliceIsSorted(diff.RLSPoliciesModified, func(i, j int) bool {
		return diff.RLSPoliciesModified[i].PolicyName < diff.RLSPoliciesModified[j].PolicyName
	}), qt.IsTrue)
	c.Assert(sort.StringsAreSorted(diff.ConstraintsAdded), qt.IsTrue)
	c.Assert(sort.SliceIsSorted(diff.ConstraintsAddedWithTables, func(i, j int) bool {
		a, b := diff.ConstraintsAddedWithTables[i], diff.ConstraintsAddedWithTables[j]
		if a.TableName != b.TableName {
			return a.TableName < b.TableName
		}
		return a.Name < b.Name
	}), qt.IsTrue)

	for _, tableDiff := range diff.TablesModified {
		c.Assert(sort.SliceIsSorted(tableDiff.ColumnsModified, func(i, j int) bool {
			return tableDiff.ColumnsModified[i].ColumnName < tableDiff.ColumnsModified[j].ColumnName
		}), qt.IsTrue)
	}
}
