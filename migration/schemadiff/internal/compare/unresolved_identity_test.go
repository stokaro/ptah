package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// unresolvedTargetSemantics is a live target whose catalog resolved nothing the
// desired schema names, which is the ordinary state of a first migration against
// an existing database.
//
// The comparison is ComparisonCatalogResolved because that is what a connection
// supplies; identifier.ForDialect never returns it, which is why this defect
// could not be reached from any offline fixture.
func unresolvedTargetSemantics() identifier.Semantics {
	return identifier.Semantics{
		DefaultSchema: "dbo",
		TableNames:    identifier.ComparisonCatalogResolved,
		ColumnNames:   identifier.ComparisonCatalogResolved,
		IndexNames:    identifier.ComparisonCatalogResolved,
	}
}

// TestGrantsWithSemantics_UnresolvedTablesAreNotOneTable pins that grants on
// two tables the catalog has not resolved stay two grants.
//
// Keying by table identity (stokaro/ptah#1283) fixed a real defect and inherited
// this one: every unresolved name shared a single key, so the second grant
// overwrote the first in the map and one of them never reached the plan
// (stokaro/ptah#1290).
func TestGrantsWithSemantics_UnresolvedTablesAreNotOneTable(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Grants: []goschema.Grant{
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: "alpha"},
			{Role: "app", Privileges: []string{"SELECT"}, OnTable: "beta"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithSemantics(generated, &types.DBSchema{}, diff, unresolvedTargetSemantics())

	granted := make([]string, 0, len(diff.GrantsAdded))
	for _, grant := range diff.GrantsAdded {
		granted = append(granted, grant.ObjectName)
	}
	c.Assert(granted, qt.DeepEquals, []string{"alpha", "beta"})
}

// TestRLSEnabledTablesWithSemantics_UnresolvedTablesAreNotOneTable is the same
// property on the other comparator #1283 moved onto table identity.
func TestRLSEnabledTablesWithSemantics_UnresolvedTablesAreNotOneTable(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: "alpha"},
			{Table: "beta"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSEnabledTablesWithSemantics(
		generated, &types.DBSchema{}, diff, unresolvedTargetSemantics(),
	)

	c.Assert(diff.RLSEnabledTablesAdded, qt.DeepEquals, []string{"alpha", "beta"})
}

// TestConstraintsWithSemantics_UnresolvedTablesAreNotOneTable covers the older
// half of the class.
//
// tableMemberKey has keyed constraints, columns and indexes through the same
// normalization since stokaro/ptah#1232, so the collapse predates #1283 on this
// path. The row is here because a fix proven on grants says nothing about
// constraints, which is how #1290 arrived.
func TestConstraintsWithSemantics_UnresolvedTablesAreNotOneTable(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Alpha", Name: "alpha"},
			{StructName: "Beta", Name: "beta"},
		},
		Constraints: []goschema.Constraint{
			{StructName: "Alpha", Name: "ck", Table: "alpha", Type: "CHECK", CheckExpression: "id > 0"},
			{StructName: "Beta", Name: "ck", Table: "beta", Type: "CHECK", CheckExpression: "id > 0"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.ConstraintsWithSemantics(generated, &types.DBSchema{}, diff, nil, unresolvedTargetSemantics())

	tables := make([]string, 0, len(diff.ConstraintsAddedWithTables))
	for _, constraint := range diff.ConstraintsAddedWithTables {
		tables = append(tables, constraint.TableName)
	}
	c.Assert(tables, qt.DeepEquals, []string{"alpha", "beta"})
}
