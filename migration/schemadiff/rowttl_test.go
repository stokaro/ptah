package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// declaredTTL is a one-table declaration carrying the given policy.
func declaredTTL(spec *ast.RowTTLSpec) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Sessions", Name: "sessions", RowTTL: spec}},
		Fields: []goschema.Field{{StructName: "Sessions", Name: "id", Type: "BIGINT", Primary: true}},
	}
}

// liveTTL is the live description of that table with the given policy.
func liveTTL(spec *ast.RowTTLSpec) *types.DBSchema {
	return &types.DBSchema{
		Tables: []types.DBTable{{
			Name:    "sessions",
			Type:    "BASE TABLE",
			Columns: []types.DBColumn{{Name: "id", DataType: "BIGINT", IsNullable: "NO"}},
			RowTTL:  spec,
		}},
	}
}

// TestCompare_RowTTLTransitions pins what the comparison reports for each
// transition, including the one that has to reach TablesModified with no column
// difference at all.
//
// That last case is the one worth the test: a table whose ONLY difference is its
// retention policy would be dropped by a modified-table condition counting
// columns, and the schema would report as synced while rows expired on a
// schedule nobody declared (stokaro/ptah#1027).
func TestCompare_RowTTLTransitions(t *testing.T) {
	tests := []struct {
		name        string
		desired     *ast.RowTTLSpec
		current     *ast.RowTTLSpec
		wantChanged bool
		wantDesired *ast.RowTTLSpec
		wantCurrent *ast.RowTTLSpec
	}{
		{
			name:        "neither side has a policy",
			desired:     nil,
			current:     nil,
			wantChanged: false,
		},
		{
			name:        "an unchanged policy",
			desired:     &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current:     &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			wantChanged: false,
		},
		{
			name:        "a policy being added",
			desired:     &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current:     nil,
			wantChanged: true,
			wantDesired: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			wantCurrent: nil,
		},
		{
			name:        "a policy being removed",
			desired:     nil,
			current:     &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			wantChanged: true,
			wantDesired: nil,
			wantCurrent: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			name:        "an expression being changed",
			desired:     &ast.RowTTLSpec{ExpirationExpression: "expires_at + INTERVAL '1 hour'"},
			current:     &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			wantChanged: true,
			wantDesired: &ast.RowTTLSpec{ExpirationExpression: "expires_at + INTERVAL '1 hour'"},
			wantCurrent: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			name:        "a knob being dropped while the policy stays",
			desired:     &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current:     &ast.RowTTLSpec{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
			wantChanged: true,
			wantDesired: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			wantCurrent: &ast.RowTTLSpec{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				declaredTTL(test.desired), liveTTL(test.current), platform.CockroachDB)

			change := rowTTLChangeOf(diff)

			c.Assert(change != nil, qt.Equals, test.wantChanged)
			c.Assert(desiredOf(change), qt.DeepEquals, test.wantDesired)
			c.Assert(currentOf(change), qt.DeepEquals, test.wantCurrent)
		})
	}
}

// rowTTLChangeOf returns the TTL transition the comparison reported, or nil.
// These three helpers exist so the assertions above stay data comparisons
// rather than conditionals in a test body.
func rowTTLChangeOf(diff *difftypes.SchemaDiff) *difftypes.RowTTLChange {
	for _, table := range diff.TablesModified {
		if table.RowTTLChange != nil {
			return table.RowTTLChange
		}
	}
	return nil
}

func desiredOf(change *difftypes.RowTTLChange) *ast.RowTTLSpec {
	if change == nil {
		return nil
	}
	return change.Desired
}

func currentOf(change *difftypes.RowTTLChange) *ast.RowTTLSpec {
	if change == nil {
		return nil
	}
	return change.Current
}

// TestCompare_ATTLOnlyDifferenceStillReachesTablesModified is the case the
// modified-table condition would drop if it counted columns alone: the two
// sides agree on every column and differ only in the retention policy.
//
// Without it the schema reports as synced while rows expire on a schedule
// nobody declared, which is the failure stokaro/ptah#1027 names.
func TestCompare_ATTLOnlyDifferenceStillReachesTablesModified(t *testing.T) {
	c := qt.New(t)

	desired := declaredTTL(&ast.RowTTLSpec{ExpirationExpression: "expires_at"})
	current := liveTTL(nil)
	current.Tables[0].Columns = columnsMatching(desired)

	diff := schemadiff.CompareWithDialect(desired, current, platform.CockroachDB)

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsAdded, qt.HasLen, 0)
	c.Assert(diff.TablesModified[0].ColumnsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 0)
	c.Assert(diff.TablesModified[0].RowTTLChange, qt.IsNotNil)
}

// columnsMatching describes the declaration's columns exactly as a read of the
// table it creates would, so a comparison over the two finds no column
// difference at all.
func columnsMatching(_ *goschema.Database) []types.DBColumn {
	return []types.DBColumn{{
		Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true,
	}}
}

// TestCompare_RowTTLChangeCarriesBothSides pins the field the planner depends
// on. `SET` replaces only the parameters it names, so a plan that has to reset a
// dropped knob can only learn which those are from the CURRENT state -- a diff
// carrying the desired policy alone could not produce a correct statement.
func TestCompare_RowTTLChangeCarriesBothSides(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(
		declaredTTL(&ast.RowTTLSpec{ExpirationExpression: "expires_at"}),
		liveTTL(&ast.RowTTLSpec{ExpirationExpression: "expires_at", JobCron: "@daily"}),
		platform.CockroachDB,
	)

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	change := diff.TablesModified[0].RowTTLChange
	c.Assert(change, qt.IsNotNil)
	c.Assert(change.Desired.JobCron, qt.Equals, "")
	c.Assert(change.Current.JobCron, qt.Equals, "@daily")
}

// TestCompare_RowTTLChangeIsIndependentOfTheSchemaItCameFrom pins that the diff
// holds copies. A planner or a caller mutating the change must not reach back
// into the declaration or the live description it was derived from.
func TestCompare_RowTTLChangeIsIndependentOfTheSchemaItCameFrom(t *testing.T) {
	c := qt.New(t)

	desired := &ast.RowTTLSpec{ExpirationExpression: "expires_at"}
	diff := schemadiff.CompareWithDialect(declaredTTL(desired), liveTTL(nil), platform.CockroachDB)

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	change := diff.TablesModified[0].RowTTLChange
	c.Assert(change, qt.IsNotNil)

	change.Desired.ExpirationExpression = "mutated"

	c.Assert(desired.ExpirationExpression, qt.Equals, "expires_at")
}
