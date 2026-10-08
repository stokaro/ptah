package schemadiff_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// oracleSlotsCatalog is table slots as the Oracle reader reports it: names
// upper-case, the primary key under the name Oracle made up, and both keys
// deferring as given.
func oracleSlotsCatalog(deferrable bool, initially string) *catalog.Database {
	key := func(name, kind, column string) catalog.Constraint {
		return catalog.Constraint{
			Name: name, TableName: "SLOTS", Type: kind, ColumnNames: []string{column}, ColumnName: column,
			Deferrable: deferrable, Initially: initially,
		}
	}
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "SLOTS", Columns: []catalog.Column{
			{Name: "ID", DataType: "NUMBER(10)", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "POS", DataType: "NUMBER(10)", IsNullable: "YES"},
			{Name: "R", DataType: "NUMBER(10)", IsNullable: "YES"},
		}}},
		Constraints: []catalog.Constraint{key("SYS_C008735", "PRIMARY KEY", "ID"), key("SLOTS_POS_KEY", "UNIQUE", "POS")},
	}
}

// TestCompare_OracleKeyDeferral_Synced compares the keys with the catalog
// Oracle reports for them: nothing is planned. The columns compare as Oracle
// resolves names, so `id` is ID; compared as text, each key was dropped and
// added on every plan.
func TestCompare_OracleKeyDeferral_Synced(t *testing.T) {
	c := qt.New(t)
	desired := deferralDesired(true, "deferred")
	desired.Constraints = desired.Constraints[:1]

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, oracleSlotsCatalog(true, "deferred"), platform.Oracle, must.Must(builtin.New())))

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

// TestCompare_OracleKeyDeferral_Changed plans keys that start deferring as a
// drop and an add, each written with its clause.
func TestCompare_OracleKeyDeferral_Changed(t *testing.T) {
	c := qt.New(t)
	desired := deferralDesired(true, "deferred")
	desired.Constraints = desired.Constraints[:1]
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, oracleSlotsCatalog(false, ""), platform.Oracle, must.Must(builtin.New())))

	plan, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Oracle, planner.Options{Capabilities: capability.ForDialect(platform.Oracle)},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(plan, "\n"), qt.Contains, "ADD PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED")
	c.Assert(strings.Join(plan, "\n"), qt.Contains, "ADD CONSTRAINT slots_pos_key UNIQUE (pos) DEFERRABLE INITIALLY DEFERRED")
}
