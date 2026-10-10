package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/synonym"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateSchemaDiffSQLStatements_PlansSynonymChanges drives a synonym
// change through both hosts that have the owner. On Oracle the host plans
// feature changes for the synonym owner alone; on SQL Server the alias's
// schema is created first, as it is for a table, because CREATE SYNONYM in a
// schema that does not exist answers Msg 2760.
func TestGenerateSchemaDiffSQLStatements_PlansSynonymChanges(t *testing.T) {
	retarget := &synonym.Change{
		Before: &synonym.ObservedSynonym{Synonym: synonym.Synonym{Schema: "app", Name: "orders", Target: "sales.orders"}},
		After:  &synonym.DesiredSynonym{Synonym: synonym.Synonym{Schema: "app", Name: "orders", Target: "sales.orders_v2"}},
	}
	created := &synonym.Change{After: retarget.After}
	tests := []struct {
		name    string
		dialect string
		change  *synonym.Change
		want    []string
	}{
		{name: "an Oracle retarget", dialect: platform.Oracle, change: retarget,
			want: []string{"DROP SYNONYM IF EXISTS app.orders", "CREATE SYNONYM app.orders FOR sales.orders_v2"}},
		{name: "a SQL Server synonym in its own schema", dialect: platform.SQLServer, change: created,
			want: []string{"IF SCHEMA_ID('app') IS NULL\n    EXEC('CREATE SCHEMA [app]')", "CREATE SYNONYM [app].[orders] FOR [sales].[orders_v2]"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: retarget.After.Ref(), Value: test.change}}}

			statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), builtintest.Runtime(), diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
