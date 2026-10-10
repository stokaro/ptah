package mysqlplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlplan"
	"ptah.run/dialect/mysql/mysqlschema"
)

// Every parent action gets a receipt that plans nothing of its own: a column
// definition writes the settings and DROP TABLE takes them away.
func TestColumnService_AccountsForEveryParentAction(t *testing.T) {
	table := objectidentity.NewBuilder(identifier.ForDialect("mysql")).TableParts("", "docs")
	tests := []struct {
		action   featureplan.ParentAction
		strategy string
	}{
		{action: featureplan.CreateTable, strategy: "each column definition writes its declared settings"},
		{action: featureplan.AlterTable, strategy: "a column keeps the settings it holds; a column the plan rewrites is written with its declared settings"},
		{action: featureplan.DropTable, strategy: "remove the settings with the table"},
		{action: featureplan.RebuildTable, strategy: "the rebuilt table's column definitions write the declared settings"},
	}
	for _, test := range tests {
		t.Run(string(test.action), func(t *testing.T) {
			c := qt.New(t)

			result, err := mysqlplan.ColumnService{}.PlanFeatures(t.Context(), featureplan.Request{
				Target: "mariadb", ParentKinds: []schemaext.Kind{mysqlschema.ColumnSettingsKind},
				Tables: []featureplan.Table{{Action: test.action, Subject: table}},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Parents, qt.DeepEquals, []featureplan.ParentPlan{{
				Subject: table, Kind: mysqlschema.ColumnSettingsKind, Action: test.action, Strategy: test.strategy}})
		})
	}
}

func TestColumnService_RefusesAnotherTarget(t *testing.T) {
	c := qt.New(t)

	result, err := mysqlplan.ColumnService{}.PlanFeatures(t.Context(), featureplan.Request{Target: "postgres"})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(err, qt.ErrorMatches, `.*MySQL column settings planning on "postgres"`)
	c.Assert(result.Complete, qt.IsFalse)
}
