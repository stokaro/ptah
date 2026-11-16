package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestViewDefinitions_SQLSecurityIsCompared pins the security change a view
// comparison reports and the mode it says the view holds. A declaration that
// names no mode states the server default, DEFINER, so a view held as INVOKER
// reports a change; the planner refuses it, and reads the held mode for that.
func TestViewDefinitions_SQLSecurityIsCompared(t *testing.T) {
	tests := []struct {
		name, dialect, declared, held string
		want                          string
		wantHeld                      string
	}{
		{"MySQL invoker against a declaration that names none", platform.MySQL, "", "INVOKER", "INVOKER -> DEFINER", "INVOKER"},
		{"MariaDB invoker against a declaration that names none", platform.MariaDB, "", "invoker", "INVOKER -> DEFINER", "INVOKER"},
		{"definer against a declaration that names none", platform.MySQL, "", "DEFINER", "", "DEFINER"},
		{"MySQL invoker to declared definer", platform.MySQL, "DEFINER", "INVOKER", "INVOKER -> DEFINER", "INVOKER"},
		{"MariaDB default definer to invoker", platform.MariaDB, "INVOKER", "DEFINER", "DEFINER -> INVOKER", "DEFINER"},
		{"explicit definer matches", platform.MySQL, "DEFINER", "DEFINER", "", "DEFINER"},
		{"explicit invoker matches", platform.MySQL, "INVOKER", "INVOKER", "", "INVOKER"},
		{"unsupported dialect reports requested mode", platform.Postgres, "INVOKER", "", "(unsupported) -> INVOKER", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.ViewDefinitionsWithDialect(schemamodel.View{Name: "v", Body: "SELECT 1", Security: test.declared},
				catalog.View{Name: "v", Body: "SELECT 1", Security: test.held}, test.dialect)
			c.Assert(diff.Changes["security"], qt.Equals, test.want)
			c.Assert(diff.PreviousSecurity, qt.Equals, test.wantHeld)
			c.Assert(diff.Desired.Security, qt.Equals, test.declared)
		})
	}
}

// TestViewDefinitions_BodyChangeReportsTheLostSQLSecurity pins a body change to
// a view held as INVOKER, declared without a mode: the comparison reports the
// mode change beside the body, and the view it leaves behind names no mode.
func TestViewDefinitions_BodyChangeReportsTheLostSQLSecurity(t *testing.T) {
	c := qt.New(t)

	diff := compare.ViewDefinitionsWithDialect(schemamodel.View{Name: "v", Body: "SELECT 2"},
		catalog.View{Name: "v", Body: "SELECT 1", Security: "INVOKER"}, platform.MySQL)

	c.Assert(diff.Changes, qt.DeepEquals, map[string]string{"body": "SELECT 1 -> SELECT 2", "security": "INVOKER -> DEFINER"})
	c.Assert(diff.PreviousSecurity, qt.Equals, "INVOKER")
	c.Assert(diff.Desired.Security, qt.Equals, "")
}

// TestViewDefinitions_RecordsTheHeldDefiner pins the definer a view comparison
// hands the planner: the one the view holds, on a target whose views have a
// SQL SECURITY mode, and none elsewhere, where no view runs as its definer.
func TestViewDefinitions_RecordsTheHeldDefiner(t *testing.T) {
	tests := []struct {
		name, dialect string
		want          string
	}{
		{name: "MySQL", dialect: platform.MySQL, want: "owner@%"},
		{name: "MariaDB", dialect: platform.MariaDB, want: "owner@%"},
		{name: "a target whose views have no mode", dialect: platform.Postgres, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitionsWithDialect(schemamodel.View{Name: "v", Body: "SELECT 2"},
				catalog.View{Name: "v", Body: "SELECT 1", Security: "DEFINER", Definer: "owner@%"}, test.dialect)

			c.Assert(diff.PreviousDefiner, qt.Equals, test.want)
		})
	}
}
