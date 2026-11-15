package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/internal/planrefusal"
)

// sqlSecurityChange is the comparison's entry for a view v that holds the mode
// held and is declared with the mode declared, for a change of its body.
func sqlSecurityChange(held, declared string) difftypes.ViewDiff {
	return difftypes.ViewDiff{
		ViewName:         "v",
		Changes:          map[string]string{"body": "SELECT 1 -> SELECT 2"},
		Desired:          schemamodel.View{Name: "v", Body: "SELECT 2", Security: declared},
		PreviousBody:     "SELECT 1",
		PreviousSecurity: held,
	}
}

// TestPlanner_SQLSecurity_HappyPath pins the replacement planned for a view
// whose declaration loses no INVOKER: a declared mode is written as declared,
// and a view held as DEFINER, or a rollback, plans with no clause.
func TestPlanner_SQLSecurity_HappyPath(t *testing.T) {
	rollback := sqlSecurityChange("INVOKER", "")
	rollback.Rollback = true
	tests := []struct {
		name string
		view difftypes.ViewDiff
		want string
	}{
		{name: "a declared invoker keeps it", view: sqlSecurityChange("INVOKER", "INVOKER"),
			want: "CREATE OR REPLACE SQL SECURITY INVOKER VIEW v AS\nSELECT 2\n;\n"},
		{name: "a declared definer changes it", view: sqlSecurityChange("INVOKER", "DEFINER"),
			want: "CREATE OR REPLACE SQL SECURITY DEFINER VIEW v AS\nSELECT 2\n;\n"},
		{name: "no mode over definer", view: sqlSecurityChange("DEFINER", ""),
			want: "CREATE OR REPLACE VIEW v AS\nSELECT 2\n;\n"},
		{name: "a rollback that names no mode over invoker", view: rollback,
			want: "CREATE OR REPLACE VIEW v AS\nSELECT 2\n;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mysql.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{test.view}})

			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL("mysql", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(legacyRenderedSQL(sql), qt.Equals, test.want)
		})
	}
}

// TestPlanner_SQLSecurity_FailurePath pins the refusal of a plan whose
// declaration names no mode for a view held as INVOKER, whatever else changes
// on the view, before a statement is planned. The refusal names each view, and
// the mode by the name the statement effect gives it.
func TestPlanner_SQLSecurity_FailurePath(t *testing.T) {
	modeOnly := sqlSecurityChange("INVOKER", "")
	modeOnly.Changes = map[string]string{"security": "INVOKER -> DEFINER"}
	modeOnly.Desired.Body = "SELECT 1"
	other := sqlSecurityChange("INVOKER", "")
	other.ViewName, other.Desired.Name = "w", "w"
	tests := []struct {
		name        string
		views       []difftypes.ViewDiff
		wantOptions []planrefusal.ViewOption
		wantMessage string
	}{
		{
			name:        "a body change",
			views:       []difftypes.ViewDiff{sqlSecurityChange("INVOKER", "")},
			wantOptions: []planrefusal.ViewOption{{View: "v", Option: "sql_security"}},
			wantMessage: `unsupported feature: view v holds SQL SECURITY INVOKER and the schema does not state it\. ` +
				`Without it the view runs with its definer's privileges, not its caller's\. ` +
				`Declare SQL SECURITY INVOKER to keep it, or SQL SECURITY DEFINER to change it\. ` +
				`A Go annotation states the mode as security="INVOKER", a YAML view as security: INVOKER, ` +
				`and an HCL view as security = INVOKER`,
		},
		{
			name:        "the mode alone",
			views:       []difftypes.ViewDiff{modeOnly},
			wantOptions: []planrefusal.ViewOption{{View: "v", Option: "sql_security"}},
			wantMessage: `unsupported feature: view v holds SQL SECURITY INVOKER .*`,
		},
		{
			name:        "two views",
			views:       []difftypes.ViewDiff{sqlSecurityChange("INVOKER", ""), other},
			wantOptions: []planrefusal.ViewOption{{View: "v", Option: "sql_security"}, {View: "w", Option: "sql_security"}},
			wantMessage: `unsupported feature: view v holds SQL SECURITY INVOKER .*; view w holds SQL SECURITY INVOKER .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mysql.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{ViewsModified: test.views})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantMessage)
			var refused *planrefusal.LostViewSecurityError
			c.Assert(err, qt.ErrorAs, &refused)
			c.Assert(refused.Options, qt.DeepEquals, test.wantOptions)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
