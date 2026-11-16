package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/internal/planrefusal"
)

// definerChange is the comparison's entry for a body change to view v, which
// holds the mode held and the definer given and is declared with the mode
// declared.
func definerChange(held, declared, definer string) difftypes.ViewDiff {
	view := sqlSecurityChange(held, declared)
	view.PreviousDefiner = definer
	return view
}

// TestPlanner_ViewDefiner_HappyPath pins the replacements the definer rule
// lets through: a view the connected account defined, a view under SQL
// SECURITY INVOKER before or after the change, a rollback, and a diff no
// database read answers for.
func TestPlanner_ViewDefiner_HappyPath(t *testing.T) {
	rollback := definerChange("DEFINER", "", "owner@%")
	rollback.Rollback = true
	tests := []struct {
		name    string
		view    difftypes.ViewDiff
		account *string
		want    string
	}{
		{name: "the connected account's view", view: definerChange("DEFINER", "", "app@%"), account: new("app@%"),
			want: "CREATE OR REPLACE VIEW v AS\nSELECT 2\n;\n"},
		{name: "the connected account's view with its host in capitals", view: definerChange("DEFINER", "", "app@LOCALHOST"),
			account: new("app@localhost"), want: "CREATE OR REPLACE VIEW v AS\nSELECT 2\n;\n"},
		{name: "a declared definer over the connected account's view", view: definerChange("DEFINER", "DEFINER", "app@%"),
			account: new("app@%"), want: "CREATE OR REPLACE SQL SECURITY DEFINER VIEW v AS\nSELECT 2\n;\n"},
		{name: "an invoker view of another definer", view: definerChange("INVOKER", "INVOKER", "owner@%"),
			account: new("app@%"), want: "CREATE OR REPLACE SQL SECURITY INVOKER VIEW v AS\nSELECT 2\n;\n"},
		{name: "a definer view of another definer declared invoker", view: definerChange("DEFINER", "INVOKER", "owner@%"),
			account: new("app@%"), want: "CREATE OR REPLACE SQL SECURITY INVOKER VIEW v AS\nSELECT 2\n;\n"},
		{name: "an invoker view of another definer declared definer", view: definerChange("INVOKER", "DEFINER", "owner@%"),
			account: new("app@%"), want: "CREATE OR REPLACE SQL SECURITY DEFINER VIEW v AS\nSELECT 2\n;\n"},
		{name: "a rollback", view: rollback, account: new("app@%"),
			want: "CREATE OR REPLACE VIEW v AS\nSELECT 2\n;\n"},
		{name: "no database read", view: definerChange("DEFINER", "", "owner@%"), account: nil,
			want: "CREATE OR REPLACE VIEW v AS\nSELECT 2\n;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mysql.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{test.view}, ConnectedAccount: test.account})

			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL("mysql", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(legacyRenderedSQL(sql), qt.Equals, test.want)
		})
	}
}

// TestPlanner_ViewDefiner_FailurePath pins the refusal of a plan that would
// replace a view running as its definer and make the connected account its
// definer, or that cannot tell, before a statement is planned. The refusal
// names each view with its definer and the connected account.
func TestPlanner_ViewDefiner_FailurePath(t *testing.T) {
	other := definerChange("DEFINER", "", "auditor@localhost")
	other.ViewName, other.Desired.Name = "w", "w"
	tests := []struct {
		name        string
		views       []difftypes.ViewDiff
		account     *string
		wantViews   []planrefusal.ViewDefiner
		wantMessage string
	}{
		{
			name:      "another definer",
			views:     []difftypes.ViewDiff{definerChange("DEFINER", "", "owner@%")},
			account:   new("app@%"),
			wantViews: []planrefusal.ViewDefiner{{View: "v", Definer: "owner@%", Account: "app@%"}},
			wantMessage: `unsupported feature: view v runs with the privileges of its definer owner@% under SQL SECURITY DEFINER, ` +
				`and replacing it as app@% would make app@% its definer\. Connect as owner@% to replace it, ` +
				`declare SQL SECURITY INVOKER if it should run with its caller's privileges, or leave the view unchanged`,
		},
		{
			name:        "another definer and a declared definer",
			views:       []difftypes.ViewDiff{definerChange("DEFINER", "DEFINER", "owner@%")},
			account:     new("app@%"),
			wantViews:   []planrefusal.ViewDefiner{{View: "v", Definer: "owner@%", Account: "app@%"}},
			wantMessage: `unsupported feature: view v runs with the privileges of its definer owner@% .*`,
		},
		{
			name:      "a user name in another case",
			views:     []difftypes.ViewDiff{definerChange("DEFINER", "", "App@localhost")},
			account:   new("app@localhost"),
			wantViews: []planrefusal.ViewDefiner{{View: "v", Definer: "App@localhost", Account: "app@localhost"}},
			wantMessage: `unsupported feature: view v runs with the privileges of its definer App@localhost .*` +
				`replacing it as app@localhost would make app@localhost its definer.*`,
		},
		{
			name:      "a definer the read did not record",
			views:     []difftypes.ViewDiff{definerChange("DEFINER", "", "")},
			account:   new("app@%"),
			wantViews: []planrefusal.ViewDefiner{{View: "v", Definer: "", Account: "app@%"}},
			wantMessage: `unsupported feature: view v runs with its definer's privileges under SQL SECURITY DEFINER, ` +
				`and the read did not record its definer \(""\) or the connected account \("app@%"\), ` +
				`so replacing it could change whose privileges it runs with`,
		},
		{
			name:        "an account the read did not record",
			views:       []difftypes.ViewDiff{definerChange("DEFINER", "", "owner@%")},
			account:     new(""),
			wantViews:   []planrefusal.ViewDefiner{{View: "v", Definer: "owner@%", Account: ""}},
			wantMessage: `unsupported feature: view v runs with its definer's privileges .* its definer \("owner@%"\) or the connected account \(""\).*`,
		},
		{
			name:        "a mode the catalog did not report",
			views:       []difftypes.ViewDiff{definerChange("", "", "owner@%")},
			account:     new("app@%"),
			wantViews:   []planrefusal.ViewDefiner{{View: "v", Definer: "owner@%", Account: "app@%"}},
			wantMessage: `unsupported feature: view v runs with the privileges of its definer owner@% .*`,
		},
		{
			name:      "two views",
			views:     []difftypes.ViewDiff{definerChange("DEFINER", "", "owner@%"), other},
			account:   new("app@%"),
			wantViews: []planrefusal.ViewDefiner{{View: "v", Definer: "owner@%", Account: "app@%"}, {View: "w", Definer: "auditor@localhost", Account: "app@%"}},
			wantMessage: `unsupported feature: view v runs with the privileges of its definer owner@% .*; ` +
				`view w runs with the privileges of its definer auditor@localhost .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mysql.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{ViewsModified: test.views, ConnectedAccount: test.account})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantMessage)
			var refused *planrefusal.ViewDefinerError
			c.Assert(err, qt.ErrorAs, &refused)
			c.Assert(refused.Views, qt.DeepEquals, test.wantViews)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
