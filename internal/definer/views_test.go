package definer_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/definer"
)

// unnamedViewDiff modifies view v, held under the mode given and declared
// with the one given, on a diff recording the account given.
func unnamedViewDiff(held, declared string, account *string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		ConnectedAccount: account,
		ViewsModified: []difftypes.ViewDiff{{
			Desired:          schemamodel.View{Name: "v", Body: "SELECT 2", Security: declared},
			PreviousSecurity: held,
			PreviousDefiner:  "owner@%",
		}},
	}
}

// A replay or a comparison of two descriptions records no account, so a
// view that runs as its definer is replaced with no DEFINER clause and the
// warning names it. A view the declaration leaves without a mode runs as its
// definer too.
func TestUnnamedViews_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		held     string
		declared string
	}{
		{name: "both DEFINER", held: "DEFINER", declared: "DEFINER"},
		{name: "a declaration that states no mode", held: "DEFINER", declared: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			definer.WarnUnnamedViews(&out, unnamedViewDiff(test.held, test.declared, nil), "mysql")

			c.Assert(definer.UnnamedViews(unnamedViewDiff(test.held, test.declared, nil), "mysql"),
				qt.DeepEquals, []string{"v"})
			c.Assert(out.String(), qt.Equals, "warning: view v runs with its definer's privileges under SQL "+
				"SECURITY DEFINER, and the migration replaces it without a DEFINER clause, so the account that "+
				"applies the migration becomes its definer. To keep a definer, write DEFINER= into the file, or "+
				"apply it as that definer\n")
		})
	}
}

// Nothing is named where the view does not run as its definer on one side,
// where the diff records the account (the planner names the definer or
// refuses), and on a target whose views have no mode.
func TestUnnamedViews_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		dialect string
	}{
		{name: "held as INVOKER", diff: unnamedViewDiff("INVOKER", "DEFINER", nil), dialect: "mysql"},
		{name: "declared INVOKER", diff: unnamedViewDiff("DEFINER", "INVOKER", nil), dialect: "mariadb"},
		{name: "an account the read recorded", diff: unnamedViewDiff("DEFINER", "DEFINER", new("owner@%")),
			dialect: "mysql"},
		{name: "a target without a view mode", diff: unnamedViewDiff("", "", nil), dialect: "sqlserver"},
		{name: "no diff", diff: nil, dialect: "mysql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			definer.WarnUnnamedViews(&out, test.diff, test.dialect)

			c.Assert(definer.UnnamedViews(test.diff, test.dialect), qt.HasLen, 0)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}
