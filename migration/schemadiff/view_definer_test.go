package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planrefusal"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// viewDefinerDesired declares view v with the body given and no mode.
func viewDefinerDesired(body string) *schemamodel.Database {
	return &schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: body}}}
}

// viewDefinerCurrent is a read of database app holding view v as SQL SECURITY
// DEFINER, with the definer and the reading account given.
func viewDefinerCurrent(definer, currentAccount string) *catalog.Database {
	return &catalog.Database{CurrentAccount: currentAccount, Views: []catalog.View{{
		Name: "v", Schema: "app", Body: "SELECT 1", CheckOption: "NONE", Security: "DEFINER", Definer: definer,
	}}}
}

// TestCompareWithDatabaseInfo_RecordsAViewsDefinerFacts pins the two facts a
// database-aware comparison hands the planner for a modified view: the
// definer the view holds and the account the read ran as.
func TestCompareWithDatabaseInfo_RecordsAViewsDefinerFacts(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), viewDefinerDesired("SELECT 2"),
				viewDefinerCurrent("owner_a@%", "migrator_a@%"), mysqlDefinerInfo(dialect), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, 1)
			c.Assert(diff.ViewsModified[0].PreviousDefiner, qt.Equals, "owner_a@%")
			c.Assert(diff.ConnectedAccount, qt.DeepEquals, new("migrator_a@%"))
		})
	}
}

// TestCompareWithDialect_RecordsNoConnectedAccount is the control: a
// comparison with no database read behind it names no account, so the planner
// checks no definer for its diff.
func TestCompareWithDialect_RecordsNoConnectedAccount(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDialect(t.Context(), viewDefinerDesired("SELECT 2"),
		viewDefinerCurrent("owner_a@%", "migrator_a@%"), "mysql", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ConnectedAccount, qt.IsNil)
}

// TestCompareWithDatabaseInfo_ReplayedCurrentRecordsNoDefinerFacts drives a
// comparison whose current state a replay built on a dev database, through
// the planner: the read's account and definer are the replay's, so the diff
// carries neither, the replacement names no definer and nothing is refused,
// although the definer is another account than the one that read it.
func TestCompareWithDatabaseInfo_ReplayedCurrentRecordsNoDefinerFacts(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			opts := config.DefaultCompareOptions()
			opts.ReplayedCurrent = true

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), viewDefinerDesired("SELECT 2"),
				viewDefinerCurrent("owner_a@%", "root@%"), mysqlDefinerInfo(dialect), opts, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, 1)
			c.Assert(diff.ViewsModified[0].PreviousDefiner, qt.Equals, "")
			c.Assert(diff.ConnectedAccount, qt.IsNil)
			sql, err := planner.GenerateSchemaDiffSQL(t.Context(), runtime, diff, dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW")
			c.Assert(sql, qt.Not(qt.Contains), "DEFINER=")
		})
	}
}

// TestCompareWithDatabaseInfo_PlansAViewOfTheConnectedAccount_HappyPath drives
// the comparison and the planner together, as a plan does: a view the reading
// account defined is replaced, naming that account as its definer.
func TestCompareWithDatabaseInfo_PlansAViewOfTheConnectedAccount_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), viewDefinerDesired("SELECT 2"),
				viewDefinerCurrent("migrator_a@%", "migrator_a@%"), mysqlDefinerInfo(dialect), nil, runtime)
			c.Assert(err, qt.IsNil)

			sql, err := planner.GenerateSchemaDiffSQL(t.Context(), runtime, diff, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "CREATE OR REPLACE DEFINER=`migrator_a`@`%` VIEW")
		})
	}
}

// TestCompareWithDatabaseInfo_PlansAViewOfAnotherDefiner_FailurePath drives the
// same path over a view another account defined, and over reads that did not
// record one of the facts: the planner refuses each, naming the view, its
// definer and the reading account.
func TestCompareWithDatabaseInfo_PlansAViewOfAnotherDefiner_FailurePath(t *testing.T) {
	tests := []struct {
		name           string
		definer        string
		currentAccount string
		wantMessage    string
	}{
		{name: "another definer", definer: "owner_a@%", currentAccount: "migrator_a@%",
			wantMessage: `.*view v runs with the privileges of its definer owner_a@% .* replacing it as migrator_a@% .*`},
		{name: "no definer", definer: "", currentAccount: "migrator_a@%",
			wantMessage: `.*view v runs with its definer's privileges .* its definer \(""\) or the connected account \("migrator_a@%"\).*`},
		{name: "no reading account", definer: "owner_a@%", currentAccount: "",
			wantMessage: `.*view v runs with its definer's privileges .* its definer \("owner_a@%"\) or the connected account \(""\).*`},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				runtime := must.Must(builtin.New())
				diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), viewDefinerDesired("SELECT 2"),
					viewDefinerCurrent(test.definer, test.currentAccount), mysqlDefinerInfo(dialect), nil, runtime)
				c.Assert(err, qt.IsNil)

				sql, err := planner.GenerateSchemaDiffSQL(t.Context(), runtime, diff, dialect)

				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches, test.wantMessage)
				var refused *planrefusal.ViewDefinerError
				c.Assert(err, qt.ErrorAs, &refused)
				c.Assert(refused.Views, qt.DeepEquals, []planrefusal.ViewDefiner{
					{View: "v", Definer: test.definer, Account: test.currentAccount},
				})
				c.Assert(sql, qt.Equals, "")
			})
		}
	}
}
