package mysql

// White-box testing required: readViews is package-local and reads the
// catalog's security column, the view's definer and the connected account
// beside the view body.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// TestReadViews_CarriesSQLSecurityAndDefiner pins the facts the planner needs
// before it may replace a view: the mode it runs under, DEFINER, the account
// it runs as under that mode, and CURRENT_USER(), the account a replacement
// would make its definer.
func TestReadViews_CarriesSQLSecurityAndDefiner(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		c.Assert(query, qt.Contains, "SECURITY_TYPE")
		c.Assert(query, qt.Contains, "DEFINER")
		c.Assert(query, qt.Contains, "CURRENT_USER()")
		return dbtest.QueryResult{
			Columns: []string{"TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "SECURITY_TYPE", "DEFINER", "CURRENT_USER()"},
			Rows:    [][]driver.Value{{"v", "select 1", "NONE", "INVOKER", "owner_a@%", "migrator_a@%"}},
		}, nil
	})

	read, err := NewMySQLReader(db.SQL, "app").readViews(t.Context(), "app")

	c.Assert(err, qt.IsNil)
	c.Assert(read.views, qt.HasLen, 1)
	c.Assert(read.views[0].Security, qt.Equals, "INVOKER")
	c.Assert(read.views[0].Definer, qt.Equals, "owner_a@%")
	c.Assert(read.currentAccount, qt.Equals, "migrator_a@%")
}
