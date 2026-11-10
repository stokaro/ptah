package mysql

// White-box testing required: readViews is package-local and reads the
// catalog's security column beside the view body.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

func TestReadViews_CarriesSQLSecurity(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		c.Assert(query, qt.Contains, "SECURITY_TYPE")
		return dbtest.QueryResult{Columns: []string{"TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "SECURITY_TYPE"}, Rows: [][]driver.Value{
			{"v", "select 1", "NONE", "INVOKER"},
		}}, nil
	})
	views, err := NewMySQLReader(db.SQL, "app").readViews(t.Context(), "app")
	c.Assert(err, qt.IsNil)
	c.Assert(views, qt.HasLen, 1)
	c.Assert(views[0].Security, qt.Equals, "INVOKER")
}
