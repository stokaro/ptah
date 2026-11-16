package mysql_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/readscope"
)

// TestReader_RecordsTheAccountBesideAViewsDefiner reads a database that holds a
// view of another definer and no routine. The view's definer and the
// connected account both reach the description, because a plan that would
// replace the view needs both, and without a routine nothing else asks the
// server who the connection is.
func TestReader_RecordsTheAccountBesideAViewsDefiner(t *testing.T) {
	tests := []struct {
		name    string
		version string
	}{
		{name: "MySQL", version: dbtest.MySQLVersion},
		{name: "MariaDB", version: dbtest.MariaDBVersion},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog := fullCatalog()
			catalog.views = [][]driver.Value{{"v_child", "select 1", "NONE", "DEFINER", "owner@%", "app@%"}}
			catalog.routines = nil
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"}, catalog.answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Authored(t.Context()))

			c.Assert(err, qt.IsNil)
			c.Assert(got.Views, qt.HasLen, 1)
			c.Assert(got.Views[0].Definer, qt.Equals, "owner@%")
			c.Assert(got.CurrentAccount, qt.Equals, "app@%")
		})
	}
}
