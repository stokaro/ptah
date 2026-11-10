package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

func TestRead_ViewSQLSecurity(t *testing.T) {
	for _, test := range []struct{ name, dialect, sql, security string }{
		{"MySQL definer", platform.MySQL, "CREATE SQL SECURITY DEFINER VIEW v AS SELECT 1 AS x;", "DEFINER"},
		{"MariaDB invoker", platform.MariaDB, "CREATE SQL SECURITY DEFINER VIEW v AS SELECT 1 AS x; CREATE OR REPLACE SQL SECURITY INVOKER VIEW v AS SELECT 2 AS x;", "INVOKER"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := readWithOwners([]byte(test.sql), test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(database.Views, qt.HasLen, 1)
			c.Assert(database.Views[0].Security, qt.Equals, test.security)
		})
	}
}
