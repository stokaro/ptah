package mysql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// A bare grantee names an account on host % on MySQL. On MariaDB it names
// the role when one exists and the account on host % otherwise. Reading only
// roles makes a grant to a login look new and makes its rollback revoke a
// privilege the account held before the migration.
func TestReader_GrantProjectionIncludesAddressableLogins(t *testing.T) {
	for _, test := range []struct {
		name, version string
		marker        string
		want          string
		absent        string
		catalog       dbtest.QueryHandler
	}{
		{name: "MySQL", version: dbtest.MySQLVersion, marker: "information_schema.COLUMN_PRIVILEGES",
			want: "u.host = '%'", absent: "u.account_locked = 'Y'",
			catalog: foldedPrivileges(visibleCatalog{selected: "app", tables: []string{"items"}}.answer)},
		{name: "MariaDB", version: dbtest.MariaDBVersion, marker: "FROM mysql.tables_priv",
			want: "u.host = '%'", absent: "AND u.is_role = 'Y'\n",
			catalog: inheritingPrivileges(visibleCatalog{selected: "app", tables: []string{"t2"}}.answer)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var queried string
			answer := dbtest.MySQLSession(test.version, dbtest.MySQLAllGrants, test.catalog)
			db := dbtest.Open(c, recordLoginGrantQuery(test.marker, &queried, answer))

			_, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "app", capability.MySQL84()).ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(queried, qt.Contains, test.want)
			c.Assert(queried, qt.Not(qt.Contains), test.absent)
		})
	}
}

func recordLoginGrantQuery(marker string, queried *string, next dbtest.QueryHandler) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, marker) {
			*queried = query
		}
		return next(query, args)
	}
}
