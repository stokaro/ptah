package mysql_test

import (
	"database/sql/driver"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// A role and a login can share a user name on different hosts. Both reads of
// the privilege views must join the complete account, and take the role name
// from mysql.user rather than split or unquote the grantee's text. The live
// account-identity test measures the rows these predicates return on both engines.
func TestReader_MatchesGrantsToTheCompleteAccount(t *testing.T) {
	c := qt.New(t)
	var queries []string
	answer := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
		foldedPrivileges(visibleCatalog{selected: "R_LC", tables: []string{"items"}}.answer))
	db := dbtest.Open(c, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		return answer(query, args)
	})

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.Grants, qt.HasLen, 3)
	grants := slices.DeleteFunc(queries, func(query string) bool {
		return !strings.Contains(query, "JOIN mysql.user AS u")
	})
	c.Assert(grants, qt.HasLen, 2)
	for _, query := range grants {
		c.Assert(query, qt.Contains, "SELECT u.user AS role_name")
		c.Assert(query, qt.Contains, "AS BINARY) = CAST(CONCAT('''', u.user, '''@''', u.host, '''') AS BINARY)")
		c.Assert(query, qt.Not(qt.Contains), "SUBSTRING_INDEX")
	}
}
