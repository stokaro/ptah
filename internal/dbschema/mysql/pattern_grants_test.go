package mysql_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// wideGrantsOf answers the reads of wide grants: lc2_r holds SELECT on every
// database, and the read of grants on a database pattern returns patterns, or
// fails with patternErr where it is set. Every other question goes to next.
func wideGrantsOf(patterns [][]driver.Value, patternErr error, next dbtest.QueryHandler) dbtest.QueryHandler {
	columns := []string{
		"Select_priv", "Insert_priv", "Update_priv", "Delete_priv", "Create_priv", "Drop_priv",
		"References_priv", "Index_priv", "Alter_priv", "Create_view_priv", "Show_view_priv", "Trigger_priv",
	}
	present := make([][]driver.Value, 0, len(columns))
	held := []driver.Value{"lc2_r"}
	for _, column := range columns {
		present = append(present, []driver.Value{column})
		held = append(held, column == "Select_priv")
	}
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "sp.TABLE_SCHEMA, sp.PRIVILEGE_TYPE"):
			return dbtest.QueryResult{Columns: []string{"role_name", "TABLE_SCHEMA", "PRIVILEGE_TYPE"}, Rows: patterns}, patternErr
		case strings.Contains(query, "SELECT column_name"):
			return dbtest.QueryResult{Columns: []string{"column_name"}, Rows: present}, nil
		case strings.Contains(query, "Insert_priv = 'Y', Update_priv = 'Y'"):
			return dbtest.QueryResult{Columns: append([]string{"user"}, columns...), Rows: [][]driver.Value{held}}, nil
		}
		return next(query, args)
	}
}

// TestReader_ReadsGrantsOnADatabasePattern reports a grant on a database name
// pattern that reaches the database as a wide grant on it, beside the grants
// on every database. A row the pattern does not reach, and a privilege no
// grant on a table names, are left out. A read of the pattern grants that the
// server refuses leaves the grants on every database and marks the wide grants
// partial: a revoke under a pattern grant the read could not see is then
// planned, and the server answers for the role.
func TestReader_ReadsGrantsOnADatabasePattern(t *testing.T) {
	everywhere := catalog.WideGrant{Role: "lc2_r", Privilege: "SELECT", Grant: "SELECT ON *.*"}
	tests := []struct {
		name        string
		patterns    [][]driver.Value
		patternErr  error
		want        []catalog.WideGrant
		wantPartial bool
	}{
		{
			name:     "a pattern that reaches the database",
			patterns: [][]driver.Value{{"lc2_r", "R%", "INSERT"}},
			want: []catalog.WideGrant{
				everywhere,
				{Role: "lc2_r", Privilege: "INSERT", Grant: "INSERT ON `R%`.*", Schema: "R_LC"},
			},
		},
		{
			name: "rows the patterns of which reach the database or not",
			patterns: [][]driver.Value{
				{"lc2_r", "other%", "SELECT"},
				{"lc2_r", `R\_LC`, "UPDATE"},
				{"lc2_r", `R\%LC`, "DELETE"},
				{"lc2_r", "R_LC_", "INSERT"},
			},
			want: []catalog.WideGrant{
				everywhere,
				{Role: "lc2_r", Privilege: "UPDATE", Grant: "UPDATE ON `R\\_LC`.*", Schema: "R_LC"},
			},
		},
		{
			name:     "a privilege no grant on a table names",
			patterns: [][]driver.Value{{"lc2_r", "R%", "EVENT"}, {"lc2_r", "R%", "insert"}},
			want: []catalog.WideGrant{
				everywhere,
				{Role: "lc2_r", Privilege: "INSERT", Grant: "INSERT ON `R%`.*", Schema: "R_LC"},
			},
		},
		{
			name:        "a refused read of the patterns",
			patternErr:  &mysqldriver.MySQLError{Number: 1142, Message: "SELECT command denied to user 'app'@'%' for table 'user'"},
			want:        []catalog.WideGrant{everywhere},
			wantPartial: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
				wideGrantsOf(test.patterns, test.patternErr, foldedPrivileges(visibleCatalog{selected: "R_LC", tables: []string{"items"}}.answer)))
			db := dbtest.Open(t, session)

			got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.WideGrants, qt.DeepEquals, test.want)
			c.Assert(got.WideGrantsPartial, qt.Equals, test.wantPartial)
		})
	}
}

// TestReader_ReadsGrantsOnADatabasePattern_FailurePath fails the read where
// the read of the pattern grants fails for a reason other than a refusal: the
// wide grants would otherwise read as complete without them.
func TestReader_ReadsGrantsOnADatabasePattern_FailurePath(t *testing.T) {
	c := qt.New(t)
	session := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
		wideGrantsOf(nil, errors.New("connection reset by peer"), foldedPrivileges(visibleCatalog{selected: "R_LC", tables: []string{"items"}}.answer)))
	db := dbtest.Open(t, session)

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `(?s).*failed to read grants on database patterns: .*connection reset by peer.*`)
	c.Assert(got, qt.IsNil)
}
