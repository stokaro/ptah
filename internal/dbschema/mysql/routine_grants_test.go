package mysql_test

import (
	"database/sql/driver"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// routineGrantsOf is the grants of grants on a function or a procedure.
func routineGrantsOf(grants []catalog.Grant) []catalog.Grant {
	return slices.DeleteFunc(slices.Clone(grants), func(grant catalog.Grant) bool {
		return grant.ObjectType != "FUNCTION" && grant.ObjectType != "PROCEDURE"
	})
}

// TestReader_ReadsTheGrantsOnRoutines_HappyPath reads the role lc2_r's
// privileges on the function f1 and the procedure p of the database shop from
// mysql.procs_priv, one grant per privilege, each on the routine as the
// database spells it: the row for F1 is the grant on f1. A row whose routine
// the database does not hold is left out, and so is a row on a procedure f1,
// which is not the function f1. A row holding only the grant option reads as
// USAGE with the option, and a login's grant is marked as one.
func TestReader_ReadsTheGrantsOnRoutines_HappyPath(t *testing.T) {
	c := qt.New(t)
	rows := [][]driver.Value{
		{"lc2_r", "%", "F1", "FUNCTION", "Execute,Grant"},
		{"lc2_r", "%", "P", "PROCEDURE", "Alter Routine"},
		{"lc2_r", "%", "gone", "PROCEDURE", "Execute"},
		{"lc2_r", "%", "f1", "PROCEDURE", "Execute"},
		{"svc", "%", "p", "PROCEDURE", "Grant"},
	}
	var queried []string
	answer := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, procsPriv(rows, nil,
		foldedPrivileges(visibleCatalog{selected: "shop", routines: [][]driver.Value{functionRow("f1", "RETURN 1"), procedureRow("p", "SELECT 1")}}.answer)))
	db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		queried = append(queried, query)
		return answer(query, args)
	})

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(routineGrantsOf(got.Grants), qt.DeepEquals, []catalog.Grant{
		{Role: "lc2_r", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "shop", ObjectName: "f1", WithOption: true},
		{Role: "lc2_r", Privilege: "ALTER ROUTINE", ObjectType: "PROCEDURE", Schema: "shop", ObjectName: "p"},
		{Role: "svc", Privilege: "USAGE", ObjectType: "PROCEDURE", Schema: "shop", ObjectName: "p", WithOption: true, Login: true},
	})
	grantQueries := slices.DeleteFunc(queried, func(query string) bool {
		return !strings.Contains(query, "FROM mysql.procs_priv") || !strings.Contains(query, "JOIN mysql.user AS u")
	})
	c.Assert(grantQueries, qt.HasLen, 1)
	c.Assert(grantQueries[0], qt.Contains, "u.user = pp.User AND u.host = pp.Host")
	c.Assert(grantQueries[0], qt.Contains, "u.host = '%'")
}

// TestReader_RefusedTheGrantsOnRoutines_FailurePath keeps the roles and every
// grant unknown together where the account may not read mysql.procs_priv, as
// where it may not read the privilege views: a description holding the roles
// and their table grants without the routine grants would read as "these
// roles hold nothing on a routine", which the account could not check.
func TestReader_RefusedTheGrantsOnRoutines_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := &mysqldriver.MySQLError{Number: 1142, Message: "SELECT command denied to user 'app'@'%' for table 'procs_priv'"}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, procsPriv(nil, refused,
		foldedPrivileges(visibleCatalog{selected: "shop", routines: [][]driver.Value{functionRow("f1", "RETURN 1")}}.answer))))

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.Roles, qt.IsNil)
	c.Assert(got.Grants, qt.IsNil)
	c.Assert(got.NotDescribed.Objects, qt.Contains, coverage.Refused(coverage.Role))
}
