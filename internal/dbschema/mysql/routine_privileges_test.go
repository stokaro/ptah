package mysql_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// procsPriv answers the reader's question about mysql.procs_priv with rows
// and err, and hands every other question to next.
func procsPriv(rows [][]driver.Value, err error, next dbtest.QueryHandler) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "FROM mysql.procs_priv") {
			return next(query, args)
		}
		return dbtest.QueryResult{Columns: []string{"User", "Host", "Routine_name", "Routine_type", "Proc_priv"}, Rows: rows}, err
	}
}

// automaticSPPrivileges answers the reader's question about
// automatic_sp_privileges with value, and hands every other question to next.
func automaticSPPrivileges(value int64, next dbtest.QueryHandler) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.TrimSpace(query) != "SELECT @@GLOBAL.automatic_sp_privileges" {
			return next(query, args)
		}
		return dbtest.QueryResult{Columns: []string{"@@GLOBAL.automatic_sp_privileges"}, Rows: [][]driver.Value{{value}}}, nil
	}
}

// The privileges on routines are read from mysql.procs_priv for every
// grantee, with the database the read describes, the privileges in upper case
// and the grant option apart from them. MariaDB records a role with an empty
// host, and only a role, so such a row is a role there; on MySQL it is an
// account whose host is empty. The reading account, app@%, holds every
// privilege with the grant option, so it may grant each row again. Its own row
// of EXECUTE and ALTER ROUTINE is the one the CREATE writes back, and is left
// out; with the grant option it is not.
func TestReader_ReadsThePrivilegesOnRoutines_HappyPath(t *testing.T) {
	rows := [][]driver.Value{
		{"app_r", "%", "P", "PROCEDURE", "Execute,Alter Routine,Grant"},
		{"svc", "10.1.%", "price", "FUNCTION", "Execute"},
		{"hostless", "", "p", "PROCEDURE", "Grant"},
		{"app", "%", "p", "PROCEDURE", "Execute,Alter Routine"},
		{"app", "%", "price", "FUNCTION", "Execute,Alter Routine,Grant"},
	}
	tests := []struct {
		name    string
		version string
		role    bool
	}{
		{name: "MySQL", version: dbtest.MySQLVersion, role: false},
		{name: "MariaDB", version: dbtest.MariaDBVersion, role: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, dbtest.MySQLAllGrants,
				procsPriv(rows, nil, visibleCatalog{selected: "shop", routines: [][]driver.Value{procedureRow("p", "SELECT 1"), functionRow("price", "RETURN 1")}}.answer)))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.RoutinePrivileges, qt.DeepEquals, catalog.RoutinePrivileges{Grants: []catalog.RoutineGrant{
				{User: "app_r", Host: "%", Schema: "shop", Name: "P", Type: "PROCEDURE", Privileges: []string{"EXECUTE", "ALTER ROUTINE"}, WithOption: true, ReaderMayGrant: true},
				{User: "svc", Host: "10.1.%", Schema: "shop", Name: "price", Type: "FUNCTION", Privileges: []string{"EXECUTE"}, ReaderMayGrant: true},
				{User: "hostless", Host: "", Role: test.role, Schema: "shop", Name: "p", Type: "PROCEDURE", WithOption: true, ReaderMayGrant: true},
				{User: "app", Host: "%", Schema: "shop", Name: "price", Type: "FUNCTION", Privileges: []string{"EXECUTE", "ALTER ROUTINE"}, WithOption: true, ReaderMayGrant: true},
			}})
		})
	}
}

// currentRole answers the reader's question about the session's active roles
// with value, and hands every other question to next.
func currentRole(value string, next dbtest.QueryHandler) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.TrimSpace(query) != "SELECT CURRENT_ROLE()" {
			return next(query, args)
		}
		return dbtest.QueryResult{Columns: []string{"CURRENT_ROLE()"}, Rows: [][]driver.Value{{value}}}, nil
	}
}

// On MySQL the CREATE after a DROP writes the account's own row on the routine
// back only from a session with no role active, so with a role active the read
// keeps the row and records whether the account may grant it again. MariaDB
// writes the row back with a role active, and the read leaves it out there.
// The account holds SELECT, CREATE and CREATE ROUTINE on the database, which
// carry no grant option.
func TestReader_KeepsTheAccountsOwnRowOnRoutinesWithARoleActive(t *testing.T) {
	rows := [][]driver.Value{{"app", "%", "p", "PROCEDURE", "Execute,Alter Routine"}}
	grants := []string{"GRANT USAGE ON *.* TO `app`@`%`", "GRANT SELECT, CREATE, CREATE ROUTINE ON `shop`.* TO `app`@`%`"}
	tests := []struct {
		name    string
		version string
		role    string
		want    []catalog.RoutineGrant
	}{
		{
			name: "MySQL with a role active", version: dbtest.MySQLVersion, role: "`rg`@`%`",
			want: []catalog.RoutineGrant{
				{User: "app", Host: "%", Schema: "shop", Name: "p", Type: "PROCEDURE", Privileges: []string{"EXECUTE", "ALTER ROUTINE"}},
			},
		},
		{name: "MySQL with no role active", version: dbtest.MySQLVersion, role: "NONE"},
		{name: "MariaDB with a role active", version: dbtest.MariaDBVersion, role: "rg"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, currentRole(test.role, dbtest.MySQLSession(test.version, grants,
				procsPriv(rows, nil, visibleCatalog{selected: "shop", routines: [][]driver.Value{procedureRow("p", "SELECT 1"), functionRow("price", "RETURN 1")}}.answer))))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.RoutinePrivileges, qt.DeepEquals, catalog.RoutinePrivileges{Grants: test.want})
		})
	}
}

// An account without the grant option on the database may not grant a row
// again: ALL PRIVILEGES on the database carries no grant option, and the grant
// option on another database does not count.
func TestReader_RecordsThePrivilegesOnRoutinesTheReaderMayNotGrant(t *testing.T) {
	c := qt.New(t)
	rows := [][]driver.Value{{"u1", "10.1.%", "p", "PROCEDURE", "Execute"}}
	grants := []string{
		"GRANT USAGE ON *.* TO `app`@`%`",
		"GRANT ALL PRIVILEGES ON `shop`.* TO `app`@`%`",
		"GRANT EXECUTE ON `other`.* TO `app`@`%` WITH GRANT OPTION",
	}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, grants,
		procsPriv(rows, nil, visibleCatalog{selected: "shop", routines: [][]driver.Value{procedureRow("p", "SELECT 1"), functionRow("price", "RETURN 1")}}.answer)))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.RoutinePrivileges, qt.DeepEquals, catalog.RoutinePrivileges{Grants: []catalog.RoutineGrant{
		{User: "u1", Host: "10.1.%", Schema: "shop", Name: "p", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}},
	}})
}

// With automatic_sp_privileges off the DROP of a routine keeps its rows of
// mysql.procs_priv, so there is nothing to give back: the read reports no row
// and no refusal, and does not ask mysql.procs_priv, which here refuses the
// account.
func TestReader_ReadsNoPrivilegesOnRoutinesTheDropKeeps(t *testing.T) {
	c := qt.New(t)
	refused := &mysqldriver.MySQLError{Number: 1142, Message: "SELECT command denied to user 'app'@'%' for table 'procs_priv'"}
	session := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
		procsPriv([][]driver.Value{{"u1", "%", "p", "PROCEDURE", "Execute"}}, refused, visibleCatalog{selected: "shop", routines: [][]driver.Value{procedureRow("p", "SELECT 1"), functionRow("price", "RETURN 1")}}.answer))
	db := dbtest.Open(t, automaticSPPrivileges(0, session))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.RoutinePrivileges, qt.DeepEquals, catalog.RoutinePrivileges{})
}

// An account that may not read mysql.procs_priv still gets its description,
// and the description records that the privileges on routines are not known,
// so a plan that would replace a routine can refuse.
func TestReader_RecordsARefusedReadOfThePrivilegesOnRoutines(t *testing.T) {
	c := qt.New(t)
	refused := &mysqldriver.MySQLError{Number: 1142, Message: "SELECT command denied to user 'app'@'%' for table 'procs_priv'"}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
		procsPriv(nil, refused, visibleCatalog{selected: "shop", routines: [][]driver.Value{procedureRow("p", "SELECT 1"), functionRow("price", "RETURN 1")}}.answer)))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.RoutinePrivileges, qt.DeepEquals, catalog.RoutinePrivileges{Refused: true})
}

// A failure that is not a refusal fails the read rather than passing for one.
func TestReader_ReadsThePrivilegesOnRoutines_FailurePath(t *testing.T) {
	c := qt.New(t)
	broken := errors.New("connection reset")
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
		procsPriv(nil, broken, visibleCatalog{selected: "shop", routines: [][]driver.Value{procedureRow("p", "SELECT 1"), functionRow("price", "RETURN 1")}}.answer)))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `failed to read privileges on routines: .*connection reset`)
	c.Assert(got, qt.IsNil)
}

// A stale row left after DROP with automatic_sp_privileges disabled belongs
// to no routine. The reader keeps only rows on an existing name and kind.
func TestReader_LeavesOutPrivilegesOnMissingRoutines(t *testing.T) {
	c := qt.New(t)
	rows := [][]driver.Value{
		{"reader", "%", "P", "PROCEDURE", "Execute"},
		{"gone", "%", "missing", "PROCEDURE", "Execute"},
		{"other_kind", "%", "p", "FUNCTION", "Execute"},
	}
	visible := visibleCatalog{selected: "shop", routines: [][]driver.Value{procedureRow("p", "SELECT 1")}}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
		procsPriv(rows, nil, visible.answer)))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.RoutinePrivileges.Grants, qt.DeepEquals, []catalog.RoutineGrant{
		{User: "reader", Host: "%", Schema: "shop", Name: "P", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}, ReaderMayGrant: true},
	})
}
