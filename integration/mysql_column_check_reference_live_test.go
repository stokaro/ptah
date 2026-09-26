//go:build integration

package integration_test

import (
	"database/sql"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/internal/mysqlcheck"
	"ptah.run/internal/sqlschema"
)

// mysqlColumnCheckStatements are CREATE TABLE statements whose CHECKs are
// written on columns. Some name a column other than their own.
var mysqlColumnCheckStatements = []struct {
	name string
	sql  string
}{
	{name: "another column", sql: "CREATE TABLE c (a int, b int CHECK (b > a))"},
	{name: "a column declared after it", sql: "CREATE TABLE c (a int CHECK (a < b), b int)"},
	{name: "a second check naming another column", sql: "CREATE TABLE c (a int CHECK (a > 0) CHECK (b > 0), b int)"},
	{name: "another column qualified", sql: "CREATE TABLE c (a int, b int CHECK (c.a > 0))"},
	{name: "its own column", sql: "CREATE TABLE c (a int, b int CHECK (b > 0 AND c.b < 9))"},
	{name: "a string spelling another column", sql: "CREATE TABLE c (a int, b varchar(10) CHECK (b <> 'a'))"},
	{name: "an INTERVAL unit spelling a column", sql: "CREATE TABLE c (a int, `year` int, b date CHECK (b > DATE_SUB('2020-01-01', INTERVAL 1 YEAR)))"},
	{name: "another column at table level", sql: "CREATE TABLE c (a int, b int, CHECK (b > a))"},
}

// TestMySQLFamilyColumnCheckReferencesMatchTheServerLive runs each statement
// on the server and reads it with Ptah for the same engine, and asserts that
// Ptah refuses exactly what the server refuses. MySQL answers `ERROR 3813
// (HY000)` to a CHECK written on a column that names another column; MariaDB
// accepts it. A reader that accepted what MySQL refuses would plan a table the
// server cannot create (stokaro/ptah#3791).
func TestMySQLFamilyColumnCheckReferencesMatchTheServerLive(t *testing.T) {
	for _, engine := range []mysqlFamilyEngine{mysqlCheckEngine, mariaDBCheckEngine} {
		for _, statement := range mysqlColumnCheckStatements {
			t.Run(engine.name+": "+statement.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newCheckScratch(c, engine)
				name, _ := scratch.database(c, "colref")
				conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name))
				c.Assert(err, qt.IsNil)
				defer func() { c.Check(conn.Close(), qt.IsNil) }()

				_, serverErr := conn.ExecContext(c.Context(), statement.sql)
				_, _, readErr := sqlschema.Read([]byte(statement.sql+";"), engine.name)

				c.Assert(errors.Is(readErr, mysqlcheck.ErrNamesOtherColumn), qt.Equals, mysqlErrorNumber(serverErr) == 3813,
					qt.Commentf("server: %v\nreader: %v", serverErr, readErr))
			})
		}
	}
}

// mysqlErrorNumber answers the error number the server returned, and 0 for a
// statement it accepted.
func mysqlErrorNumber(err error) uint16 {
	var serverErr *mysqldriver.MySQLError
	if errors.As(err, &serverErr) {
		return serverErr.Number
	}
	return 0
}
