//go:build integration

package mysql_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestForeignDefinerReplacementRefusal_Live proves that the catalog accounts
// read by the production MySQL-family reader reach the database-aware compare
// boundary. The routine is created by one account and compared through another
// account that could otherwise drop and recreate it.
func TestForeignDefinerReplacementRefusal_Live(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		dsnEnv  string
	}{
		{name: "mysql", dialect: platform.MySQL, dsnEnv: "MYSQL_ADMIN_TEST_DSN"},
		{name: "mariadb", dialect: platform.MariaDB, dsnEnv: "MARIADB_ADMIN_TEST_DSN"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			adminDSN := requireMySQLAdminDSN(c, test.dsnEnv)
			adminDB := openMySQLWriterLiveDatabase(c, test.dsnEnv)
			c.Cleanup(func() { c.Check(adminDB.Close(), qt.IsNil) })

			suffix := time.Now().UnixNano()
			databaseName := fmt.Sprintf("ptah_definer_%d", suffix)
			owner := fmt.Sprintf("ptah_owner_%d", suffix)
			password := fmt.Sprintf("Ptah-owner-%d!", suffix)
			createMySQLWriterLiveDatabase(c, adminDB, databaseName)
			c.Cleanup(func() { dropMySQLWriterLiveDatabase(c, adminDB, databaseName) })
			createMySQLWriterLiveUser(c, adminDB, owner, password)
			c.Cleanup(func() { dropMySQLWriterLiveUser(c, adminDB, owner) })
			grantRoutineOwner(c, adminDB, test.dialect, databaseName, owner)

			ownerDB := openFunctionOwnerDatabase(c, adminDSN, databaseName, owner, password)
			c.Cleanup(func() { c.Check(ownerDB.Close(), qt.IsNil) })
			_, err := ownerDB.ExecContext(
				c.Context(),
				"CREATE FUNCTION f() RETURNS INT DETERMINISTIC SQL SECURITY DEFINER RETURN 1",
			)
			c.Assert(err, qt.IsNil)

			adminURL := mysqlFamilyDatabaseURL(c, adminDSN, databaseName, test.dialect)
			adminConn, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(adminConn.Close(), qt.IsNil) })
			live, err := adminConn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(live.Functions, qt.HasLen, 1)
			c.Assert(live.Functions[0].Definer, qt.Contains, owner+"@")
			c.Assert(live.Functions[0].CurrentAccount, qt.Not(qt.Equals), live.Functions[0].Definer)

			desired := &goschema.Database{Functions: []goschema.Function{{
				Name: "f", Returns: "int", Language: "sql", Security: "DEFINER",
				Volatility: "IMMUTABLE", Body: "RETURN 2",
			}}}
			diff, err := schemadiff.CompareWithDatabase(
				c.Context(), adminConn, desired, live, nil,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*cannot safely replace.*execution principal.*`)
			c.Assert(diff, qt.IsNil)
			assertRoutineStillOwnedAndUnchanged(c, adminDB, databaseName, owner)

			ownerURL := mysqlFamilyDatabaseURL(c, ownerDSN(c, adminDSN, owner, password), databaseName, test.dialect)
			ownerConn, err := dbschema.ConnectToDatabase(c.Context(), ownerURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(ownerConn.Close(), qt.IsNil) })
			ownerLive, err := ownerConn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			ownerDiff, err := schemadiff.CompareWithDatabase(
				c.Context(), ownerConn, desired, ownerLive, nil,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(ownerDiff.FunctionsModified, qt.HasLen, 1)
		})
	}
}

func requireMySQLAdminDSN(c *qt.C, dsnEnv string) string {
	c.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		c.Skipf("%s is not set", dsnEnv)
	}
	return dsn
}

func grantRoutineOwner(c *qt.C, db *sql.DB, dialect, databaseName, owner string) {
	c.Helper()
	query := fmt.Sprintf(
		"GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%'",
		sqlident.Quote(dialect, databaseName),
		owner,
	)
	_, err := db.ExecContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
	// The pinned MySQL image enables binary logging and leaves
	// log_bin_trust_function_creators off. SUPER is therefore required even for
	// a deterministic function; the account is unique to this test and removed
	// by its cleanup.
	_, err = db.ExecContext(
		c.Context(),
		fmt.Sprintf("GRANT SUPER ON *.* TO '%s'@'%%'", owner),
	)
	c.Assert(err, qt.IsNil)
}

func openFunctionOwnerDatabase(c *qt.C, adminDSN, databaseName, owner, password string) *sql.DB {
	c.Helper()
	config, err := mysqldriver.ParseDSN(adminDSN)
	c.Assert(err, qt.IsNil)
	config.User = owner
	config.Passwd = password
	config.DBName = databaseName
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Assert(db.PingContext(c.Context()), qt.IsNil)
	return db
}

func ownerDSN(c *qt.C, adminDSN, owner, password string) string {
	c.Helper()
	config, err := mysqldriver.ParseDSN(adminDSN)
	c.Assert(err, qt.IsNil)
	config.User = owner
	config.Passwd = password
	return config.FormatDSN()
}

func mysqlFamilyDatabaseURL(c *qt.C, dsn, databaseName, dialect string) string {
	c.Helper()
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	config.DBName = databaseName
	return fmt.Sprintf("%s://%s", dialect, config.FormatDSN())
}

func assertRoutineStillOwnedAndUnchanged(c *qt.C, db *sql.DB, databaseName, owner string) {
	c.Helper()
	var definer, body string
	err := db.QueryRowContext(
		c.Context(),
		"SELECT DEFINER, ROUTINE_DEFINITION FROM information_schema.ROUTINES "+
			"WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = 'f' AND ROUTINE_TYPE = 'FUNCTION'",
		databaseName,
	).Scan(&definer, &body)
	c.Assert(err, qt.IsNil)
	c.Assert(definer, qt.Contains, owner+"@")
	c.Assert(body, qt.Equals, "RETURN 1")

	var count int
	err = db.QueryRowContext(
		c.Context(),
		"SELECT COUNT(*) FROM information_schema.ROUTINES "+
			"WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = 'f' AND ROUTINE_TYPE = 'FUNCTION'",
		databaseName,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}
