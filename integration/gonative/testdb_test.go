//go:build integration

package gonative_test

import (
	"database/sql"
	"os"
	"testing"
)

// requireReachableTestDSN resolves a *driver* DSN, which is why it still reads
// the environment itself rather than calling internal/dbtarget.
//
// Its result goes straight to sql.Open(driverName, dsn), so it has to be in the
// grammar that driver parses. dbtarget answers with the address ptah connects
// with, and for four of the engines reached through here that is a different
// string: the MySQL and MariaDB addresses carry a mysql:// or mariadb:// scheme
// that go-sql-driver/mysql reads as part of the username, and the CockroachDB
// and YugabyteDB addresses carry cockroachdb:// or yugabytedb://, which pgx
// does not parse at all. Routing this helper through dbtarget would turn a
// configured live run into an Open or Ping failure rather than a passing test.
//
// One caller also names MYSQL_CLEANUP_TEST_DSN, a second MySQL database whose
// tables the test drops wholesale. dbtarget declares no engine for it, and it is
// not MySQLAdmin: that is an account, this is a separate database.
func requireReachableTestDSN(t *testing.T, envName, driverName, databaseName string) string {
	t.Helper()

	dsn := os.Getenv(envName)
	if dsn == "" {
		t.Skipf("Skipping %s tests: %s environment variable not set", databaseName, envName)
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		t.Fatalf("%s is set but %s database open failed: %v", envName, databaseName, err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("%s is set but %s database connection failed: %v", envName, databaseName, err)
	}

	return dsn
}
