//go:build integration

package gonative_test

import (
	"database/sql"
	"os"
	"testing"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// requireReachableEngine resolves an engine's driver-form address and proves
// it answers before a test runs against it.
//
// It goes through internal/dbtarget rather than reading a variable, so a run
// configured with the canonical spelling is exercised rather than skipped. The
// helper below used to read the environment directly, and the reason it gave
// was that dbtarget answered only with the address ptah connects with -- which
// carries a scheme go-sql-driver/mysql reads as part of the username and pgx
// does not parse at all. DriverDSN answers with the driver's own form, so that
// reason is gone.
func requireReachableEngine(t *testing.T, engine dbtarget.Engine, driverName, databaseName string) string {
	t.Helper()
	return probeReachable(t, dbtarget.DriverDSN(t, engine), driverName, databaseName, engine.String())
}

// requireReachableTestDSN resolves a *driver* DSN named by one variable, for
// the one target dbtarget declares no engine for.
//
// MYSQL_CLEANUP_TEST_DSN is a second MySQL database whose tables a test drops
// wholesale. It is not MySQLAdmin: that is an account, this is a separate
// database, and giving it an engine would put a destructive target in the same
// list every ordinary test picks from.
func requireReachableTestDSN(t *testing.T, envName, driverName, databaseName string) string {
	t.Helper()

	dsn := os.Getenv(envName)
	if dsn == "" {
		t.Skipf("Skipping %s tests: %s environment variable not set", databaseName, envName)
	}
	return probeReachable(t, dsn, driverName, databaseName, envName)
}

// probeReachable opens dsn and pings it, so a configured but unreachable
// database fails the run rather than letting the test report against nothing.
func probeReachable(t *testing.T, dsn, driverName, databaseName, source string) string {
	t.Helper()

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		t.Fatalf("%s is set but %s database open failed: %v", source, databaseName, err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("%s is set but %s database ping failed: %v", source, databaseName, err)
	}
	return dsn
}
