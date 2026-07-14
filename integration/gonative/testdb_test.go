//go:build integration

package gonative_test

import (
	"database/sql"
	"os"
	"testing"
)

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
