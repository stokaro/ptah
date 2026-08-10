//go:build ptah_live_migrator

package migrator_test

import (
	"os"
	"strings"
	"testing"
)

func sqlServerTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if dbURL == "" {
		t.Skip("PTAH_SQLSERVER_TEST_URL not set")
	}
	if !strings.HasPrefix(dbURL, "sqlserver://") && !strings.HasPrefix(dbURL, "mssql://") {
		t.Skip("SQL Server URL required for live migration test")
	}
	return dbURL
}
