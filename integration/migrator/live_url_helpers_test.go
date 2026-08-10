//go:build integration

package migrator_test

import (
	"os"
	"strings"
	"testing"
)

func postgresTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("POSTGRES_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN or TEST_DATABASE_URL not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for live migration test")
	}
	return dbURL
}

func mySQLFamilyTestURL(t *testing.T, dialect string, envNames ...string) string {
	t.Helper()

	for _, envName := range envNames {
		dbURL := os.Getenv(envName)
		if dbURL == "" {
			continue
		}
		if !strings.HasPrefix(dbURL, dialect+"://") {
			t.Skipf("%s URL required for live migration test", dialect)
		}
		return dbURL
	}

	t.Skipf("%s not set", strings.Join(envNames, " or "))
	return ""
}
