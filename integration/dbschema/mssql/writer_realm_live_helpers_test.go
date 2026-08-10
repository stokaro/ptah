//go:build integration

package mssql_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/microsoft/go-mssqldb"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/sqlident"
)

func openLiveSQLServerRealmDatabase(t *testing.T) *sql.DB {
	t.Helper()
	adminURL, configured := os.LookupEnv("PTAH_SQLSERVER_REALM_TEST_URL")
	if !configured {
		t.Skip("PTAH_SQLSERVER_REALM_TEST_URL is not configured")
	}
	admin, err := sql.Open("sqlserver", adminURL)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, admin.PingContext(t.Context()), qt.IsNil)

	databaseName := fmt.Sprintf("ptah_realm_%d", time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(platform.SQLServer, databaseName)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase)
	qt.Assert(t, err, qt.IsNil)

	parsedURL, err := url.Parse(adminURL)
	qt.Assert(t, err, qt.IsNil)
	query := parsedURL.Query()
	query.Set("database", databaseName)
	parsedURL.RawQuery = query.Encode()
	db, err := sql.Open("sqlserver", parsedURL.String())
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, db.PingContext(t.Context()), qt.IsNil)

	t.Cleanup(func() {
		qt.Check(t, db.Close(), qt.IsNil)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, cleanupErr := admin.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+quotedDatabase+
				" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+quotedDatabase,
		)
		qt.Check(t, cleanupErr, qt.IsNil)
		qt.Check(t, admin.Close(), qt.IsNil)
	})
	return db
}
