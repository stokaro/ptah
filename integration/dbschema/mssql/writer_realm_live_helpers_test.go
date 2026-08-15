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
	c := qt.New(t)
	t.Helper()
	adminURL, configured := os.LookupEnv("PTAH_SQLSERVER_REALM_TEST_URL")
	if !configured {
		t.Skip("PTAH_SQLSERVER_REALM_TEST_URL is not configured")
	}
	admin, err := sql.Open("sqlserver", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(t.Context()), qt.IsNil)

	databaseName := fmt.Sprintf("ptah_realm_%d", time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(platform.SQLServer, databaseName)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase)
	c.Assert(err, qt.IsNil)

	parsedURL, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	query := parsedURL.Query()
	query.Set("database", databaseName)
	parsedURL.RawQuery = query.Encode()
	db, err := sql.Open("sqlserver", parsedURL.String())
	c.Assert(err, qt.IsNil)
	c.Assert(db.PingContext(t.Context()), qt.IsNil)

	t.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, cleanupErr := admin.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+quotedDatabase+
				" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+quotedDatabase,
		)
		c.Check(cleanupErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return db
}
