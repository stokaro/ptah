//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/sqlident"
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

// mySQLFamilyScratchDatabaseURL keeps the administrative connection on its
// configured system database and returns a separate URL rooted in a unique test
// database. The returned URL deliberately preserves the administrative
// credentials: callers in this package need privileges such as CREATE TRIGGER,
// while the system database is used only to provision and remove the realm.
func mySQLFamilyScratchDatabaseURL(t *testing.T, dialect, adminEnv, prefix string) string {
	t.Helper()
	c := qt.New(t)

	adminURL := mySQLFamilyTestURL(t, dialect, adminEnv)
	adminConn, err := dbschema.ConnectToDatabase(t.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(adminConn) })
	database := fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(dialect, database)
	_, err = adminConn.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, cleanupErr := adminConn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quotedDatabase)
		c.Check(cleanupErr, qt.IsNil)
	})

	return mySQLFamilyURLWithDatabase(t, adminURL, database)
}

func mySQLFamilyURLWithDatabase(t *testing.T, rawURL, database string) string {
	t.Helper()
	c := qt.New(t)

	if strings.Contains(rawURL, "@tcp(") {
		scheme, dsn, found := strings.Cut(rawURL, "://")
		c.Assert(found, qt.IsTrue)
		config, err := mysqldriver.ParseDSN(dsn)
		c.Assert(err, qt.IsNil)
		config.DBName = database
		return scheme + "://" + config.FormatDSN()
	}

	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	return parsed.String()
}

func TestMySQLFamilyURLWithDatabase(t *testing.T) {
	tests := []struct {
		name     string
		rawURL   string
		database string
		want     string
	}{
		{
			name:     "driver TCP form",
			rawURL:   "mysql://root@tcp(127.0.0.1:3306)/mysql?parseTime=true",
			database: "scratch",
			want:     "mysql://root@tcp(127.0.0.1:3306)/scratch?parseTime=true",
		},
		{
			name:     "standard URL form",
			rawURL:   "mariadb://root@127.0.0.1:3306/mysql?parseTime=true",
			database: "scratch",
			want:     "mariadb://root@127.0.0.1:3306/scratch?parseTime=true",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := mySQLFamilyURLWithDatabase(t, test.rawURL, test.database)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
