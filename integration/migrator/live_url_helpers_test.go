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
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/sqlident"
)

// postgresTestURL resolves the live PostgreSQL address. dbtarget refuses an
// address carrying another engine's scheme, so the dialect guard this helper
// used to carry has nothing left to catch.
func postgresTestURL(t *testing.T) string {
	t.Helper()

	return dbtarget.URL(t, dbtarget.PostgreSQL)
}

// mySQLFamilyTestURL resolves one MySQL-family engine and keeps the dialect
// guard: dbtarget declares no scheme for the MySQL family, because a MySQL
// address is often a driver DSN with no scheme at all, so nothing in the
// package stops a MariaDB address sitting in a MySQL variable.
func mySQLFamilyTestURL(t *testing.T, dialect string, engine dbtarget.Engine) string {
	t.Helper()

	dbURL := dbtarget.URL(t, engine)
	if !strings.HasPrefix(dbURL, dialect+"://") {
		t.Skipf("%s URL required for live migration test", dialect)
	}
	return dbURL
}

// mySQLFamilyScratchDatabaseURL keeps the administrative connection on its
// configured system database and returns a separate URL rooted in a unique test
// database. The returned URL deliberately preserves the administrative
// credentials: callers in this package need privileges such as CREATE TRIGGER,
// while the system database is used only to provision and remove the realm.
func mySQLFamilyScratchDatabaseURL(
	t *testing.T,
	dialect string,
	adminEngine dbtarget.Engine,
	prefix string,
) string {
	c := qt.New(t)
	t.Helper()

	adminURL := mySQLFamilyTestURL(t, dialect, adminEngine)
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
	c := qt.New(t)
	t.Helper()

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
