//go:build ptah_live_realm_cleanup

package devlock_test

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/devlock"
)

func TestSameRealm_PostgresDriverURLOverridesLive(t *testing.T) {
	c := qt.New(t)
	targetURL := requireLiveRealmURL(c, "POSTGRES_TEST_DSN")
	overrideURL := postgresDriverOverrideURL(c, targetURL)
	targetConn, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(targetConn)
	overrideConn, err := dbschema.ConnectToDatabase(c.Context(), overrideURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(overrideConn)

	same, err := devlock.SameRealm(c.Context(), targetConn, overrideConn)

	c.Assert(err, qt.IsNil)
	c.Assert(same, qt.IsTrue)
}

func TestSameRealm_NetworkDatabasesLive(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name           string
		environmentKey string
	}{
		{name: "postgres", environmentKey: "POSTGRES_TEST_DSN"},
		{name: "cockroachdb", environmentKey: "COCKROACHDB_URL"},
		{name: "yugabytedb", environmentKey: "YUGABYTEDB_URL"},
		{name: "mysql", environmentKey: "MYSQL_TEST_URL"},
		{name: "mariadb", environmentKey: "MARIADB_TEST_URL"},
		{name: "clickhouse", environmentKey: "CLICKHOUSE_URL"},
		{name: "sqlserver", environmentKey: "PTAH_SQLSERVER_TEST_URL"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			databaseURL := requireLiveRealmURL(c, test.environmentKey)
			firstConn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				dbschema.CloseAndWarn(firstConn)
			})
			secondConn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				dbschema.CloseAndWarn(secondConn)
			})

			same, err := devlock.SameRealm(c.Context(), firstConn, secondConn)

			c.Assert(err, qt.IsNil)
			c.Assert(same, qt.IsTrue)
		})
	}
}

func TestAcquire_CockroachDBSerializesSameRealmLive(t *testing.T) {
	c := qt.New(t)
	databaseURL := requireCockroachDBURL(c)
	firstConn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(firstConn)
	secondConn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(secondConn)

	firstLock, err := devlock.Acquire(c.Context(), firstConn, 0)
	c.Assert(err, qt.IsNil)

	waitCtx, cancel := context.WithTimeout(c.Context(), 50*time.Millisecond)
	defer cancel()
	blockedLock, err := devlock.Acquire(waitCtx, secondConn, 0)
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(blockedLock, qt.IsNil)

	c.Assert(firstLock.Release(), qt.IsNil)
	secondLock, err := devlock.Acquire(c.Context(), secondConn, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(secondLock.Release(), qt.IsNil)
}

func requireCockroachDBURL(c *qt.C) string {
	return requireLiveRealmURL(c, "COCKROACHDB_URL")
}

func requireLiveRealmURL(c *qt.C, environmentKey string) string {
	c.Helper()
	databaseURL := os.Getenv(environmentKey)
	if databaseURL == "" {
		c.Skip(environmentKey + " is not set")
	}
	return databaseURL
}

func postgresDriverOverrideURL(c *qt.C, rawURL string) string {
	c.Helper()
	config, err := pgx.ParseConfig(rawURL)
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	if parsed.Scheme == "" {
		c.Skip("POSTGRES_TEST_DSN is not a URL")
	}
	parsed.Host = "guard.invalid:1"
	parsed.Path = "/ignored"
	query := parsed.Query()
	query.Set("host", config.Host)
	query.Set("port", strconv.Itoa(int(config.Port)))
	query.Set("database", config.Database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
