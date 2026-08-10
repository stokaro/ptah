//go:build integration

package generator_test

import (
	"net/url"
	"os"
	"strconv"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
)

func requireRollbackPostgresURL(c *qt.C) string {
	c.Helper()
	rawURL := os.Getenv("POSTGRES_TEST_DSN")
	if rawURL == "" {
		c.Skip("POSTGRES_TEST_DSN is not set")
	}
	return rawURL
}

func rollbackPostgresDatabaseURL(c *qt.C, rawURL, database string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	if parsed.Scheme == "" {
		c.Skip("POSTGRES_TEST_DSN is not a URL")
	}
	parsed.Path = "/" + database
	query := parsed.Query()
	query.Set("database", database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func rollbackPostgresDriverOverrideURL(c *qt.C, rawURL string) string {
	c.Helper()
	config, err := pgx.ParseConfig(rawURL)
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Host = "guard.invalid:1"
	parsed.Path = "/ignored"
	query := parsed.Query()
	query.Set("host", config.Host)
	query.Set("port", strconv.Itoa(int(config.Port)))
	query.Set("database", config.Database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
