package platform_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
)

func TestNormalizeDialect_DistributedSQLAliases(t *testing.T) {
	c := qt.New(t)

	tests := map[string]string{
		"cockroach":      platform.CockroachDB,
		"cockroachdb":    platform.CockroachDB,
		"crdb":           platform.CockroachDB,
		"yugabyte":       platform.YugabyteDB,
		"yugabytedb":     platform.YugabyteDB,
		"ysql":           platform.YugabyteDB,
		"spanner":        platform.Spanner,
		"cloudspanner":   platform.Spanner,
		"google-spanner": platform.Spanner,
		"google_spanner": platform.Spanner,
		" CockroachDB ":  platform.CockroachDB,
	}

	for input, expected := range tests {
		c.Assert(platform.NormalizeDialect(input), qt.Equals, expected)
	}
}

func TestNormalizeDialect_SQLiteAliases(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"sqlite", "sqlite3", " SQLite3 "} {
		c.Assert(platform.NormalizeDialect(dialect), qt.Equals, platform.SQLite)
	}
}

func TestNormalizeDialect_SQLServerAliases(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"mssql", "sqlserver", "sql-server", "sql_server", "tsql", " SQLServer "} {
		c.Assert(platform.NormalizeDialect(dialect), qt.Equals, platform.SQLServer)
	}
}

func TestIsPostgresFamily(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"postgres", "pgx", "cockroachdb", "yugabytedb", "spanner"} {
		c.Assert(platform.IsPostgresFamily(dialect), qt.IsTrue, qt.Commentf("dialect %q", dialect))
	}
	for _, dialect := range []string{"mysql", "mariadb", "clickhouse", "sqlite", "sqlserver", "oracle"} {
		c.Assert(platform.IsPostgresFamily(dialect), qt.IsFalse, qt.Commentf("dialect %q", dialect))
	}
}
