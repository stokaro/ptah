package platform_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
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

func TestNormalizeDialect_MySQLFamilyAliases_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := map[string]string{
		"mysql":     platform.MySQL,
		"mariadb":   platform.MariaDB,
		"maria":     platform.MariaDB,
		" Maria ":   platform.MariaDB,
		"MARIA":     platform.MariaDB,
		" MariaDB ": platform.MariaDB,
	}

	for input, expected := range tests {
		c.Assert(platform.NormalizeDialect(input), qt.Equals, expected, qt.Commentf("input %q", input))
	}
}

// TestNormalizeDialect_MySQLFamilyAliases_FailurePath pins spellings the
// pinned community binary v1.3.0 refuses with `unknown driver`, so accepting
// `maria` did not widen the family into a prefix match.
func TestNormalizeDialect_MySQLFamilyAliases_FailurePath(t *testing.T) {
	c := qt.New(t)

	for _, input := range []string{"maria+tcp", "mariadb+tcp", "mysql+tcp", "mari", "mariadbx"} {
		c.Assert(platform.NormalizeDialect(input), qt.Equals, "", qt.Commentf("input %q", input))
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
