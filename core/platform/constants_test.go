package platform_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

func TestNormalizeDialect_DistributedSQLAliases(t *testing.T) {
	c := qt.New(t)

	tests := map[string]string{
		"cockroach":     platform.CockroachDB,
		"cockroachdb":   platform.CockroachDB,
		"crdb":          platform.CockroachDB,
		"yugabyte":      platform.YugabyteDB,
		"yugabytedb":    platform.YugabyteDB,
		"ysql":          platform.YugabyteDB,
		" CockroachDB ": platform.CockroachDB,
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
		// The socket transports the pinned community binary v1.3.0 opens.
		"mysql+unix":   platform.MySQL,
		"mariadb+unix": platform.MariaDB,
		"maria+unix":   platform.MariaDB,
		"MARIA+UNIX":   platform.MariaDB,
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

// ydb and ydbs name the engine the official YDB provider serves, and no
// built-in dialect answers to them. grpc and grpcs are the YDB SDK's schemes
// and name a protocol, so every gRPC URL would read as a database URL.
func TestNormalizeDialect_YDBSpellings_FailurePath(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"ydb", "ydbs", " YDB ", "grpc", "grpcs", "ydb+grpc", "yql", "ydbsql"} {
		c.Assert(platform.NormalizeDialect(dialect), qt.Equals, "", qt.Commentf("dialect %q", dialect))
	}
}

// Spanner is served only through its executable provider: no built-in
// descriptor claims any of its spellings.
func TestNormalizeDialect_SpannerSpellings_FailurePath(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"spanner", " Spanner ", "cloudspanner", "google-spanner", "google_spanner"} {
		c.Assert(platform.NormalizeDialect(dialect), qt.Equals, "", qt.Commentf("dialect %q", dialect))
	}
}

func TestIsPostgresFamily(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"postgres", "pgx", "cockroachdb", "yugabytedb"} {
		c.Assert(platform.IsPostgresFamily(dialect), qt.IsTrue, qt.Commentf("dialect %q", dialect))
	}
	for _, dialect := range []string{"mysql", "mariadb", "clickhouse", "sqlite", "sqlserver", "oracle"} {
		c.Assert(platform.IsPostgresFamily(dialect), qt.IsFalse, qt.Commentf("dialect %q", dialect))
	}
}

// Measured on MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3: the grant
// option belongs to the grantee at the object, not to the privilege.
func TestGrantOptionIsPerObject(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"mysql", "mariadb", "maria"} {
		c.Assert(platform.GrantOptionIsPerObject(dialect), qt.IsTrue, qt.Commentf("dialect %q", dialect))
	}
	for _, dialect := range []string{
		"postgres", "cockroachdb", "yugabytedb", "clickhouse", "sqlite", "sqlserver", "oracle", "bogus",
	} {
		c.Assert(platform.GrantOptionIsPerObject(dialect), qt.IsFalse, qt.Commentf("dialect %q", dialect))
	}
}
