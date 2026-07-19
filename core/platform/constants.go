package platform

import (
	"strings"
)

const (
	Postgres    = "postgres"
	MySQL       = "mysql"
	MariaDB     = "mariadb"
	ClickHouse  = "clickhouse"
	SQLite      = "sqlite"
	CockroachDB = "cockroachdb"
	YugabyteDB  = "yugabytedb"
	Spanner     = "spanner"
)

func NormalizeDialect(dialect string) string {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "pgx", "postgresql", "postgres":
		return Postgres
	case "mysql":
		return MySQL
	case "mariadb":
		return MariaDB
	case "clickhouse", "ch":
		return ClickHouse
	case "sqlite", "sqlite3":
		return SQLite
	case "cockroach", "cockroachdb", "crdb":
		return CockroachDB
	case "yugabyte", "yugabytedb", "ysql":
		return YugabyteDB
	case "spanner", "cloudspanner", "google-spanner", "google_spanner":
		return Spanner
	default:
		return ""
	}
}

func IsPostgresFamily(dialect string) bool {
	switch NormalizeDialect(dialect) {
	case Postgres, CockroachDB, YugabyteDB, Spanner:
		return true
	default:
		return false
	}
}
