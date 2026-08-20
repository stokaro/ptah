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
	SQLServer   = "sqlserver"
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
	// libsql is a transport, not a dialect. The pinned community binary v1.3.0
	// resolves both spellings onto its SQLite driver -- the errors it answers
	// with are prefixed `sqlite:` and the HCL it inspects is SQLite HCL -- so
	// the renderer, planner and reader are SQLite's and only the connection
	// differs (stokaro/ptah#1615).
	case "libsql", "libsql+ws":
		return SQLite
	case "mssql", "sqlserver", "sql-server", "sql_server", "tsql":
		return SQLServer
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
