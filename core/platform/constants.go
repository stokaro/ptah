package platform

import (
	"strings"
)

// The canonical dialect names. Every other accepted spelling folds onto one
// of these through NormalizeDialect, and the layers that vary by target --
// renderers, readers, planners, capability presets -- switch on exactly these
// lowercase values. Compare against the constants, never against a string a
// user typed.
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
	Oracle      = "oracle"
)

// NormalizeDialect folds every spelling of a target onto the one constant the
// rest of Ptah compares against, and returns "" for a name it does not know.
//
// The empty answer is load-bearing: a caller that treats it as a dialect asks
// every layer below to render for a target nobody implemented. Check it.
//
// A name added here becomes valid in every layer that switches on the result,
// including the ones that cannot handle it yet: a renderer, a reader and a
// planner each carry their own list, and none of them is derived from this one.
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
	case "oracle", "oracledb":
		return Oracle
	default:
		return ""
	}
}

// IsPostgresFamily reports whether a target speaks the PostgreSQL wire protocol
// and catalog, which is what lets one reader and one renderer serve all four.
//
// It answers a question about the DIALECT, not about a feature: CockroachDB,
// YugabyteDB and Spanner are in the family and each refuses things PostgreSQL
// accepts. A caller deciding whether a capability exists reads the capability,
// not this.
func IsPostgresFamily(dialect string) bool {
	switch NormalizeDialect(dialect) {
	case Postgres, CockroachDB, YugabyteDB, Spanner:
		return true
	default:
		return false
	}
}
