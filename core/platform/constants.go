package platform

import (
	"slices"
	"strings"
)

// The canonical dialect names. Every other accepted spelling folds onto one
// of these through NormalizeDialect, and the layers that vary by target --
// renderers, readers, planners, capability presets -- key off these values.
// Use the constants rather than a string a user typed or a literal of your
// own: the names are the public identifiers, and the values can still change
// before a stable release.
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
	// YDB is the YDB database, read and written in its own query language,
	// YQL, over gRPC. It is not in the PostgreSQL family: YDB serves no
	// PostgreSQL wire protocol or syntax, and in YQL a double-quoted "x" is a
	// string rather than a name.
	YDB = "ydb"
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
	spelling := strings.ToLower(strings.TrimSpace(dialect))
	for _, target := range dialectNames {
		if slices.Contains(target, spelling) {
			return target[0]
		}
	}
	return ""
}

// DialectSpellings returns every built-in target name and alias accepted by
// NormalizeDialect. Each call returns an independent slice. Composition uses
// this declaration so registered aliases cannot drift from normalization.
func DialectSpellings() []string {
	var names []string
	for _, target := range dialectNames {
		names = append(names, target...)
	}
	return names
}

// Each row starts with its canonical target name. Transport spellings select
// the same schema semantics: +unix uses the MySQL/MariaDB socket transport
// (#3755), libsql+ws uses SQLite (#1615), and ydbs uses YDB over TLS (#4015).
// grpc and grpcs identify a protocol, not a database, and are not aliases.
// The maria spelling is accepted by the pinned Atlas binary (#3744).
var dialectNames = [][]string{
	{Postgres, "pgx", "postgresql"},
	{MySQL, "mysql+unix"},
	{MariaDB, "maria", "mariadb+unix", "maria+unix"},
	{ClickHouse, "ch"},
	{SQLite, "sqlite3", "libsql", "libsql+ws"},
	{SQLServer, "mssql", "sql-server", "sql_server", "tsql"},
	{CockroachDB, "cockroach", "crdb"},
	{YugabyteDB, "yugabyte", "ysql"},
	{Spanner, "cloudspanner", "google-spanner", "google_spanner"},
	{Oracle, "oracledb"},
	{YDB, "ydbs"},
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

// GrantOptionIsPerObject reports whether a WITH GRANT OPTION grant on this
// target belongs to the grantee at the whole object, rather than to one
// privilege.
//
// Measured on MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3: after
// `GRANT SELECT, INSERT ON t TO r WITH GRANT OPTION`, `REVOKE GRANT OPTION ON
// t FROM r` leaves both privileges in place and makes neither grantable, and a
// later `GRANT INSERT ON t TO r` -- without repeating WITH GRANT OPTION --
// makes SELECT grantable again along with it. The option is a property of
// (grantee, object), not of (grantee, object, privilege). Every other target
// this constant answers false for keeps one grantable flag per privilege.
func GrantOptionIsPerObject(dialect string) bool {
	switch NormalizeDialect(dialect) {
	case MySQL, MariaDB:
		return true
	default:
		return false
	}
}
