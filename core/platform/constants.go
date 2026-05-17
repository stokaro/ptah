package platform

import (
	"strings"
)

const (
	Postgres   = "postgres"
	MySQL      = "mysql"
	MariaDB    = "mariadb"
	ClickHouse = "clickhouse"
)

func NormalizeDialect(dialect string) string {
	switch strings.ToLower(dialect) {
	case "pgx", "postgresql", "postgres":
		return Postgres
	case "mysql":
		return MySQL
	case "mariadb":
		return MariaDB
	case "clickhouse", "ch":
		return ClickHouse
	default:
		return ""
	}
}
