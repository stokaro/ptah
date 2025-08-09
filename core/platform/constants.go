package platform

import (
	"strings"
)

const (
	Postgres = "postgres"
	MySQL    = "mysql"
	MariaDB  = "mariadb"
)

func NormalizeDialect(dialect string) string {
	switch strings.ToLower(dialect) {
	case "pgx", "postgresql", "postgres":
		return Postgres
	case "mysql":
		return MySQL
	case "mariadb":
		return MariaDB
	default:
		return ""
	}
}
